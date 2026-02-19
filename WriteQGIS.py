# Imports
from os.path import exists

import pandas as pd
import geopandas as gpd
import sqlite3
import pathlib
import shutil
import os
import json
from datetime import datetime


# Value Maps aus JSON laden
def load_value_maps(json_path="value_maps.json"):
    """Lädt Value Map Definitionen aus einer JSON-Datei."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[WARNING] Value Maps Datei nicht gefunden: {json_path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"[WARNING] Fehler beim Lesen der Value Maps: {e}")
        return {}


# GeoPackage einlesen
def read_layer(path, layer):
    """
    Liest einen Layer aus GPKG mit pyogrio Engine (moderner und schneller als fiona).
    Die GPKG-Datentypen sind führend!
    Gibt IMMER ein GeoDataFrame zurück (auch ohne Geometrie).
    """
    try:
        gdf = gpd.read_file(path, layer=layer, engine="pyogrio")
    except ValueError:
        # Wenn keine echte Geometrie gefunden wurde, lese ohne geometry
        df = gpd.read_file(path, layer=layer, engine="pyogrio", ignore_geometry=True)
        # Konvertiere zu GeoDataFrame mit None-Geometrie
        gdf = gpd.GeoDataFrame(df, geometry=None)

    return gdf


# Funktion zur Konvertierung von Strings zu Indices basierend auf Value Maps
def convert_to_coded_value(column_name, value, value_maps, layer_name):
    """
    Konvertiert einen String-Wert zu seinem Index basierend auf der Value Map.
    Falls der Wert nicht in der Map existiert, wird der Original-Wert zurückgegeben.

    Args:
        column_name: Name der Spalte
        value: Der zu konvertierende Wert
        value_maps: Das gesamte Value Maps Dictionary
        layer_name: Der Layer-Name (z.B. "PN_Protokoll" oder "PN_Gruppe")

    Returns:
        Index (int) wenn Wert in Map gefunden, sonst Original-Wert
    """
    if pd.isna(value):
        return value

    # Hole die Value Map für diesen Layer
    layer_maps = value_maps.get(layer_name, {})

    # Hole die Value Map für diese Spalte
    col_map = layer_maps.get(column_name, {})

    # Konvertiere Wert zu String für Vergleich
    value_str = str(value).strip()

    # Suche nach dem Index
    if value_str in col_map:
        return col_map[value_str]

    # Wenn nicht gefunden, gib Original-Wert zurück
    return value


# Daten in GPKG Datenbank schreiben - DIREKT MIT SQLite UPDATE (KEINE INSERT/DELETE)
def write_gpkg(df, gdf_group, gdf_prt, filepath, probes_list, debug=True, debug_probe_numbers=None):
    """
    Aktualisiert existierende Zeilen in der GPKG direkt mit SQLite UPDATE.
    Keine Duplikate, keine Geometrie-Probleme, keine Klammern!

    debug: wenn True, werden Debug-Informationen zu Skips/Typfehlern/Rowcount ausgegeben.
    debug_probe_numbers: optionales Iterable von Probennummern, um Debug-Ausgaben zu filtern.
    """

    # Backup erstellen
    safe_copy(filepath)

    # Verbinde mit GPKG
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    def _should_debug(probe):
        if not debug:
            return False
        if debug_probe_numbers is None:
            return True
        return probe in debug_probe_numbers

    def _get_column_type(table, column):
        rows = cursor.execute(f"PRAGMA table_info('{table}')").fetchall()
        for row in rows:
            if row[1] == column:
                return row[2]
        return None

    def _build_where(column, value, col_type):
        if pd.isna(value):
            return None, None
        t = (col_type or "").upper()
        if "TEXT" in t:
            # Bei TEXT: numerisch vergleichen, falls möglich
            if isinstance(value, (int, float, pd.Int64Dtype().type)):
                return f"CAST(`{column}` AS REAL) = ?", [float(value)]
            return f"`{column}` = ?", [str(value)]
        if "INT" in t:
            return f"`{column}` = ?", [int(value)]
        if "REAL" in t or "FLOA" in t or "DOUB" in t:
            return f"`{column}` = ?", [float(value)]
        return f"`{column}` = ?", [value]

    def _try_load_spatialite():
        try:
            conn.enable_load_extension(True)
        except Exception:
            return False

        candidates = [
            "mod_spatialite",
            "mod_spatialite.dll",
            os.path.join(os.environ.get("CONDA_PREFIX", ""), "Library", "bin", "mod_spatialite.dll"),
            os.path.join(os.environ.get("CONDA_PREFIX", ""), "Library", "lib", "mod_spatialite.dll"),
        ]
        for path in candidates:
            if not path:
                continue
            if path.endswith(".dll") and not os.path.exists(path):
                continue
            try:
                conn.load_extension(path)
                return True
            except Exception:
                continue
        return False

    spatialite_ok = _try_load_spatialite()
    if debug and not spatialite_ok:
        print("[DEBUG] Spatialite-Erweiterung konnte nicht geladen werden. UPDATEs auf Geo-Tabellen koennen fehlschlagen (ST_IsEmpty).")

    # Lade Value Maps aus der QGIS-Projektverwaltung
    try:
        with open("resources/value_maps_from_qgis.json", "r", encoding="utf-8") as f:
            value_maps = json.load(f)
        if debug:
            print("[DEBUG] Value Maps aus QGIS-Projekt geladen.")
    except FileNotFoundError:
        if debug:
            print("[WARNING] value_maps_from_qgis.json nicht gefunden. Verwende Fallback value_maps.json")
        value_maps = load_value_maps("value_maps.json")
    except json.JSONDecodeError as e:
        if debug:
            print(f"[WARNING] Fehler beim Lesen von value_maps_from_qgis.json: {e}")
        value_maps = load_value_maps("value_maps.json")

    # Debug-Zaehler
    dbg_prt = {"nan_skips": 0, "conv_errors": 0, "empty_updates": 0, "zero_rowcount": 0}
    dbg_grp = {"nan_skips": 0, "conv_errors": 0, "empty_updates": 0, "zero_rowcount": 0}

    prt_key_type = _get_column_type("PN_Protokoll", "Nummer")
    grp_key_type = _get_column_type("PN_Gruppe", "Gruppe_Nummer")

    # Schritt 1: PN_Protokoll mit SQL UPDATE aktualisieren
    print("Aktualisiere PN_Protokoll...")
    for probe in probes_list:
        # Probe in df finden
        df_row = df[df["Nummer"] == probe]
        if df_row.empty:
            if _should_debug(probe):
                print(f"[DEBUG PN_Protokoll] Keine Zeile in df fuer Nummer={probe} gefunden")
            continue

        df_idx = df_row.index[0]

        # Baue SQL UPDATE Pairs fuer alle Spalten aus gdf_prt
        update_pairs = []
        update_values = []

        for col in gdf_prt.columns:
            if col == "geometry":
                continue

            # Wert aus df nehmen, falls vorhanden, sonst behalte Original
            if col in df.columns:
                val = df.loc[df_idx, col]

                # Nur wenn Wert nicht NaN, update
                if pd.notna(val):
                    # Konvertiere zu SQL-kompatiblem Wert
                    if pd.api.types.is_float_dtype(gdf_prt[col].dtype):
                        try:
                            sql_val = float(val)
                            update_pairs.append(f"`{col}` = ?")
                            update_values.append(sql_val)
                        except (ValueError, TypeError) as exc:
                            dbg_prt["conv_errors"] += 1
                            if _should_debug(probe):
                                print(f"[DEBUG PN_Protokoll] Float-Konvertierung fehlgeschlagen: Nummer={probe}, Spalte={col}, Wert={val!r}, Fehler={exc}")
                    elif pd.api.types.is_integer_dtype(gdf_prt[col].dtype):
                        try:
                            sql_val = int(float(val))
                            update_pairs.append(f"`{col}` = ?")
                            update_values.append(sql_val)
                        except (ValueError, TypeError) as exc:
                            dbg_prt["conv_errors"] += 1
                            if _should_debug(probe):
                                print(f"[DEBUG PN_Protokoll] Int-Konvertierung fehlgeschlagen: Nummer={probe}, Spalte={col}, Wert={val!r}, Fehler={exc}")
                    elif pd.api.types.is_datetime64_any_dtype(gdf_prt[col].dtype):
                        sql_val = str(pd.Timestamp(val))
                        update_pairs.append(f"`{col}` = ?")
                        update_values.append(sql_val)
                    else:
                        # TEXT-Spalte: Versuche Value Map Konvertierung
                        converted_val = convert_to_coded_value(col, val, value_maps, "PN_Protokoll")
                        sql_val = str(converted_val) if converted_val != val else str(val)
                        update_pairs.append(f"`{col}` = ?")
                        update_values.append(sql_val)
                else:
                    dbg_prt["nan_skips"] += 1
                    if _should_debug(probe):
                        print(f"[DEBUG PN_Protokoll] NaN/leer uebersprungen: Nummer={probe}, Spalte={col}")

        # Fuehre UPDATE aus wenn es Aenderungen gibt
        if update_pairs:
            where_sql, where_vals = _build_where("Nummer", probe, prt_key_type)
            if not where_sql:
                if _should_debug(probe):
                    print(f"[DEBUG PN_Protokoll] WHERE fuer Nummer nicht ableitbar: Nummer={probe}")
                continue
            update_values.extend(where_vals)
            sql = f"UPDATE PN_Protokoll SET {', '.join(update_pairs)} WHERE {where_sql}"
            try:
                cursor.execute(sql, update_values)
                if cursor.rowcount == 0:
                    dbg_prt["zero_rowcount"] += 1
                    if _should_debug(probe):
                        count_sql = f"SELECT COUNT(*) FROM PN_Protokoll WHERE {where_sql}"
                        cnt = cursor.execute(count_sql, where_vals).fetchone()[0]
                        print(f"[DEBUG PN_Protokoll] UPDATE trifft keine Zeile: Nummer={probe}, Treffer={cnt}")
            except Exception as exc:
                if _should_debug(probe):
                    print(f"[DEBUG PN_Protokoll] UPDATE-Fehler: Nummer={probe}, Fehler={exc}")
        else:
            dbg_prt["empty_updates"] += 1
            if _should_debug(probe):
                print(f"[DEBUG PN_Protokoll] Keine updatebaren Werte: Nummer={probe}")

    conn.commit()

    # Schritt 2: PN_Gruppe mit SQL UPDATE aktualisieren
    print("Aktualisiere PN_Gruppe...")
    for probe in probes_list:
        df_row = df[df["Nummer"] == probe]
        if df_row.empty:
            if _should_debug(probe):
                print(f"[DEBUG PN_Gruppe] Keine Zeile in df fuer Nummer={probe} gefunden")
            continue

        df_idx = df_row.index[0]
        grp_number = df_row.loc[df_idx, "Gruppe_Nummer"]

        if pd.isna(grp_number):
            if _should_debug(probe):
                print(f"[DEBUG PN_Gruppe] Gruppe_Nummer leer: Nummer={probe}")
            continue

        # Baue SQL UPDATE Pairs
        update_pairs = []
        update_values = []

        for col in gdf_group.columns:
            if col == "geometry":
                continue

            if col in df.columns:
                val = df.loc[df_idx, col]

                if pd.notna(val):
                    if pd.api.types.is_float_dtype(gdf_group[col].dtype):
                        try:
                            sql_val = float(val)
                            update_pairs.append(f"`{col}` = ?")
                            update_values.append(sql_val)
                        except (ValueError, TypeError) as exc:
                            dbg_grp["conv_errors"] += 1
                            if _should_debug(probe):
                                print(f"[DEBUG PN_Gruppe] Float-Konvertierung fehlgeschlagen: Nummer={probe}, Spalte={col}, Wert={val!r}, Fehler={exc}")
                    elif pd.api.types.is_integer_dtype(gdf_group[col].dtype):
                        try:
                            sql_val = int(float(val))
                            update_pairs.append(f"`{col}` = ?")
                            update_values.append(sql_val)
                        except (ValueError, TypeError) as exc:
                            dbg_grp["conv_errors"] += 1
                            if _should_debug(probe):
                                print(f"[DEBUG PN_Gruppe] Int-Konvertierung fehlgeschlagen: Nummer={probe}, Spalte={col}, Wert={val!r}, Fehler={exc}")
                    else:
                        # TEXT-Spalte: Versuche Value Map Konvertierung
                        converted_val = convert_to_coded_value(col, val, value_maps, "PN_Gruppe")
                        sql_val = str(converted_val) if converted_val != val else str(val)
                        update_pairs.append(f"`{col}` = ?")
                        update_values.append(sql_val)
                else:
                    dbg_grp["nan_skips"] += 1
                    if _should_debug(probe):
                        print(f"[DEBUG PN_Gruppe] NaN/leer uebersprungen: Nummer={probe}, Spalte={col}")

        # Fuehre UPDATE aus
        if update_pairs:
            where_sql, where_vals = _build_where("Gruppe_Nummer", grp_number, grp_key_type)
            if not where_sql:
                if _should_debug(probe):
                    print(f"[DEBUG PN_Gruppe] WHERE fuer Gruppe_Nummer nicht ableitbar: Nummer={probe}")
                continue
            update_values.extend(where_vals)
            sql = f"UPDATE PN_Gruppe SET {', '.join(update_pairs)} WHERE {where_sql}"
            try:
                cursor.execute(sql, update_values)
                if cursor.rowcount == 0:
                    dbg_grp["zero_rowcount"] += 1
                    if _should_debug(probe):
                        count_sql = f"SELECT COUNT(*) FROM PN_Gruppe WHERE {where_sql}"
                        cnt = cursor.execute(count_sql, where_vals).fetchone()[0]
                        print(f"[DEBUG PN_Gruppe] UPDATE trifft keine Zeile: Gruppe_Nummer={grp_number} (Nummer={probe}), Treffer={cnt}")
            except Exception as exc:
                if _should_debug(probe):
                    print(f"[DEBUG PN_Gruppe] UPDATE-Fehler: Gruppe_Nummer={grp_number}, Nummer={probe}, Fehler={exc}")
        else:
            dbg_grp["empty_updates"] += 1
            if _should_debug(probe):
                print(f"[DEBUG PN_Gruppe] Keine updatebaren Werte: Nummer={probe}")

    conn.commit()
    conn.close()

    if debug:
        print(f"[DEBUG PN_Protokoll] Summary: {dbg_prt}")
        print(f"[DEBUG PN_Gruppe] Summary: {dbg_grp}")

    print("GPKG mit SQLite UPDATE aktualisiert)")


def safe_copy(filepath):
    p = pathlib.Path(filepath)
    d = p.parent
    today = datetime.today().strftime("%Y%m%d")
    dest_path = f"{d}/{today}_pn_protokoll.gpkg"

    if exists(dest_path):
        i = 1
        dest_path = f"{d}/{today}_pn_protokoll_V{i}.gpkg"
        while exists(dest_path):
            i += 1
            dest_path = f"{d}/{today}_pn_protokoll_V{i}.gpkg"

    shutil.copyfile(filepath, dest_path)
