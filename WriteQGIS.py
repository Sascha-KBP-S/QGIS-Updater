# Imports
from os.path import exists
import pandas as pd
import geopandas as gpd
import pathlib
import shutil
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
    Liest einen Layer aus GPKG mit pyogrio Engine.
    Gibt ein GeoDataFrame zurück.
    """
    try:
        gdf = gpd.read_file(path, layer=layer, engine="pyogrio")
    except ValueError:
        # Wenn keine Geometrie gefunden wird, lese ohne Geometrie
        gdf = gpd.read_file(path, layer=layer, engine="pyogrio", ignore_geometry=True)

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


# Daten in GPKG Datenbank schreiben mit Geopandas
def write_gpkg(df, gdf_group, gdf_prt, filepath, probes_list):
    """
    Aktualisiert existierende Zeilen in der GPKG mit Geopandas.
    Wendet Value Maps an und schreibt korrekt in die Datei.

    Args:
        df: DataFrame mit Eingabedaten aus Excel
        gdf_group: GeoDataFrame PN_Gruppe Layer
        gdf_prt: GeoDataFrame PN_Protokoll Layer
        filepath: Pfad zur GPKG-Datei
        probes_list: Liste der zu aktualisierenden Probennummern
    """

    # Backup erstellen
    safe_copy(filepath)

    # Lade Value Maps
    try:
        with open("resources/value_maps_from_qgis.json", "r", encoding="utf-8") as f:
            value_maps = json.load(f)
    except FileNotFoundError:
        print("[WARNING] value_maps_from_qgis.json nicht gefunden. Verwende Fallback value_maps.json")
        value_maps = load_value_maps("value_maps.json")
    except json.JSONDecodeError as e:
        print(f"[WARNING] Fehler beim Lesen von value_maps_from_qgis.json: {e}")
        value_maps = load_value_maps("value_maps.json")

    # Aktualisiere PN_Protokoll
    print("Aktualisiere PN_Protokoll...")
    for probe in probes_list:
        # Finde Zeile in Excel-Dataframe
        df_mask = df["Nummer"] == probe
        if not df_mask.any():
            continue

        df_idx = df[df_mask].index[0]

        # Finde entsprechende Zeile in GPKG
        gpkg_mask = gdf_prt["Nummer"] == probe
        if not gpkg_mask.any():
            continue

        gpkg_idx = gdf_prt[gpkg_mask].index[0]

        # Aktualisiere jede Spalte
        for col in gdf_prt.columns:
            if col == "geometry":
                continue

            if col in df.columns:
                val = df.loc[df_idx, col]

                # Prüfe ob Wert leer ist (NaN, None, leerer String)
                is_empty = pd.isna(val) or (isinstance(val, str) and val.strip() == "")

                if not is_empty:
                    # Wert ist nicht leer -> aktualisieren
                    # Versuche Value Map Konvertierung für TEXT-Spalten
                    converted_val = convert_to_coded_value(col, val, value_maps, "PN_Protokoll")

                    # Konvertiere zum richtigen Datentyp
                    try:
                        if pd.api.types.is_bool_dtype(gdf_prt[col].dtype):
                            # Bool-Spalte: konvertiere 0.0/1.0 zu False/True
                            if isinstance(converted_val, (int, float)):
                                gdf_prt.at[gpkg_idx, col] = bool(int(converted_val))
                            else:
                                gdf_prt.at[gpkg_idx, col] = bool(converted_val)
                        elif pd.api.types.is_datetime64_any_dtype(gdf_prt[col].dtype):
                            # Datetime-Spalte: konvertiere zu Pandas Timestamp
                            dt_val = pd.to_datetime(converted_val)
                            # Entferne Timezone falls vorhanden um dtype-Kompatibilität sicherzustellen
                            if hasattr(dt_val, 'tz_localize'):
                                dt_val = dt_val.tz_localize(None)
                            gdf_prt.at[gpkg_idx, col] = dt_val
                        elif pd.api.types.is_float_dtype(gdf_prt[col].dtype):
                            gdf_prt.at[gpkg_idx, col] = float(converted_val)
                        elif pd.api.types.is_integer_dtype(gdf_prt[col].dtype):
                            gdf_prt.at[gpkg_idx, col] = int(float(converted_val))
                        else:
                            gdf_prt.at[gpkg_idx, col] = converted_val
                    except (ValueError, TypeError):
                        # Bei Konvertierungsfehlern: ursprünglichen Wert behalten
                        pass
                else:
                    # Wert ist leer -> setze explizit None/NULL in GPKG
                    gdf_prt.at[gpkg_idx, col] = None

    # Aktualisiere PN_Gruppe
    print("Aktualisiere PN_Gruppe...")
    for probe in probes_list:
        # Finde Zeile in Excel-Dataframe
        df_mask = df["Nummer"] == probe
        if not df_mask.any():
            continue

        df_idx = df[df_mask].index[0]
        grp_number = df.loc[df_idx, "Gruppe_Nummer"]

        if pd.isna(grp_number):
            continue

        # Finde entsprechende Zeile in GPKG
        gpkg_mask = gdf_group["Gruppe_Nummer"] == grp_number
        if not gpkg_mask.any():
            continue

        gpkg_idx = gdf_group[gpkg_mask].index[0]

        # Aktualisiere jede Spalte
        for col in gdf_group.columns:
            if col == "geometry":
                continue

            if col in df.columns:
                val = df.loc[df_idx, col]

                # Prüfe ob Wert leer ist (NaN, None, leerer String)
                is_empty = pd.isna(val) or (isinstance(val, str) and val.strip() == "")

                if not is_empty:
                    # Wert ist nicht leer -> aktualisieren
                    # Versuche Value Map Konvertierung für TEXT-Spalten
                    converted_val = convert_to_coded_value(col, val, value_maps, "PN_Gruppe")

                    # Konvertiere zum richtigen Datentyp
                    try:
                        if pd.api.types.is_bool_dtype(gdf_group[col].dtype):
                            # Bool-Spalte: konvertiere 0.0/1.0 zu False/True
                            if isinstance(converted_val, (int, float)):
                                gdf_group.at[gpkg_idx, col] = bool(int(converted_val))
                            else:
                                gdf_group.at[gpkg_idx, col] = bool(converted_val)
                        elif pd.api.types.is_datetime64_any_dtype(gdf_group[col].dtype):
                            # Datetime-Spalte: konvertiere zu Pandas Timestamp
                            dt_val = pd.to_datetime(converted_val)
                            # Entferne Timezone falls vorhanden um dtype-Kompatibilität sicherzustellen
                            if hasattr(dt_val, 'tz_localize'):
                                dt_val = dt_val.tz_localize(None)
                            gdf_group.at[gpkg_idx, col] = dt_val
                        elif pd.api.types.is_float_dtype(gdf_group[col].dtype):
                            gdf_group.at[gpkg_idx, col] = float(converted_val)
                        elif pd.api.types.is_integer_dtype(gdf_group[col].dtype):
                            gdf_group.at[gpkg_idx, col] = int(float(converted_val))
                        else:
                            gdf_group.at[gpkg_idx, col] = converted_val
                    except (ValueError, TypeError):
                        # Bei Konvertierungsfehlern: ursprünglichen Wert behalten
                        pass
                else:
                    # Wert ist leer -> setze explizit None/NULL in GPKG
                    gdf_group.at[gpkg_idx, col] = None

    # Schreibe Layers zurück mit Geopandas
    print("Schreibe GPKG...")

    try:
        # Konvertiere zu GeoDataFrame falls nötig
        if not isinstance(gdf_prt, gpd.GeoDataFrame):
            gdf_prt = gpd.GeoDataFrame(gdf_prt)

        # Schreibe PN_Protokoll (überschreibe nur diesen Layer, behalte andere)
        gdf_prt.to_file(filepath, layer="PN_Protokoll", driver="GPKG", engine="pyogrio")
        print("✓ PN_Protokoll geschrieben")
    except Exception as e:
        print(f"[ERROR] Fehler beim Schreiben von PN_Protokoll: {e}")

    try:
        # Konvertiere zu GeoDataFrame falls nötig
        if not isinstance(gdf_group, gpd.GeoDataFrame):
            gdf_group = gpd.GeoDataFrame(gdf_group)

        # Schreibe PN_Gruppe (überschreibe nur diesen Layer)
        gdf_group.to_file(filepath, layer="PN_Gruppe", driver="GPKG", engine="pyogrio")
        print("✓ PN_Gruppe geschrieben")
    except Exception as e:
        print(f"[ERROR] Fehler beim Schreiben von PN_Gruppe: {e}")

    print("GPKG erfolgreich aktualisiert!")





def safe_copy(filepath):
    """
    Erstellt ein Backup der GPKG-Datei mit aktuellem Datum.

    Args:
        filepath: Pfad zur GPKG-Datei die gesichert werden soll
    """
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


