# Imports
from os.path import exists

import pandas as pd
import geopandas as gpd
import sqlite3
import pathlib
import shutil
from datetime import datetime


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


# Daten in GPKG Datenbank schreiben - DIREKT MIT SQLite (KEINE GeoPandas/Fiona Konvertierung!)
def write_gpkg(df, gdf_group, gdf_prt, filepath, probes_list):
    """
    Schreibt Daten direkt in die GPKG-SQLite-Datenbank.
    Verhindert alle Datentyp-Konvertierungsprobleme durch direkten SQLite-Zugriff!
    """

    # Gemeinsame Spalten ermitteln
    common_cols_prt = [col for col in df.columns if col in gdf_prt.columns]
    common_cols_group = [col for col in df.columns if col in gdf_group.columns]

    # Backup erstellen
    safe_copy(filepath)

    # WICHTIG: Nicht mit DELETE arbeiten - das triggert spatialite-Funktionen!
    # Stattdessen: UPDATE verwenden - das ändert nur die Werte, nicht die Struktur
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()


# Daten in GPKG Datenbank schreiben - DIREKT MIT SQLite UPDATE (KEINE INSERT/DELETE)
def write_gpkg(df, gdf_group, gdf_prt, filepath, probes_list):
    """
    Aktualisiert existierende Zeilen in der GPKG direkt mit SQLite UPDATE.
    Keine Duplikate, keine Geometrie-Probleme, keine Klammern!
    """

    # Backup erstellen
    safe_copy(filepath)

    # Verbinde mit GPKG
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()

    # Schritt 1: PN_Protokoll mit SQL UPDATE aktualisieren
    print("Aktualisiere PN_Protokoll...")
    for probe in probes_list:
        # Probe in df finden
        df_row = df[df["Nummer"] == probe]
        if df_row.empty:
            continue

        df_idx = df_row.index[0]

        # Baue SQL UPDATE Pairs für alle Spalten aus gdf_prt
        update_pairs = []
        update_values = []

        for col in gdf_prt.columns:
            if col == "geometry":
                continue

            # Wert aus df nehmen, falls vorhanden, sonst behalte Original
            if col in df.columns:
                val = df.loc[df_idx, col]

                # Nur wenn Wert nicht NaN, üpdate
                if pd.notna(val):
                    # Konvertiere zu SQL-kompatiblem Wert
                    if pd.api.types.is_float_dtype(gdf_prt[col].dtype):
                        try:
                            sql_val = float(val)
                            update_pairs.append(f"`{col}` = ?")
                            update_values.append(sql_val)
                        except (ValueError, TypeError):
                            pass
                    elif pd.api.types.is_integer_dtype(gdf_prt[col].dtype):
                        try:
                            sql_val = int(float(val))
                            update_pairs.append(f"`{col}` = ?")
                            update_values.append(sql_val)
                        except (ValueError, TypeError):
                            pass
                    elif pd.api.types.is_datetime64_any_dtype(gdf_prt[col].dtype):
                        sql_val = str(pd.Timestamp(val))
                        update_pairs.append(f"`{col}` = ?")
                        update_values.append(sql_val)
                    else:
                        sql_val = str(val)
                        update_pairs.append(f"`{col}` = ?")
                        update_values.append(sql_val)

        # Führe UPDATE aus wenn es Änderungen gibt
        if update_pairs:
            update_values.append(probe)
            sql = f"UPDATE PN_Protokoll SET {', '.join(update_pairs)} WHERE Nummer = ?"
            try:
                cursor.execute(sql, update_values)
            except Exception:
                pass

    conn.commit()

    # Schritt 2: PN_Gruppe mit SQL UPDATE aktualisieren
    print("Aktualisiere PN_Gruppe...")
    for probe in probes_list:
        df_row = df[df["Nummer"] == probe]
        if df_row.empty:
            continue

        df_idx = df_row.index[0]
        grp_number = df_row.loc[df_idx, "Gruppe_Nummer"]

        if pd.isna(grp_number):
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
                        except (ValueError, TypeError):
                            pass
                    elif pd.api.types.is_integer_dtype(gdf_group[col].dtype):
                        try:
                            sql_val = int(float(val))
                            update_pairs.append(f"`{col}` = ?")
                            update_values.append(sql_val)
                        except (ValueError, TypeError):
                            pass
                    else:
                        sql_val = str(val)
                        update_pairs.append(f"`{col}` = ?")
                        update_values.append(sql_val)

        # Führe UPDATE aus
        if update_pairs:
            update_values.append(int(grp_number))
            sql = f"UPDATE PN_Gruppe SET {', '.join(update_pairs)} WHERE Gruppe_Nummer = ?"
            try:
                cursor.execute(sql, update_values)
            except Exception:
                pass

    conn.commit()
    conn.close()

    print("GPKG mit SQLite UPDATE aktualisiert (keine Duplikate, keine Klammern!)")


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
