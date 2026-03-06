# Imports
import pandas as pd
import geopandas as gpd
import pathlib
import shutil
import json
from datetime import datetime
import numpy as np


#  Konstanten
BOOLEAN_COLUMNS = {"ung_Nummer_Chbx", "Reparatur"}
DATETIME_COLUMNS = {"pue_date", "Datum", "Datum_Zeit"}
LAYER_PN_PROTOKOLL = "PN_Protokoll"
LAYER_PN_GRUPPE = "PN_Gruppe"


#  Value Mapping laden
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


#  GPKG lesen
def read_layer(path, layer):
    """Liest einen Layer aus GPKG mit pyogrio Engine."""
    try:
        return gpd.read_file(path, layer=layer, engine="pyogrio")
    except ValueError:
        return gpd.read_file(path, layer=layer, engine="pyogrio", ignore_geometry=True)


#  Value Map konvertierung
def convert_to_coded_value(column_name, value, value_maps, layer_name):
    """
    Konvertiert String-Wert zu Index basierend auf Value Map.

    Returns:
        Index (int) wenn gefunden, sonst Original-Wert
    """
    if pd.isna(value):
        return value

    value_str = str(value).strip()
    col_map = value_maps.get(layer_name, {}).get(column_name, {})

    return col_map.get(value_str, value)


#  Datentyp konvertierung
def cast_to_target_dtype(value, target_dtype, column_name=None):
    """Konvertiert Wert zum Ziel-Datentyp mit Spezialbehandlung."""

    # Boolean-Spalten: True/False (nie None)
    if column_name in BOOLEAN_COLUMNS:
        if pd.isna(value) or value == "":
            return False
        # Prüfe auf Python bool UND NumPy bool_
        if isinstance(value, (bool, np.bool_)):
            return bool(value)  # Konvertiere zu Python bool
        # Für andere Typen: True wenn Wert vorhanden
        return True

    # NULL bei NaN/None
    if pd.isna(value):
        return None

    # Datetime: pd.Timestamp ohne Timezone
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        dt_val = pd.Timestamp(value) if not isinstance(value, pd.Timestamp) else value
        return dt_val.tz_localize(None) if dt_val.tzinfo else dt_val

    if pd.api.types.is_datetime64_any_dtype(target_dtype):
        dt_val = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt_val):
            return None
        return dt_val.tz_localize(None) if hasattr(dt_val, 'tz') and dt_val.tz else dt_val

    # Boolean-Dtype
    if pd.api.types.is_bool_dtype(target_dtype):
        return bool(int(value)) if isinstance(value, (int, float, np.integer, np.floating)) else bool(value)

    # Numerische Typen
    if isinstance(target_dtype, pd.Float32Dtype) or str(target_dtype) == "Float32":
        try:
            return np.float32(value)
        except (ValueError, TypeError):
            return value

    if pd.api.types.is_float_dtype(target_dtype):
        return float(value)

    if pd.api.types.is_integer_dtype(target_dtype):
        return int(float(value))

    return value


#  Layer aktualisieren
def update_layer(df, gdf, probes_list, value_maps, layer_name, match_column="Nummer"):
    """
    Aktualisiert einen Layer (PN_Protokoll oder PN_Gruppe).

    Args:
        df: Source DataFrame (Excel)
        gdf: Target GeoDataFrame (GPKG Layer)
        probes_list: Liste der zu aktualisierenden Proben
        value_maps: Value Maps Dictionary
        layer_name: Name des Layers ("PN_Protokoll" oder "PN_Gruppe")
        match_column: Spalte zum Matchen (Standard: "Nummer")
    """
    print(f"Aktualisiere {layer_name}...")

    for probe in probes_list:
        # Finde Zeilen in beiden DataFrames
        df_mask = df["Nummer"] == probe
        if not df_mask.any():
            continue

        df_idx = df[df_mask].index[0]

        # Bei PN_Gruppe über Gruppe_Nummer matchen
        if layer_name == LAYER_PN_GRUPPE:
            grp_number = df.loc[df_idx, "Gruppe_Nummer"]
            if pd.isna(grp_number):
                continue
            gpkg_mask = gdf["Gruppe_Nummer"] == grp_number
        else:
            gpkg_mask = gdf[match_column] == probe

        if not gpkg_mask.any():
            continue

        gpkg_idx = gdf[gpkg_mask].index[0]

        # Aktualisiere Spalten
        for col in gdf.columns:
            if col == "geometry" or col not in df.columns:
                continue

            val = df.loc[df_idx, col]
            is_empty = pd.isna(val) or (isinstance(val, str) and val.strip() == "")

            if not is_empty:
                # Value Map anwenden (nur bei String-Spalten, nicht bei Booleans)
                converted_val = val
                if col not in BOOLEAN_COLUMNS and \
                   (pd.api.types.is_object_dtype(gdf[col].dtype) or
                    pd.api.types.is_string_dtype(gdf[col].dtype)):
                    converted_val = convert_to_coded_value(col, val, value_maps, layer_name)

                # Datentyp-Konvertierung
                try:
                    gdf.at[gpkg_idx, col] = cast_to_target_dtype(
                        converted_val, gdf[col].dtype, column_name=col
                    )
                except (ValueError, TypeError):
                    pass  # Behalte Original-Wert bei Fehler
            else:
                # Leere Werte: NA für Booleans, None für Rest
                gdf.at[gpkg_idx, col] = pd.NA if col in BOOLEAN_COLUMNS else None


#  Datentypen vorbereiten
def prepare_datatypes(gdf_prt, gdf_group):
    """Stelle sicher, dass alle Spalten korrekte Datentypen haben."""
    # Boolean-Spalten
    for col in BOOLEAN_COLUMNS:
        if col in gdf_prt.columns:
            gdf_prt[col] = gdf_prt[col].astype(pd.BooleanDtype())
        if col in gdf_group.columns:
            gdf_group[col] = gdf_group[col].astype(pd.BooleanDtype())

    # Datetime-Spalten
    for col in DATETIME_COLUMNS:
        if col in gdf_prt.columns:
            gdf_prt[col] = pd.to_datetime(gdf_prt[col], errors="coerce")
        if col in gdf_group.columns:
            gdf_group[col] = pd.to_datetime(gdf_group[col], errors="coerce")


#  Layer schreiben
def write_layer(gdf, filepath, layer_name):
    """Schreibt einen Layer in GPKG."""
    try:
        if not isinstance(gdf, gpd.GeoDataFrame):
            gdf = gpd.GeoDataFrame(gdf)

        gdf.to_file(filepath, layer=layer_name, driver="GPKG", engine="pyogrio")
        print(f"✓ {layer_name} geschrieben")
        return True
    except Exception as e:
        print(f"[ERROR] Fehler beim Schreiben von {layer_name}: {e}")
        return False


#  Daten Schreiben
def write_gpkg(df, gdf_group, gdf_prt, filepath, probes_list):
    """
    Aktualisiert existierende Zeilen in der GPKG mit Geopandas.

    Args:
        df: DataFrame mit Eingabedaten aus Excel
        gdf_group: GeoDataFrame PN_Gruppe Layer
        gdf_prt: GeoDataFrame PN_Protokoll Layer
        filepath: Pfad zur GPKG-Datei
        probes_list: Liste der zu aktualisierenden Probennummern
    """
    # Datentypen vorbereiten
    prepare_datatypes(gdf_prt, gdf_group)

    # Backup erstellen
    safe_copy(filepath)

    # Value Maps laden
    try:
        with open("value_maps.json", "r", encoding="utf-8") as f:
            value_maps = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[WARNING] value_maps_from_qgis.json nicht verfügbar: {e}")

    # Layer aktualisieren (DRY!)
    update_layer(df, gdf_prt, probes_list, value_maps, LAYER_PN_PROTOKOLL)
    update_layer(df, gdf_group, probes_list, value_maps, LAYER_PN_GRUPPE)

    # Layer schreiben
    print("Schreibe GPKG...")
    success_prt = write_layer(gdf_prt, filepath, LAYER_PN_PROTOKOLL)
    success_grp = write_layer(gdf_group, filepath, LAYER_PN_GRUPPE)

    if success_prt and success_grp:
        print("GPKG erfolgreich aktualisiert!")
    else:
        print("[WARNING] GPKG teilweise aktualisiert - siehe Fehler oben")


#  Sicherheitskopie des aktuellen Stands erstellen
def safe_copy(filepath):
    """Erstellt ein Backup der GPKG-Datei mit aktuellem Datum."""
    p = pathlib.Path(filepath)
    today = datetime.today().strftime("%Y%m%d")

    # Finde freien Dateinamen
    dest_path = p.parent / f"{today}_pn_protokoll.gpkg"
    counter = 1
    while dest_path.exists():
        dest_path = p.parent / f"{today}_pn_protokoll_V{counter}.gpkg"
        counter += 1

    shutil.copyfile(filepath, dest_path)

