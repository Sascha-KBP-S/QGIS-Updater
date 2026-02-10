# Imports
import pandas as pd
import geopandas as gpd
import fiona
from fiona import Env


# GeoPackage einlesen
def read_layer(path, layer):
    try:
        return gpd.read_file(path, layer=layer, engine="fiona")
    except ValueError:
        # Wenn keine echte Geometrie gefunden wurde
        return gpd.read_file(path, layer=layer, engine="fiona", ignore_geometry=True)


# Daten in neue GPGK Datenbank schreiben
def write_gpkg(df, gdf_group, gdf_prt, filepath, probes_list):
    # Gemeinsame Spalten ermitteln
    common_cols_prt = [col for col in df.columns if col in gdf_prt.columns]
    common_cols_group = [col for col in df.columns if col in gdf_group.columns]

    for probe in probes_list:

        # Probe in df finden
        df_row = df[df["Nummer"] == probe]
        if df_row.empty:
            print(f"Probe {probe} nicht in df gefunden.")
            continue

        df_idx = df_row.index[0]

        # Probe in PN_Protokoll finden
        gdf_prt_row = gdf_prt[gdf_prt["Nummer"] == probe]

        if not gdf_prt_row.empty:
            prt_idx = gdf_prt_row.index[0]
            for col in common_cols_prt:
                val = df.loc[df_idx, col]
                if pd.notna(val):
                    gdf_prt.loc[prt_idx, col] = val
        else:
            print(f"Keine Zeile in PN_Protokoll für Probe {probe} gefunden.")

        # Zugehörige Gruppe in df abfragen
        grp_number = df_row.loc[df_idx, "Gruppe_Nummer"]

        if pd.isna(grp_number):
            print(f"Probe {probe} hat keine Gruppe_Nummer – übersprungen.")
            continue

        # Gruppe im gpkg matchen
        gdf_group_row = gdf_group[gdf_group["Gruppe_Nummer"] == grp_number]

        if not gdf_group_row.empty:
            grp_idx = gdf_group_row.index[0]
            for col in common_cols_group:
                val = df.loc[df_idx, col]
                if pd.notna(val):
                    gdf_group.loc[grp_idx, col] = val
        else:
            print(f"Gruppe {grp_number} nicht in PN_Gruppe gefunden.")

    # Schreiben
    gdf_group.to_file(filepath, driver="GPKG", layer="PN_Gruppe", engine="fiona")

    write_table_to_gpkg(gdf_prt, filepath, "PN_Protokoll")


def write_table_to_gpkg(df: pd.DataFrame, filepath: str, layer_name: str):
    # datetime-Spalten ohne Zeitzone machen
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    # Pandas-Typen → Fiona-Typen mappen
    def map_dtype(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "int"
        elif pd.api.types.is_float_dtype(dtype):
            return "float"
        elif pd.api.types.is_bool_dtype(dtype):
            return "bool"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "datetime"
        else:
            return "string"

    schema = {
    "geometry": "None",
    "properties": {col: map_dtype(df[col].dtype) for col in df.columns}
}


    # Werte vor dem Schreiben kompatibel machen
    def normalize_value(val):
        if pd.isna(val):
            return None

        # Strings
        if isinstance(val, str) or pd.api.types.is_string_dtype(type(val)):
            return val

        # Floats / ints / bool
        if isinstance(val, (int, float, bool)):
            return val

        # Datumswerte
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime()  # ohne TZ

        return str(val)

    # Schreiben
    with Env():
        with fiona.open(
                filepath,
                mode="w",
                driver="GPKG",
                schema=schema,
                layer=layer_name
        ) as dst:
            for _, row in df.iterrows():
                dst.write({
                    "geometry": None,
                    "properties": {col: normalize_value(row[col]) for col in df.columns}
                })
