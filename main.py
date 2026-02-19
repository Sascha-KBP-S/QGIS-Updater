# Imports
import warnings
import os
from ReadExcel import init_reading, load_input_data, append_columns, return_probes
from WriteQGIS import write_gpkg, read_layer
from ExcelUtils import col_index
from pyogrio import read_info
import json

os.environ["PROJ_LIB"] = r"C:\Users\SASBOITE\AppData\Local\anaconda3\envs\nuklidvektoren\Library\share\proj"
os.environ["GDAL_DATA"] = os.path.join(os.environ["CONDA_PREFIX"], "Library", "share", "gdal")

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

init_reading()

# Dateipfad zum PN-Protokoll
pn_path = r"G:\_Projekte\Stilllegungsplanung KKB\TP03_RM\VH01_A-CH\02_radProbe\21_GIS_Daten\01_Rad_Daten\Projekt\20260204_PN-Protokol_1bis594_QGIS.xlsm"
qg_path = "resources/pn_protokoll.gpkg"


# Funktion zum Extrahieren von Value Maps aus der GPKG
def extract_value_maps_from_gpkg(gpkg_path, layer_name):
    """
    Liest Feld-Definitionen und Value Maps aus einer QGIS-GPKG-Datei.
    Nutzt SQLite um auf die QGIS-Metadaten zuzugreifen.
    """
    import sqlite3

    try:
        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()

        print(f"\n[INFO] Feld-Definitionen fuer Layer '{layer_name}':")

        # Lese Spalten und deren Typen
        cursor.execute(f"PRAGMA table_info('{layer_name}')")
        columns = cursor.fetchall()

        print(f"\nSpalten in '{layer_name}':")
        for col in columns:
            col_name, col_type = col[1], col[2]
            print(f"  {col_name}: {col_type}")

        # Versuche QGIS-Metadaten zu lesen (Value Maps)
        try:
            cursor.execute("SELECT * FROM qgis_metadata LIMIT 1")
            metadata = cursor.fetchall()
            if metadata:
                print(f"\n[INFO] QGIS-Metadaten gefunden:")
                print(f"  {metadata}")
        except Exception as e:
            print(f"\n[INFO] Keine QGIS-Metadaten in der Tabelle: {e}")

        # Versuche qgis_projects Tabelle
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%qgis%'")
            qgis_tables = cursor.fetchall()
            if qgis_tables:
                print(f"\n[INFO] QGIS-Tabellen gefunden: {qgis_tables}")
        except Exception as e:
            print(f"[INFO] Keine QGIS-Tabellen gefunden: {e}")

        conn.close()
        return columns

    except Exception as e:
        print(f"[FEHLER] Konnte Feld-Definitionen nicht auslesen: {e}")
        import traceback
        traceback.print_exc()
        return None


# Dataframe source erstellen
df_source = load_input_data(pn_path)

# Gefilterten Dataframe erstellen
df = append_columns(df_source)
gdf_group = read_layer(qg_path, "PN_Gruppe")
gdf_prt = read_layer(qg_path, "PN_Protokoll")

# Extrahiere Value Maps fuer beide Layer
print("="*80)
print("EXTRAKTION VON FELD-DEFINITIONEN UND VALUE MAPS")
print("="*80)

info_prt = extract_value_maps_from_gpkg(qg_path, "PN_Protokoll")
print("\n" + "="*80)
info_grp = extract_value_maps_from_gpkg(qg_path, "PN_Gruppe")
print("="*80 + "\n")

# Dataframe ausgeben
# Zeile mit Probennummer 243 aus df (Excel)
row_243_df = df.loc[df["Nummer"] == 243]
print("Excel-Zeile fuer Probennummer 243 (gefiltert):")
print(row_243_df)

# Schreiben aktivieren
write_gpkg(
    df,
    gdf_group,
    gdf_prt,
    qg_path,
    return_probes(df),
    debug=True,
    debug_probe_numbers=[243],
)

# Nach dem Schreiben: GPKG neu einlesen
gdf_prt_after = read_layer(qg_path, "PN_Protokoll")
row_243_gpkg_after = gdf_prt_after.loc[gdf_prt_after["Nummer"] == 243]
print("\nGPKG-Zeile fuer Probennummer 243 (nach dem Schreiben) - PN_Protokoll:")
print(row_243_gpkg_after)

# Auch PN_Gruppe kontrollieren
gdf_group_after = read_layer(qg_path, "PN_Gruppe")
row_243_group_after = gdf_group_after.loc[gdf_group_after["Gruppe_Nummer"] == 214]
print("\nGPKG-Zeile fuer Probennummer 243 (nach dem Schreiben) - PN_Gruppe:")
print(row_243_group_after)
