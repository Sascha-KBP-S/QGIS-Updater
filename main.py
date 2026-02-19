# Imports
import warnings
import os
from ReadExcel import init_reading, load_input_data, append_columns, return_probes
from WriteQGIS import write_gpkg, read_layer

os.environ["PROJ_LIB"] = r"C:\Users\SASBOITE\AppData\Local\anaconda3\envs\nuklidvektoren\Library\share\proj"
os.environ["GDAL_DATA"] = os.path.join(os.environ["CONDA_PREFIX"], "Library", "share", "gdal")

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

init_reading()

# Dateipfad zum PN-Protokoll
pn_path = r"G:\_Projekte\Stilllegungsplanung KKB\TP03_RM\VH01_A-CH\02_radProbe\21_GIS_Daten\01_Rad_Daten\Projekt\20260204_PN-Protokol_1bis594_QGIS.xlsm"
qg_path = "resources/pn_protokoll.gpkg"


# Dataframe source erstellen
df_source = load_input_data(pn_path)

# Gefilterten Dataframe erstellen
df = append_columns(df_source)
gdf_group = read_layer(qg_path, "PN_Gruppe")
gdf_prt = read_layer(qg_path, "PN_Protokoll")

print("="*80)
print("DATENTRANSFER QGIS UPDATER")
print("="*80)
print(f"✓ Excel gelesen: {len(df)} Zeilen")
print(f"✓ GPKG PN_Protokoll geladen: {len(gdf_prt)} Zeilen")
print(f"✓ GPKG PN_Gruppe geladen: {len(gdf_group)} Zeilen")
print(f"✓ Value Maps werden von value_maps.json verwendet")
print("="*80 + "\n")

# Schreiben aktivieren
write_gpkg(
    df,
    gdf_group,
    gdf_prt,
    qg_path,
    return_probes(df),
    debug=False,
)

print("\n" + "="*80)
print("DATENTRANSFER ABGESCHLOSSEN")
print("="*80)
