# Imports
import warnings
import os
import geopandas as gpd
from ReadExcel import init_reading, load_input_data, append_columns, return_probes
from WriteQGIS import load_gpkg_data, write_gpkg, write_table_to_gpkg

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
gdf_group = load_gpkg_data(qg_path, "PN_Gruppe")
gdf_prt = load_gpkg_data(qg_path, "PN_Protokoll")

# Dataframe ausgeben
import fiona
for layer in fiona.listlayers(qg_path):
    try:
        test = gpd.read_file(qg_path, layer=layer)
        if "Nummer" in test.columns:
            print(layer, ":", test["Nummer"].min(), "bis", test["Nummer"].max())
    except:
        pass

#print(gdf_group.head())
#print(df.columns.tolist())
#print(df.head())
write_gpkg(df, gdf_group, gdf_prt, qg_path, return_probes(df))




