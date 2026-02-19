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

# Dataframe ausgeben
# Zeile mit Probennummer 44
row_44 = gdf_prt.loc[gdf_prt["Nummer"] == 45]

print("Werte der Probennummer 44:")
print(row_44)

if not row_44.empty:
    print("\nDatentypen der Probennummer 44:")
    print(row_44.iloc[0].apply(type))
else:
    print("\nProbe 44 nicht gefunden - wird durch Daten-Update erstellt")


print(gdf_group.head())
print(df.columns.tolist())
print(df.head())
write_gpkg(df, gdf_group, gdf_prt, qg_path, return_probes(df))
