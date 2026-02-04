# Imports
from ReadExcel import init_reading, load_input_data, append_columns

init_reading()

# Dateipfad zum PN-Protokoll
pn_path = r"G:\_Projekte\Stilllegungsplanung KKB\TP03_RM\VH01_A-CH\02_radProbe\21_GIS_Daten\01_Rad_Daten\Projekt\20260204_PN-Protokol_1bis594_QGIS.xlsm"

# Dataframe source erstellen
df_source = load_input_data(pn_path)

# Gefilterten Dataframe erstellen
df = append_columns(df_source)

# Dataframe ausgeben
print(df)