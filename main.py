# Imports
import warnings
import os
import pandas as pd
from ReadExcel import init_reading, load_input_data, append_columns, return_probes
from WriteQGIS import write_gpkg, read_layer

os.environ["PROJ_LIB"] = r"C:\Users\SASBOITE\AppData\Local\anaconda3\envs\nuklidvektoren\Library\share\proj"
os.environ["GDAL_DATA"] = os.path.join(os.environ["CONDA_PREFIX"], "Library", "share", "gdal")

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", message=".*Measured.*geometry.*")
warnings.filterwarnings("ignore", message=".*Non-conformant content.*")


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
)

print("\n" + "="*80)
print("DATENTRANSFER ABGESCHLOSSEN")
print("="*80)

# Debug: Überprüfe die geschriebenen Daten für Probe 326
print("\n" + "="*80)
print("DEBUG: Überprüfung Probe 326 in geschriebener GPKG")
print("="*80)

try:
    # Lese die Layers neu ein
    gdf_prt_check = read_layer(qg_path, "PN_Protokoll")
    gdf_group_check = read_layer(qg_path, "PN_Gruppe")

    # Suche Probe 326 in PN_Protokoll
    row_prt = gdf_prt_check.loc[gdf_prt_check["Nummer"] == 326]
    if not row_prt.empty:
        print("\n[DEBUG] PN_Protokoll - Probe 326:")
        print(row_prt.to_string(index=False))
    else:
        print("\n[DEBUG] Probe 326 nicht in PN_Protokoll gefunden")

    # Suche Probe 326 in PN_Gruppe (über Gruppe_Nummer)
    probe_326_data = df.loc[df["Nummer"] == 326]
    if not probe_326_data.empty:
        grp_num = probe_326_data.iloc[0]["Gruppe_Nummer"]
        if pd.notna(grp_num):
            row_group = gdf_group_check.loc[gdf_group_check["Gruppe_Nummer"] == grp_num]
            if not row_group.empty:
                print("\n[DEBUG] PN_Gruppe - Gruppe_Nummer", int(grp_num), "(für Probe 326):")
                print(row_group.to_string(index=False))
            else:
                print("\n[DEBUG] Gruppe_Nummer", int(grp_num), "nicht in PN_Gruppe gefunden")
        else:
            print("\n[DEBUG] Probe 326 hat keine Gruppe_Nummer")
    else:
        print("\n[DEBUG] Probe 326 nicht im Excel-DataFrame gefunden")

except Exception as e:
    print(f"\n[ERROR] Fehler beim Debug-Check: {e}")
    import traceback
    traceback.print_exc()

