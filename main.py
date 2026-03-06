# Imports
import warnings
import os
import pandas as pd
from ReadExcel import init_reading, load_input_data, append_columns, return_probes
from WriteQGIS import write_gpkg, read_layer
from ExcelUtils import load_value_maps, decode_value

os.environ["PROJ_LIB"] = r"C:\Users\SASBOITE\AppData\Local\anaconda3\envs\nuklidvektoren\Library\share\proj"
os.environ["GDAL_DATA"] = os.path.join(os.environ["CONDA_PREFIX"], "Library", "share", "gdal")

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", message=".*Measured.*geometry.*")
warnings.filterwarnings("ignore", message=".*Non-conformant content.*")

init_reading()

# Dateipfade zum PN-Protokoll, QGIS-Datenbank
pn_path = r"G:\_Projekte\Stilllegungsplanung KKB\TP03_RM\VH01_A-CH\02_radProbe\21_GIS_Daten\01_Rad_Daten\Projekt\20260204_PN-Protokol_1bis594_QGIS.xlsm"
qg_path = "resources/pn_protokoll.gpkg"

# Excel Daten einlesen
df_source = load_input_data(pn_path)

# Dataframe mit korrekten Datentypen & Spaltennamen erstellen
df = append_columns(df_source)
gdf_group = read_layer(qg_path, "PN_Gruppe")
gdf_prt = read_layer(qg_path, "PN_Protokoll")

print("=" * 80)
print("DATENTRANSFER QGIS UPDATER")
print("=" * 80)
print(f"[OK] Excel gelesen: {len(df)} Zeilen")
print(f"[OK] GPKG PN_Protokoll geladen: {len(gdf_prt)} Zeilen")
print(f"[OK] GPKG PN_Gruppe geladen: {len(gdf_group)} Zeilen")
print(f"[OK] Value Maps werden von value_maps.json verwendet")
print("=" * 80 + "\n")

# Daten in die QGIS-Datenbank schreiben
write_gpkg(df, gdf_group, gdf_prt, qg_path, return_probes(df))

print("\n" + "=" * 80)
print("DATENTRANSFER ABGESCHLOSSEN")
print("=" * 80)



# Debug: Vergleiche Excel-Daten mit geschriebener GPKG für eine Probe

# Zu vergleichende Probennummer
probe = 583

if probe:
    print("\n" + "=" * 80)
    print(f"DEBUG: VERGLEICH Excel <-> GPKG fuer Probe {probe}")
    print("=" * 80)

    try:
        # Lade Value Maps für Dekodierung
        value_maps = load_value_maps("value_maps.json")

        # Lese die Layers neu ein
        gdf_prt_check = read_layer(qg_path, "PN_Protokoll")
        gdf_group_check = read_layer(qg_path, "PN_Gruppe")

        # Excel-Daten für diese Probe
        excel_row = df.loc[df["Nummer"] == probe]

        if excel_row.empty:
            print(f"\n[ERROR] Probe {probe} nicht im Excel-DataFrame gefunden!")
        else:
            excel_data = excel_row.iloc[0]


            # Hilfsfunktion für Werte-Vergleich
            def values_equal(v1, v2):
                if pd.isna(v1) and pd.isna(v2):
                    return True

                if v1 is None and v2 is None:
                    return True

                if pd.isna(v1) or pd.isna(v2):
                    return False

                return v1 == v2


            # ========== PN_PROTOKOLL Vergleich ==========
            print(f"\n{'-' * 80}")
            print(f"LAYER: PN_Protokoll")
            print(f"{'-' * 80}")

            gpkg_prt_row = gdf_prt_check.loc[gdf_prt_check["Nummer"] == probe]

            if gpkg_prt_row.empty:
                print(f"[WARNING] Probe {probe} nicht in PN_Protokoll gefunden!")
            else:
                gpkg_prt_data = gpkg_prt_row.iloc[0]

                # Alle Spalten aus Excel-DataFrame, die auch in GPKG existieren
                prt_columns = [col for col in excel_data.index if col in gpkg_prt_data.index]

                identical_count = 0
                diff_count = 0

                print("\n  Spaltenvergleich:")
                for col in sorted(prt_columns):
                    excel_val = excel_data[col]
                    gpkg_val = gpkg_prt_data[col]

                    # Dekodiere GPKG-Wert wenn es ein Index ist
                    gpkg_val_decoded = decode_value(gpkg_val, col, "PN_Protokoll", value_maps)

                    if values_equal(excel_val, gpkg_val_decoded):
                        identical_count += 1
                        print(f"  [OK] {col:30s} | {repr(excel_val):50s}")
                    else:
                        diff_count += 1
                        # Zeige dekodiert Wert wenn verschieden vom Index
                        if gpkg_val != gpkg_val_decoded:
                            print(
                                f"  [XX] {col:30s} | Excel: {repr(excel_val):35s} | GPKG: {repr(gpkg_val_decoded)} [Index: {gpkg_val}]")
                        else:
                            print(f"  [XX] {col:30s} | Excel: {repr(excel_val):35s} | GPKG: {repr(gpkg_val)}")

                print(f"\n  Zusammenfassung: {identical_count} identisch [OK], {diff_count} unterschiedlich [XX]")

            # ========== PN_GRUPPE Vergleich ==========
            print(f"\n{'-' * 80}")
            print(f"LAYER: PN_Gruppe")
            print(f"{'-' * 80}")

            grp_num = excel_data.get("Gruppe_Nummer")

            if pd.isna(grp_num):
                print(f"[INFO] Probe {probe} hat keine Gruppe_Nummer")
            else:
                gpkg_grp_row = gdf_group_check.loc[gdf_group_check["Gruppe_Nummer"] == grp_num]

                if gpkg_grp_row.empty:
                    print(f"[WARNING] Gruppe_Nummer {grp_num} nicht in PN_Gruppe gefunden!")
                else:
                    gpkg_grp_data = gpkg_grp_row.iloc[0]

                    # Alle Spalten aus Excel-DataFrame, die auch in GPKG existieren
                    grp_columns = [col for col in excel_data.index if col in gpkg_grp_data.index]

                    identical_count = 0
                    diff_count = 0

                    print("\n  Spaltenvergleich:")
                    for col in sorted(grp_columns):
                        excel_val = excel_data[col]
                        gpkg_val = gpkg_grp_data[col]

                        # Dekodiere GPKG-Wert wenn es ein Index ist
                        gpkg_val_decoded = decode_value(gpkg_val, col, "PN_Gruppe", value_maps)

                        if values_equal(excel_val, gpkg_val_decoded):
                            identical_count += 1
                            print(f"  [OK] {col:30s} | {repr(excel_val):50s}")
                        else:
                            diff_count += 1
                            # Zeige dekodiert Wert wenn verschieden vom Index
                            if gpkg_val != gpkg_val_decoded:
                                print(
                                    f"  [XX] {col:30s} | Excel: {repr(excel_val):35s} | GPKG: {repr(gpkg_val_decoded)} [Index: {gpkg_val}]")
                            else:
                                print(f"  [XX] {col:30s} | Excel: {repr(excel_val):35s} | GPKG: {repr(gpkg_val)}")

                    print(f"\n  Zusammenfassung: {identical_count} identisch [OK], {diff_count} unterschiedlich [XX]")

    except Exception as e:
        print(f"\n[ERROR] Fehler beim Debug-Check: {e}")
        import traceback

        traceback.print_exc()

    print("\n" + "=" * 80)
else:
    print("\n[WARNING] Keine gültige Probennummer zum Testen gefunden")
