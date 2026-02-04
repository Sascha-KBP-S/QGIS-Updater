# Imports
import time

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from ExcelUtils import col_index, config


# PN-Protokoll einlesen, dabei die ersten 4 Zeilen überspringen
def load_input_data(filepath: str):
    return pd.read_excel(filepath, skiprows=4)


# Probenobjekte definieren
def return_probes(df: DataFrame):
    probes_list = np.array([])

    seen = set()
    for idx, pb in enumerate(df["Proben-Nr."]):
        # Überprüfe, ob der Wert ein String ist
        if isinstance(pb, str):
            probe_num = int(pb)

        # Prüfe, ob die Probe bereits in der Menge ist
        if probe_num not in seen:
            seen.add(probe_num)
            probes_list = np.append(probes_list, probe_num)
            print(probe_num)
        else:
            print(f"Doppelte Probennummer vorhanden: {pb}")

    return probes_list


# Pandas Anzeigeoptionen definieren
def init_reading():
    pd.set_option('future.no_silent_downcasting', True)
    pd.set_option("display.max_columns", None)  # Mehr Spalten anzeigen
    pd.set_option("display.width", 200)  # Gesamtbreite in Zeichen


# Spalten des PN-Protokolls in neuen, gefilterten Dataframe einlesen: (Benennung wie in QGIS)
def append_columns(df_src: DataFrame):
    df = pd.DataFrame({
        # Probennummer
        "Auftrags_Nummer": pd.to_numeric(df_src.iloc[:, col_index("A")], errors="coerce").astype(pd.Int64Dtype()),
        "Serien_Nummer": pd.to_numeric(df_src.iloc[:, col_index("B")], errors="coerce").astype(pd.Int64Dtype()),
        "Nummer": pd.to_numeric(df_src.iloc[:, col_index("C")], errors="coerce").astype(pd.Int64Dtype()),
        "Nummer_Bezeichnung": (df_src.iloc[:, col_index("D")]).astype(pd.StringDtype()),
        "Gruppe_Nummer": pd.to_numeric(df_src.iloc[:, col_index("E")], errors="coerce").astype(pd.Int64Dtype()),
        "ung_Nummer_Chbx": (df_src.iloc[:, col_index("F")]).astype(pd.StringDtype()),
        "ung_Nummer": pd.to_numeric(df_src.iloc[:, col_index("G")], errors="coerce").astype(pd.Int64Dtype()),

        # P-Nr.
        # -> nicht gefunden

        # Datum
        "pue_date":(df_src.iloc[:, col_index("I")]).astype(pd.StringDtype()),

        # Uhrzeit
        "Datum_Zeit": (df_src.iloc[:, col_index("J")]).astype(pd.StringDtype()),

        # Probenahmezweck
        "pn_zweck": (df_src.iloc[:, col_index("K")]).astype(pd.StringDtype()),

        # Auftrag
        "Auftrag": (df_src.iloc[:, col_index("L")]).astype(pd.StringDtype()),

        # Auftraggeber
        "Auftraggeber": (df_src.iloc[:, col_index("M")]).astype(pd.StringDtype()),

        # Projektleiter
        "Projektleiter": (df_src.iloc[:, col_index("N")]).astype(pd.StringDtype()),

        # Probentyp
        "Probentyp": (df_src.iloc[:, col_index("O")]).astype(pd.StringDtype()),

        # Materialart
        "Materialart": (df_src.iloc[:, col_index("P")]).astype(pd.StringDtype()),

        # Lage der Probenahmestelle
        "Gebaeude": (df_src.iloc[:, col_index("Q")]).astype(pd.StringDtype()),
        "Raum_Herkunft": (df_src.iloc[:, col_index("R")]).astype(pd.StringDtype()),
        "Raum_aktuell": (df_src.iloc[:, col_index("S")]).astype(pd.StringDtype()),
        "x_Raum": pd.to_numeric(df_src.iloc[:, col_index("T")], errors="coerce").astype(float),
        "y_Raum": pd.to_numeric(df_src.iloc[:, col_index("U")], errors="coerce").astype(float),
        "Hoehe": (df_src.iloc[:, col_index("V")]).astype(pd.StringDtype()),
        "Geschoss": (df_src.iloc[:, col_index("W")]).astype(pd.StringDtype()),
        "x": pd.to_numeric(df_src.iloc[:, col_index("X")], errors="coerce").astype(float),
        "y": pd.to_numeric(df_src.iloc[:, col_index("Y")], errors="coerce").astype(float),
        "Hoehe_absolut": pd.to_numeric(df_src.iloc[:, col_index("Z")], errors="coerce").astype(float),
        "Lagebeschreibung": (df_src.iloc[:, col_index("AA")]).astype(pd.StringDtype()),

        # Bauteil





    })
    return df


# Dataframe ausgeben
# print(df_filtered[["Summe Beta-Gamma", "Summe Alpha", "Verhältnis Beta-Gamma/Alpha", "∑ges-α / ∑ges-B"]].to_string(index=false))


'''
print(df_filtered[[
    "Masseinheit",
    "Co60-Wert",
    "Co60-Faktor",
    "Am241-Faktor",
    "Am-241(RC)-Faktor",
    "Ag110m+-Faktor",
    "Cm-243/244-Faktor",
    "Pu-238-Faktor",
    "Pu-239/240-Faktor",
    "Sb125+-Faktor",
    "Ni-63-Faktor",
    "Cs137+-Faktor",
    "Eu154-Faktor",
    "Sr-90-Faktor",
    "Fe-55-Faktor",
    "Ag108m+-Faktor",
    "U-233/234-Faktor",
    "U-238-Faktor",
]].to_string(index=False))
'''

'''
# Maximale Faktoren mit Nukliden ausgeben
print("\nMaximale Faktoren")
for key, value in nuclide_max.items():
    print(f"{key}: {value}")
'''
