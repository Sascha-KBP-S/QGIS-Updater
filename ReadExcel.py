# Imports
import time

import numpy as np
import pandas as pd
from pandas import DataFrame
from sympy import true, false

from ExcelUtils import col_index, config


# PN-Protokoll einlesen, dabei die ersten 4 Zeilen überspringen
def load_input_data(filepath: str):
    return pd.read_excel(filepath, skiprows=4)


# Probenliste definieren (Anzahl gültige Proben)
def return_probes(df: DataFrame):
    seen = set()
    probes = []

    for num in df["Nummer"]:
        if not pd.isna(num):
            if num in seen:
                print(f"Doppelte Probennummer vorhanden: {num}")
            else:
                seen.add(num)
                probes.append(num)

    return np.array(probes, dtype=int)


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
        "ung_Nummer_Chbx": (df_src.iloc[:, col_index("F")].astype(pd.StringDtype()).apply(bool_str)),
        "ung_Nummer": pd.to_numeric(df_src.iloc[:, col_index("G")], errors="coerce").astype(pd.Int64Dtype()),

        # P-Nr.
        # -> nicht gefunden

        # Datum
        "Datum": (df_src.iloc[:, col_index("I")]).astype(pd.StringDtype()),

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
        "Beprobungsflaeche": (df_src.iloc[:, col_index("AB")]).astype(pd.StringDtype()),
        "System": (df_src.iloc[:, col_index("AC")]).astype(pd.StringDtype()),
        "Anlagenkennzeichnung": (df_src.iloc[:, col_index("AD")]).astype(pd.StringDtype()),
        "Beschreibung": (df_src.iloc[:, col_index("AE")]).astype(pd.StringDtype()),
        "Bauteil_laenge": pd.to_numeric(df_src.iloc[:, col_index("AF")], errors="coerce").astype(float),
        "Bauteil_breite": pd.to_numeric(df_src.iloc[:, col_index("AG")], errors="coerce").astype(float),
        "Bauteil_hoehe": pd.to_numeric(df_src.iloc[:, col_index("AH")], errors="coerce").astype(float),
        "Bauteil_dicke": pd.to_numeric(df_src.iloc[:, col_index("AI")], errors="coerce").astype(float),
        "Bauteil_nutzungsart": (df_src.iloc[:, col_index("AJ")]).astype(pd.StringDtype()),
        "Oberflaechenbeschaffenheit": (df_src.iloc[:, col_index("AK")]).astype(pd.StringDtype()),
        "Verdacht": (df_src.iloc[:, col_index("AL")]).astype(pd.StringDtype()),
        "Besonderheiten_PN_Stelle": (df_src.iloc[:, col_index("AM")]).astype(pd.StringDtype()),

        # Foto-Nr.
        "Foto1": (df_src.iloc[:, col_index("AN")]).astype(pd.StringDtype()),  # übersicht
        "Foto2": (df_src.iloc[:, col_index("AO")]).astype(pd.StringDtype()),  # Detail
        "Foto3": (df_src.iloc[:, col_index("AP")]).astype(pd.StringDtype()),  # Anlagen-kennzeichen
        "Foto4": (df_src.iloc[:, col_index("AQ")]).astype(pd.StringDtype()),  # Detail
        "Foto5": (df_src.iloc[:, col_index("AR")]).astype(pd.StringDtype()),  # Probenbehälter offen
        "Foto6": (df_src.iloc[:, col_index("AS")]).astype(pd.StringDtype()),  # Probenbehälter hochkant

        # Ortsdosisleistung an der PN-Stelle
        "Mess_Typ_Stelle": (df_src.iloc[:, col_index("AT")]).astype(pd.StringDtype()),
        "Messwert": pd.to_numeric(df_src.iloc[:, col_index("AU")], errors="coerce").astype(float),
        "Messabstand": pd.to_numeric(df_src.iloc[:, col_index("AV")], errors="coerce").astype(float),

        # Oberflächenkontamination Direktmessung
        "Mess_Typ_DM": (df_src.iloc[:, col_index("AW")]).astype(pd.StringDtype()),
        "Geraetnr_DM": (df_src.iloc[:, col_index("AX")]).astype(pd.StringDtype()),
        "Ni_Alpha": pd.to_numeric(df_src.iloc[:, col_index("AY")], errors="coerce").astype(float),
        "Ni_Beta": pd.to_numeric(df_src.iloc[:, col_index("AZ")], errors="coerce").astype(float),
        "vPN_Alpha": pd.to_numeric(df_src.iloc[:, col_index("BA")], errors="coerce").astype(float),
        "vPN_Beta": pd.to_numeric(df_src.iloc[:, col_index("BB")], errors="coerce").astype(float),
        "nPN_Alpha": pd.to_numeric(df_src.iloc[:, col_index("BC")], errors="coerce").astype(float),
        "nPN_Beta": pd.to_numeric(df_src.iloc[:, col_index("BD")], errors="coerce").astype(float),

        # Probenahme allgemeine Angaben
        "Material_spez": (df_src.iloc[:, col_index("BE")]).astype(pd.StringDtype()),
        "PN_Verfahren": (df_src.iloc[:, col_index("BF")]).astype(pd.StringDtype()),
        "PN_Geraet": (df_src.iloc[:, col_index("BG")]).astype(pd.StringDtype()),
        "PN_laenge": pd.to_numeric(df_src.iloc[:, col_index("BH")], errors="coerce").astype(float),
        "PN_breite": pd.to_numeric(df_src.iloc[:, col_index("BI")], errors="coerce").astype(float),
        "PN_flaeche": pd.to_numeric(df_src.iloc[:, col_index("BJ")], errors="coerce").astype(float),
        "Ma_Zustand": (df_src.iloc[:, col_index("BK")]).astype(pd.StringDtype()),
        "Ma_Beschaffenheit": (df_src.iloc[:, col_index("BL")]).astype(pd.StringDtype()),
        "PN_Verlust": pd.to_numeric(df_src.iloc[:, col_index("BM")], errors="coerce").astype(float),
        "Zusatzinformationen": (df_src.iloc[:, col_index("BN")]).astype(pd.StringDtype()),

        # Feststoffprobenahme
        "FSPN_dn": pd.to_numeric(df_src.iloc[:, col_index("BO")], errors="coerce").astype(float),
        "FSPN_tiefe_von": pd.to_numeric(df_src.iloc[:, col_index("BP")], errors="coerce").astype(float),
        "FSPN_tiefe_bis": pd.to_numeric(df_src.iloc[:, col_index("BR")], errors="coerce").astype(float),
        "Probe_EP_MP": (df_src.iloc[:, col_index("BS")]).astype(pd.StringDtype()),
        "FSPN_Anzahl": pd.to_numeric(df_src.iloc[:, col_index("BT")], errors="coerce").astype(pd.Int64Dtype()),

        # Wischprobenahme
        "WP_Material": (df_src.iloc[:, col_index("BU")]).astype(pd.StringDtype()),
        "WP_dn": pd.to_numeric(df_src.iloc[:, col_index("BV")], errors="coerce").astype(float),
        "WP_An_Fl": pd.to_numeric(df_src.iloc[:, col_index("BW")], errors="coerce").astype(pd.Int64Dtype()),
        "WP_Anzahl": pd.to_numeric(df_src.iloc[:, col_index("BX")], errors="coerce").astype(pd.Int64Dtype()),

        # Kernbohrung
        "KB_dn": pd.to_numeric(df_src.iloc[:, col_index("BY")], errors="coerce").astype(float),
        "KB_tiefe_von": pd.to_numeric(df_src.iloc[:, col_index("BZ")], errors="coerce").astype(float),
        "KB_tiefe_bis": pd.to_numeric(df_src.iloc[:, col_index("CA")], errors="coerce").astype(float),
        "KB_art": (df_src.iloc[:, col_index("CB")]).astype(pd.StringDtype()),
        "KB_beschreibung": (df_src.iloc[:, col_index("CC")]).astype(pd.StringDtype()),

        # Angaben zur Probe
        "AP_behaelter": (df_src.iloc[:, col_index("CD")]).astype(pd.StringDtype()),
        "AP_masse": pd.to_numeric(df_src.iloc[:, col_index("CE")], errors="coerce").astype(float),
        "AP_hochkant": pd.to_numeric(df_src.iloc[:, col_index("CF")], errors="coerce").astype(float),
        "AP_fuellgrad": pd.to_numeric(df_src.iloc[:, col_index("CG")], errors="coerce").astype(float),
        # "AP_kreissegment": pd.to_numeric(df_src.iloc[:, col_index("CH")], errors="coerce").astype(float), //müsste man berechnen mit Radius, da nur Füllhöhe gegeben
        "AP_dichte": pd.to_numeric(df_src.iloc[:, col_index("CI")], errors="coerce").astype(float),
        "AP_NEimpuls": pd.to_numeric(df_src.iloc[:, col_index("CJ")], errors="coerce").astype(float),
        "AP_Bruttoimpuls": pd.to_numeric(df_src.iloc[:, col_index("CK")], errors="coerce").astype(float),
        "AP_Nettoimpuls": pd.to_numeric(df_src.iloc[:, col_index("CL")], errors="coerce").astype(float),
        "AP_ortsdosis": pd.to_numeric(df_src.iloc[:, col_index("CM")], errors="coerce").astype(float),

        # Bemerkungen
        "Bm_Freitxt": (df_src.iloc[:, col_index("CN")]).astype(pd.StringDtype()),

        # Probenehmer
        "PN_team": (df_src.iloc[:, col_index("CO")]).astype(pd.StringDtype()),
        "pn_nehmer1": (df_src.iloc[:, col_index("CP")]).astype(pd.StringDtype()),
        "pn_nehmer2": (df_src.iloc[:, col_index("CQ")]).astype(pd.StringDtype()),

        # Probenübergabe
        "pue_empfaenger": (df_src.iloc[:, col_index("CR")]).astype(pd.StringDtype()),
        "pue_date": (df_src.iloc[:, col_index("CS")]).astype(pd.StringDtype()),

    })
    return df


def bool_str(value) -> str:
    if pd.isna(value) or value == "":
        return "false"
    return "true"
