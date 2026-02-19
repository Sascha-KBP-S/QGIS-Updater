# Imports
import time
import json
import os

import numpy as np
import pandas as pd
from pandas import DataFrame
from sympy import true, false

from ExcelUtils import col_index, config


# Value Maps laden
def load_value_maps(json_path="value_maps.json"):
    """Lädt Value Map Definitionen aus einer JSON-Datei."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


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

    return probes


# Pandas Anzeigeoptionen definieren
def init_reading():
    pd.set_option('future.no_silent_downcasting', True)
    pd.set_option("display.max_columns", None)  # Mehr Spalten anzeigen
    pd.set_option("display.width", 200)  # Gesamtbreite in Zeichen


# Spalten des PN-Protokolls in neuen, gefilterten Dataframe einlesen: (Benennung wie in QGIS)
def append_columns(df_src: DataFrame):
    # Lade Value Maps
    value_maps = load_value_maps("value_maps.json")
    prt_maps = value_maps.get("PN_Protokoll", {})
    grp_maps = value_maps.get("PN_Gruppe", {})

    df = pd.DataFrame({
        # Probennummer
        "Auftrags_Nummer": pd.to_numeric(df_src.iloc[:, col_index("A")], errors="coerce").astype(pd.Int64Dtype()),
        "Serien_Nummer": pd.to_numeric(df_src.iloc[:, col_index("B")], errors="coerce").astype(pd.Int64Dtype()),
        "Nummer": pd.to_numeric(df_src.iloc[:, col_index("C")], errors="coerce").astype(pd.Int64Dtype()),
        "Nummer_Bezeichnung": df_src.iloc[:, col_index("D")].fillna("").astype(str),
        "Gruppe_Nummer": pd.to_numeric(df_src.iloc[:, col_index("E")], errors="coerce").astype(pd.Int64Dtype()),
        "ung_Nummer_Chbx": df_src.iloc[:, col_index("F")].apply(bool_to_float),
        "ung_Nummer": pd.to_numeric(df_src.iloc[:, col_index("G")], errors="coerce").astype(float),

        # P-Nr.
        # -> nicht gefunden

        # Datum
        "Datum": pd.to_datetime(df_src.iloc[:, col_index("I")], errors="coerce"),

        # Uhrzeit
        "Datum_Zeit": pd.to_datetime(df_src.iloc[:, col_index("J")], errors="coerce"),

        # Probenahmezweck
        "pn_zweck": df_src.iloc[:, col_index("K")].fillna("").astype(str),

        # Auftrag
        "Auftrag": df_src.iloc[:, col_index("L")].fillna("").astype(str),

        # Auftraggeber
        "Auftraggeber": df_src.iloc[:, col_index("M")].fillna("").astype(str),

        # Projektleiter
        "Projektleiter": df_src.iloc[:, col_index("N")].fillna("").astype(str),

        # Probentyp
        "Probentyp": df_src.iloc[:, col_index("O")].fillna("").astype(str),

        # Materialart
        "Materialart": df_src.iloc[:, col_index("P")].fillna("").astype(str),

        # Lage der Probenahmestelle
        "Gebaeude": df_src.iloc[:, col_index("Q")].fillna("").astype(str),
        "Raum_Herkunft": df_src.iloc[:, col_index("R")].fillna("").astype(str),
        "Raum_aktuell": df_src.iloc[:, col_index("S")].fillna("").astype(str),
        "x_Raum": pd.to_numeric(df_src.iloc[:, col_index("T")], errors="coerce").astype(float),
        "y_Raum": pd.to_numeric(df_src.iloc[:, col_index("U")], errors="coerce").astype(float),
        "Hoehe": pd.to_numeric(df_src.iloc[:, col_index("V")], errors="coerce").astype(float),
        "Geschoss": df_src.iloc[:, col_index("W")].fillna("").astype(str),
        "x": pd.to_numeric(df_src.iloc[:, col_index("X")], errors="coerce").astype(float),
        "y": pd.to_numeric(df_src.iloc[:, col_index("Y")], errors="coerce").astype(float),
        "Hoehe_absolut": pd.to_numeric(df_src.iloc[:, col_index("Z")], errors="coerce").astype(float),
        "Lagebeschreibung": df_src.iloc[:, col_index("AA")].fillna("").astype(str),

        # Bauteil
        "Beprobungsflaeche": df_src.iloc[:, col_index("AB")].fillna("").astype(str),
        "System": df_src.iloc[:, col_index("AC")].fillna("").astype(str),
        "Anlagenkennzeichnung": df_src.iloc[:, col_index("AD")].fillna("").astype(str),
        "Beschreibung": df_src.iloc[:, col_index("AE")].fillna("").astype(str),
        "Bauteil_laenge": pd.to_numeric(df_src.iloc[:, col_index("AF")], errors="coerce").astype(float),
        "Bauteil_breite": pd.to_numeric(df_src.iloc[:, col_index("AG")], errors="coerce").astype(float),
        "Bauteil_hoehe": pd.to_numeric(df_src.iloc[:, col_index("AH")], errors="coerce").astype(float),
        "Bauteil_dicke": pd.to_numeric(df_src.iloc[:, col_index("AI")], errors="coerce").astype(float),
        "Bauteil_nutzungsart": df_src.iloc[:, col_index("AJ")].fillna("").astype(str),
        "Oberflaechenbeschaffenheit": df_src.iloc[:, col_index("AK")].fillna("").astype(str),
        "Verdacht": df_src.iloc[:, col_index("AL")].fillna("").astype(str),
        "Besonderheiten_Freitext": df_src.iloc[:, col_index("AM")].fillna("").astype(str),

        # Foto-Nr.
        "Foto1": df_src.iloc[:, col_index("AN")].fillna("").astype(str),  # übersicht
        "Foto2": df_src.iloc[:, col_index("AO")].fillna("").astype(str),  # Detail
        "Foto3": df_src.iloc[:, col_index("AP")].fillna("").astype(str),  # Anlagen-kennzeichen
        "Foto4": df_src.iloc[:, col_index("AQ")].fillna("").astype(str),  # Detail
        "Foto5": df_src.iloc[:, col_index("AR")].fillna("").astype(str),  # Probenbehälter offen
        "Foto6": df_src.iloc[:, col_index("AS")].fillna("").astype(str),  # Probenbehälter hochkant

        # Ortsdosisleistung an der PN-Stelle
        "Mess_Typ_Stelle": df_src.iloc[:, col_index("AT")].fillna("").astype(str),
        "Messwert": pd.to_numeric(df_src.iloc[:, col_index("AU")], errors="coerce").astype(float),
        "Messabstand": df_src.iloc[:, col_index("AV")].fillna("").astype(str),

        # Oberflächenkontamination Direktmessung
        "Mess_Typ_DM": df_src.iloc[:, col_index("AW")].fillna("").astype(str),
        "Geraetnr_DM": df_src.iloc[:, col_index("AX")].fillna("").astype(str),
        "Ni_Alpha": pd.to_numeric(df_src.iloc[:, col_index("AY")], errors="coerce").astype(float),
        "Ni_Beta": pd.to_numeric(df_src.iloc[:, col_index("AZ")], errors="coerce").astype(float),
        "vPN_Alpha": pd.to_numeric(df_src.iloc[:, col_index("BA")], errors="coerce").astype(float),
        "vPN_Beta": pd.to_numeric(df_src.iloc[:, col_index("BB")], errors="coerce").astype(float),
        "nPN_Alpha": pd.to_numeric(df_src.iloc[:, col_index("BC")], errors="coerce").astype(float),
        "nPN_Beta": pd.to_numeric(df_src.iloc[:, col_index("BD")], errors="coerce").astype(float),

        # Probenahme allgemeine Angaben
        "Material_spez": df_src.iloc[:, col_index("BE")].fillna("").astype(str),
        "PN_Verfahren": df_src.iloc[:, col_index("BF")].fillna("").astype(str),
        "PN_Geraet": df_src.iloc[:, col_index("BG")].fillna("").astype(str),
        "PN_laenge": pd.to_numeric(df_src.iloc[:, col_index("BH")], errors="coerce").astype(float),
        "PN_breite": pd.to_numeric(df_src.iloc[:, col_index("BI")], errors="coerce").astype(float),
        "PN_flaeche": pd.to_numeric(df_src.iloc[:, col_index("BJ")], errors="coerce").astype(float),
        "Ma_Zustand": df_src.iloc[:, col_index("BK")].fillna("").astype(str),
        "Ma_Beschaffenheit": df_src.iloc[:, col_index("BL")].fillna("").astype(str),
        "PN_Verlust": pd.to_numeric(df_src.iloc[:, col_index("BM")], errors="coerce").astype(float),
        "Zi_Freitext": df_src.iloc[:, col_index("BN")].fillna("").astype(str),

        # Feststoffprobenahme
        "FSPN_dn": pd.to_numeric(df_src.iloc[:, col_index("BO")], errors="coerce").astype(float),
        "FSPN_tiefe_von": pd.to_numeric(df_src.iloc[:, col_index("BP")], errors="coerce").astype(float),
        "FSPN_tiefe_bis": pd.to_numeric(df_src.iloc[:, col_index("BR")], errors="coerce").astype(float),
        "Probe_EP_MP": df_src.iloc[:, col_index("BS")].fillna("").astype(str),
        "FSPN_Anzahl": pd.to_numeric(df_src.iloc[:, col_index("BT")], errors="coerce").astype(pd.Int64Dtype()),

        # Wischprobenahme
        "WP_Material": df_src.iloc[:, col_index("BU")].fillna("").astype(str),
        "WP_dn": pd.to_numeric(df_src.iloc[:, col_index("BV")], errors="coerce").astype(float),
        "WP_An_Fl": pd.to_numeric(df_src.iloc[:, col_index("BW")], errors="coerce").astype(pd.Int64Dtype()),
        "WP_Anzahl": pd.to_numeric(df_src.iloc[:, col_index("BX")], errors="coerce").astype(pd.Int64Dtype()),

        # Kernbohrung
        "KB_dn": pd.to_numeric(df_src.iloc[:, col_index("BY")], errors="coerce").astype(float),
        "KB_tiefe_von": pd.to_numeric(df_src.iloc[:, col_index("BZ")], errors="coerce").astype(float),
        "KB_tiefe_bis": pd.to_numeric(df_src.iloc[:, col_index("CA")], errors="coerce").astype(float),
        "KB_art": df_src.iloc[:, col_index("CB")].fillna("").astype(str),
        "KB_beschreibung": df_src.iloc[:, col_index("CC")].fillna("").astype(str),

        # Angaben zur Probe
        "AP_behaelter": df_src.iloc[:, col_index("CD")].fillna("").astype(str),
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
        "Bm_Freitxt": df_src.iloc[:, col_index("CN")].fillna("").astype(str),

        # Probenehmer
        "PN_team": df_src.iloc[:, col_index("CO")].fillna("").astype(str),
        "pn_nehmer1": df_src.iloc[:, col_index("CP")].fillna("").astype(str),
        "pn_nehmer2": df_src.iloc[:, col_index("CQ")].fillna("").astype(str),

        # Probenübergabe
        "pue_empfaenger": df_src.iloc[:, col_index("CR")].fillna("").astype(str),
        "pue_date": pd.to_datetime(df_src.iloc[:, col_index("CS")], errors="coerce"),

    })
    return df


def bool_str(value) -> str:
    if pd.isna(value) or value == "":
        return "false"
    return "true"


def bool_to_float(value) -> float:
    """Konvertiert Boolean/String zu float (1.0 = true, 0.0 = false) für GPKG-Kompatibilität"""
    if pd.isna(value) or value == "" or value == False or value == "false" or value == "False" or value == 0:
        return 0.0
    return 1.0

