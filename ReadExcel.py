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




    })
    return df


# Nuklid-Messwerte in Dataframe übernehmen
def read_nuclides(df_src: DataFrame, df_dest: DataFrame, nuclides: np.ndarray):
    for nuc in nuclides:
        idx_value = col_index(nuc.col_value)  # Spalte Messwerte
        idx_mu = col_index(nuc.col_mu)  # Spalte Messunsicherheiten
        idx_unit = col_index("CT")  # Spalte Masseinheit

        # Spalten in numerische Werte umwandeln
        val_series = pd.to_numeric(df_src.iloc[:, idx_value], errors="coerce")
        mu_series = pd.to_numeric(df_src.iloc[:, idx_mu], errors="coerce")

        # Bedingungen: Einheit und MU nicht leer
        cond_cs = (mu_series.notna()) & (mu_series < 0.3) & (df_src.iloc[:, idx_unit] == "[Bq/cm²]")
        cond_ll = (mu_series.notna()) & (mu_series < 0.3) & (df_src.iloc[:, idx_unit] == "[Bq/g]")

        # Faktoren in den Dataframe übernehmen
        df_dest[f"{nuc.name}-Faktor"] = np.select(
            condlist=[cond_cs, cond_ll],  # Auswahl: je nach Bedingung mit CS oder LL normieren
            choicelist=[
                val_series / nuc.CS,
                val_series / nuc.LL
            ],
            default=float(0)
        )
        # Absolute Messwerte in den Dataframe übernehmen
        df_dest[f"{nuc.name}-Wert"] = np.where(
            ((mu_series.notna() & mu_series > 0) & (mu_series < 0.3)),
            val_series,
            None
        )


# Filtern nach aktuellem System
def filter_df(df: DataFrame, filter: str, exact: bool):
    if exact:
        # Oder exakt so heissen
        return df[df["System"] == filter]

    else:
        # Erhält Systemname
        return df[df["System"].astype(str).str.contains(filter, na=False, case=False)].copy()


# Maximale Messwerte ausgeben (Flächenspezifisch / Massenspezifisch)
def max_measured_value(df: DataFrame, unit: str, nuclides: np.ndarray):
    df_unit = df[df["Masseinheit"] == unit]
    global_max = 0.0

    for nuc in nuclides:
        col = f"{nuc.name}-Wert"
        if col in df_unit.columns:
            col_values = pd.to_numeric(df_unit[col], errors="coerce")
            max_val = col_values.max(skipna=True)

            if pd.notna(max_val) and max_val > global_max:
                global_max = max_val

    return float(global_max)


# Beta-Gamma / Alpha – Verhältnis
def bga_ratio(df: DataFrame, a_nucs: np.ndarray, b_g_nucs: np.ndarray):
    # Spalten dynamisch erzeugen
    bet_gam_cols = [f"{n}-Wert" for n in b_g_nucs if f"{n}-Wert" in df.columns]
    alpha_cols = [f"{n}-Wert" for n in a_nucs if f"{n}-Wert" in df.columns]

    # Sicherstellen, dass alle Werte numerisch sind
    df[bet_gam_cols] = df[bet_gam_cols].apply(pd.to_numeric, errors="coerce")
    df[alpha_cols] = df[alpha_cols].apply(pd.to_numeric, errors="coerce")

    # Überprüfen ob Radiochemische Werte vorhanden sind mit Abweichung <30%
    for n in a_nucs:
        g_spec = f"{n}-Wert"
        r_chem = f"{n}(RC)-Wert"
        if g_spec in df.columns and r_chem in df.columns:
            # Prüfen, ob keiner der Werte NaN oder 0 ist
            valid_values = (df[g_spec].notna()) & (df[r_chem].notna()) & (df[g_spec] != 0) & (df[r_chem] != 0)

            # Berechnung des Verhältnisses nur für gültige Werte
            ratio = (df[g_spec] - df[r_chem]) / df[g_spec]

            # Bedingung: Werte innerhalb des Bereichs [-0.3, 0.3]
            within_range: Series[bool] = (ratio >= -0.3) & (ratio <= 0.3) & valid_values

            # Werte in den betroffenen Zeilen anpassen
            df.loc[within_range & valid_values, g_spec] = 0
            df.loc[~within_range & valid_values, r_chem] = 0

    # Summieren
    df.loc[:, "Summe Beta-Gamma"] = df[bet_gam_cols].fillna(0).sum(axis=1)
    df.loc[:, "Summe Alpha"] = df[alpha_cols].fillna(0).sum(axis=1)

    # Verhältnis berechnen
    ratio_columns(df, "Verhältnis Beta-Gamma/Alpha", "Summe Beta-Gamma", "Summe Alpha")

    # Gesamt Beta / Gesamt Alpha
    ratio_columns(df, "∑ges-B / ∑ges-a", "∑ges-B [Bq/cm²]", "∑ges-α [Bq/cm²]")


# Alpha vorhanden Flag setzen
def check_alpha(df: DataFrame, ratio_sum_ba: float):
    alpha_state = "nein"

    if "Am241-Wert" in df.columns:
        am241_values = pd.to_numeric(df["Am241-Wert"], errors="coerce")
        if (am241_values > 0).any():
            alpha_state = "ja"

    if "Am241(RC)-Wert" in df.columns:
        am241_rc_values = pd.to_numeric(df["Am241(RC)-Wert"], errors="coerce")
        if (am241_rc_values > 0).any():
            alpha_state = "ja"

    if ratio_sum_ba != 0 and ratio_sum_ba < 100:
        alpha_state = "ja"

    return alpha_state


# Maximalen Faktor für ein Nuklid mit bestimmter Masseinheit zurückgeben
def get_max_factor(df: DataFrame, nuclide_col: str, unit: str):
    filtered = df[(df["Masseinheit"] == unit) & (df[nuclide_col] > 0)]
    return filtered[nuclide_col].max(skipna=True) or 0  # NaN → 0


# Dictionary für {Nuklidname: Maximaler Faktor über alle Proben}
def max_factor(df: DataFrame, nuclides: np.ndarray):
    nuclide_max = {}
    for nuc in nuclides:
        col = f"{nuc.name}-Faktor"
        if col in df.columns:
            max_val = df[col].max()
            if max_val > 0:  # nur wenn Faktor > 0 vorkommt
                nuclide_max[nuc.name] = max_val
    return nuclide_max


# Daten in Objektparameter Speichern
def save_values(sys: System, df_system: DataFrame, lead_nuclides: list, more_nuclides: list, maxXFactor: str,
                max_area_val: float, max_mass_val: float, rad_zone: str, alpha_state: str, ratio_bga: float,
                ratio_sum_ba: float):
    sys.sampleNumbers = "; ".join(df_system["Proben-Nr."].astype(str).tolist())
    sys.sampleGroups = "; ".join(df_system["Proben-Gruppe"].astype(str).drop_duplicates())
    sys.Co60Fact = df_system["Co60-Faktor"].max()
    sys.Cs137Fact = df_system["Cs137+-Faktor"].max()
    sys.leadNuc = "; ".join(lead_nuclides)
    sys.moreNuc = "; ".join(more_nuclides)
    sys.maxFact = maxXFactor
    sys.maxValCS = round(max_area_val, 3)
    sys.maxValLL = round(max_mass_val, 3)
    sys.zone = rad_zone
    sys.alpha = alpha_state
    sys.ratio_bga = format_hyphen(ratio_bga)
    sys.ratio_sum_ba = format_hyphen(ratio_sum_ba)


# Start Programmablauf
def prepare_dataframe(rad_probes: str, nuclides: np.ndarray):
    """Lädt Rohdaten und bereitet den DataFrame vor."""
    df_raw = load_input_data(rad_probes)
    df = append_columns(df_raw)
    read_nuclides(df_raw, df, nuclides)
    return df


# Auf gängige Leitnuklide prüfen und falls vorhanden übernehmen
def check_lead_nuc(com_nucs: list, max_nucs: list, lead_nucs: list):
    remaining_nucs = []
    for nuc in max_nucs:
        if nuc in com_nucs:
            lead_nucs.append(nuc)
        else:
            remaining_nucs.append(nuc)
    max_nucs.clear()
    max_nucs.extend(remaining_nucs)


# Berechnet die Werte für das angegebene System
def process_system(sys: System, df: DataFrame, nuclides: np.ndarray, constants: dict):
    df_system = filter_df(df, sys.name, config.precision_filter)

    print(f"\n{sys.name}")

    cs_sum_alpha = constants["cs_sum_alpha"]
    cs_sum_beta = constants["cs_sum_beta"]
    cs_unit = constants["cs_unit"]
    ll_unit = constants["ll_unit"]

    # --- Maximale Messwerte ---
    max_area_val = max_measured_value(df_system, cs_unit, nuclides)
    max_mass_val = max_measured_value(df_system, ll_unit, nuclides)

    max_alpha_val = returnMax(df_system, "∑ges-α [Bq/cm²]")
    max_alpha_fac = max_alpha_val / cs_sum_alpha

    max_beta_val = returnMax(df_system, "∑ges-B [Bq/cm²]")
    max_beta_fac = max_beta_val / cs_sum_beta

    # --- Leitnuklide ---
    lead_nuclides = []
    nuclide_max = max_factor(df_system, nuclides)
    max_nuclides = sorted(nuclide_max, key=nuclide_max.get, reverse=True)

    # Gängige Leitnuklide Prüfen
    common_nucs = ["Co60", "Cs137+", "Am241", "Am241(RC)"]
    check_lead_nuc(common_nucs, max_nuclides, lead_nuclides)

    # Wenn weder Co60, Cs137+ oder Am241 vorhanden ist, Eintrag der drei höchsten übrigen Nuklide
    if not lead_nuclides:
        lead_nuclides = max_nuclides[:3]
    more_nuclides = [nuc for nuc in max_nuclides if nuc not in lead_nuclides]

    # --- x-fache Freigabe ---
    all_factors = dict(nuclide_max)
    all_factors["Summe Alpha"] = max_alpha_fac
    all_factors["Summe Beta"] = max_beta_fac
    values = [v for v in all_factors.values() if not pd.isna(v)]
    if values:
        maxXFactor = max(values)
    else:
        maxXFactor = 0

    # --- Beta-Gamma / Alpha verhältnisse ---
    beta_gamma_nucs, alpha_nucs = get_nuclide_groups()  # Liste der Nuklidnamen holen
    bga_ratio(df_system, alpha_nucs, beta_gamma_nucs)
    ratio_bga = returnMax(df_system, "Verhältnis Beta-Gamma/Alpha")
    ratio_sum_ba = returnMax(df_system, "∑ges-B / ∑ges-a")

    # --- Zonierung ---
    co60_cs = get_max_factor(df_system, "Co60-Faktor", cs_unit)
    co60_ll = get_max_factor(df_system, "Co60-Faktor", ll_unit)
    cs137_cs = get_max_factor(df_system, "Cs137+-Faktor", cs_unit)
    cs137_ll = get_max_factor(df_system, "Cs137+-Faktor", ll_unit)
    am241_cs = get_max_factor(df_system, "Am241-Faktor", cs_unit)
    am241_ll = get_max_factor(df_system, "Am241-Faktor", ll_unit)
    am241_rc_cs = get_max_factor(df_system, "Am241(RC)-Faktor", cs_unit)
    am241_rc_ll = get_max_factor(df_system, "Am241(RC)-Faktor", ll_unit)

    rad_zone = rad_zoning(co60_cs, co60_ll, cs137_cs, cs137_ll, am241_cs, am241_ll, am241_rc_cs, am241_rc_ll, ratio_bga,
                          ratio_sum_ba)

    # --- Alpha vorhanden? ---
    alpha_state = check_alpha(df_system, ratio_sum_ba)

    # --- Speichern in Systemobjekt ---
    save_values(
        sys, df_system, lead_nuclides, more_nuclides, maxXFactor,
        max_area_val, max_mass_val, rad_zone, alpha_state, ratio_bga, ratio_sum_ba
    )
    return sys


# Startet den Gesamtprozess
def process_all_systems(df: DataFrame, systems: np.ndarray, nuclides: np.ndarray):
    constants = get_constants()
    for sys in systems:
        process_system(sys, df, nuclides, constants)


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
