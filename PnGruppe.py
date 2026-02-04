import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from typing import Optional, Any, Dict, Li
from dataclasses import dataclass


def col_index(col_name: str) -> int:
    """
    Wandelt Excel-Spaltenbuchstaben ('A', 'Z', 'AA', 'BZ') in 0-basierten Index um.
    """
    col_name = col_name.upper()
    index = 0
    for i, char in enumerate(reversed(col_name)):
        index += (ord(char) - ord('A') + 1) * (26 ** i)
    return index - 1


@dataclass
class PnGruppe:
    fid: int = read_value(df, i, col_index("A"))
    Ref_Geschoss: str = None
    Geschoss: str = None
    Gebaeude: str = None
    Raum_Herkunft: str = None
    Raum_aktuell: str = None
    UUID: str = None
    x: float = None
    y: float = None
    Hoehe: float = None
    Hoehe_absolut: float = None
    Gruppe_Nummer: int = None
    Auftrags_Nummer: int = None
    Serien_Nummer: int = None
    pn_zweck: str = None
    Auftrag: str = None
    Auftraggeber: str = None
    Projektleiter: str = None
    Lagebeschreibung: str = None
    x_Raum: float = None
    y_Raum: float = None
    Beprobungsflaeche: str = None
    System: str = None
    Anlagenkennzeichnung: str = None
    Beschreibung: str = None
    Bauteil_laenge: float = None
    Bauteil_breite: float = None
    Bauteil_hoehe: float = None
    Bauteil_dicke: float = None
    Foto3: str = None
    Foto1: str = None
    Mess_Typ_Stelle: str = None
    Messwert: str = None
    Messabstand: str = None
    Mess_Typ_DM: str = None
    Geraetnr_DM: str = None
    Ni_Alpha: float = None
    Ni_Beta: float = None
    PN_team: str = None
    pn_nehmer1: str = None
    pn_nehmer2: str = None

    def read_value (self, df: pd.DataFrame, row_idx: int, col: str):
        """
        Liest den Wert aus df (über Excel-Spaltenbuchstabe) und weist ihn dem Objekt-Attribut zu.
        """
        j = col_index(col)  # Spaltenindex berechnen
        value = df.iat[row_idx, j]  # Rohwert aus DataFrame holen
        setattr(self, attribute, value)  # Attribut setzen

