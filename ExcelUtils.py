# Imports
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame


class Global:
    def __init__(self):
        self.probes_path = str()
        self.db_path = str()


config = Global()


def col_index(col_name: str):
    """
    Wandelt einen Excel-Spaltenbuchstaben (z.B. 'A', 'Z', 'AA', 'BZ') in einen 0-basierten Index für pandas um.
    
    Parameter:
        col_name (str): Excel-Spaltenbuchstabe
    
    Rückgabe:
        int: 0-basierter Index
    """
    col_name = col_name.upper()
    index = 0
    for i, char in enumerate(reversed(col_name)):
        index += (ord(char) - ord('A') + 1) * (26 ** i)
    return index - 1


def return_max(dataframe: DataFrame, colName: str):
    max_val = float(0.0)
    if colName in dataframe.columns:
        max_val = dataframe[colName].max()
    return max_val


# Werte Formatieren für Excel Tabelle
def format_hyphen(value: float):
    if value == 0.0 or pd.isna(value):
        return "-"
    else:
        return value
