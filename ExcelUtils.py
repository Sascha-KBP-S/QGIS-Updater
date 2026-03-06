# Imports
import pandas as pd
import json
from pandas import DataFrame
import numpy as np



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


def load_value_maps(json_path="value_maps.json"):
    """Lädt Value Maps aus JSON."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def decode_value(value, column_name, layer_name, value_maps):
    """
    Konvertiert einen Index zurück zu seinem String-Wert basierend auf value_maps.

    Returns: String-Wert wenn Index gefunden, sonst Original-Wert
    """
    if pd.isna(value) or value is None:
        return value

    try:
        # Konvertiere zu int ob es eine Zahl (int, float, numpy) oder String ist
        if isinstance(value, str):
            # Versuche String als int zu parsen
            try:
                idx = int(value)
            except ValueError:
                return value  # Nicht konvertierbar, Original zurückgeben
        elif isinstance(value, (int, float, np.integer, np.floating)):
            idx = int(value)
        else:
            return value

        col_map = value_maps.get(layer_name, {}).get(column_name, {})

        if col_map:
            for string_val, idx_val in col_map.items():
                if int(idx_val) == idx:
                    return string_val
    except (ValueError, TypeError, KeyError):
        pass

    return value
