from datetime import datetime
import pandas as pd

def remove_comma_to_float(value: str) -> float:
    """
    Remove commas from the value and convert it to float.
    
    Args:
        value (str): The value to be converted.
    
    Returns:
        float: The converted value.
    """
    if isinstance(value, str):
        return float(value.replace('.', '').replace(',', '.'))
    return value


def remove_thousands_separator(value: str) -> int:
    """
    Remove the thousands separator from the value.
    
    Args:
        value (str): The value to be converted.
    
    Returns:
        str: The converted value.
    """
    if isinstance(value, str):
        return int(value.replace('.', ''))
    return value


def convert_format_date(date_str: str) -> str:
    """
    Convert the date string to the desired format.
    
    Args:
        date_str (str): The date string to be converted.
    
    Returns:
        str: The converted date string.
    """
    if not isinstance(date_str, str):
        return date_str
    try:
        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
        return date_obj.strftime('%Y-%m-%d')

    except ValueError:
        print(f"Invalid date format: {date_str}. Expected format is 'dd/mm/yyyy'.")

    return date_str


def normalize_json (line, element):
    df_normalized = pd.json_normalize(line, sep='_')
    df_normalized[element] = df_normalized[element].str.split(';')
    df_normalized = df_normalized.explode(element)
    return df_normalized


