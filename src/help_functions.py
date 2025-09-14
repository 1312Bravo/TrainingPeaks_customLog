import pandas as pd
import numpy as np
from typing import Tuple
from gspread.worksheet import Worksheet 

# Set up repository root path
def set_up_repo_root_path():
    import os
    import sys
    repo_root = os.path.abspath(os.getcwd())  
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

# Helps ~ Import Google sheet to DataFrame (return dataframe and sheet object for further use)
def import_google_sheet(googleDrive_client, filename, sheet_name) -> Tuple[pd.DataFrame, Worksheet]:
    
    file = googleDrive_client.open(filename)
    sheet = file.worksheet(sheet_name)
    data = sheet.get_all_values()
    dataframe = pd.DataFrame(data[1:], columns=data[0]) 

    return dataframe, sheet

# Convert columns to numeric if possible (all are object ...)
def safe_convert_to_numeric(x):
    if x == "":
        return np.nan  
    try:
        return pd.to_numeric(x)
    except (ValueError, TypeError):
        return str(x)
    
def data_safe_convert_to_numeric(df):
    for col in df.columns:
        try:
            df[col] = df[col].apply(safe_convert_to_numeric)
        except ValueError:
            pass 

    return df

# Write to Google Sheets
def replace_nan_with_empty_string(obj):
    if isinstance(obj, dict):  
        return {k: replace_nan_with_empty_string(v) for k, v in obj.items()}
    elif isinstance(obj, tuple):  
        return tuple(replace_nan_with_empty_string(v) for v in obj)
    elif isinstance(obj, list):  
        return [replace_nan_with_empty_string(v) for v in obj]
    elif isinstance(obj, float) and np.isnan(obj):
        return ""
    else:
        return obj

# Write to Google Sheets
def clean_data(obj):
    if isinstance(obj, dict): 
        return {k: clean_data(v) for k, v in obj.items()}
    elif isinstance(obj, tuple):
        return " | ".join(map(str, obj)) if any(obj) else ""  
    elif isinstance(obj, list):  
        return [clean_data(v) for v in obj]
    elif isinstance(obj, float) and np.isnan(obj): 
        return ""
    else:
        return obj
    


