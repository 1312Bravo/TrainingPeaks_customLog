import pandas as pd
import numpy as np
from typing import Tuple
import gspread

# Helps ~ Import Google sheet to DataFrame (return dataframe and sheet object for further use)
def import_google_sheet(googleDrive_client, filename, sheet_index=0) -> Tuple[pd.DataFrame, gspread.models.Worksheet]:
    
    file = googleDrive_client.open(filename)
    sheet = file.get_worksheet(sheet_index)
    data = sheet.get_all_values()
    dataframe = pd.DataFrame(data[1:], columns=data[0]) 

    return dataframe, sheet
    

# Helps ~ Write to Google Sheets
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

# Helps ~ Write to Google Sheets
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
    


