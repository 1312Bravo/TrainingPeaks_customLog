import numpy as np

def replace_nan_with_empty_string(obj):
    if isinstance(obj, dict):  # If it's a dictionary, apply the function to each value
        return {k: replace_nan_with_empty_string(v) for k, v in obj.items()}
    elif isinstance(obj, tuple):  # If it's a tuple, apply the function to each element
        return tuple(replace_nan_with_empty_string(v) for v in obj)
    elif isinstance(obj, list):  # If it's a list, apply the function to each element
        return [replace_nan_with_empty_string(v) for v in obj]
    elif isinstance(obj, float) and np.isnan(obj):  # If it's np.nan, replace it with ""
        return ""
    else:
        return obj

def clean_data(obj):
    if isinstance(obj, dict):  # Process dictionaries recursively
        return {k: clean_data(v) for k, v in obj.items()}
    elif isinstance(obj, tuple):  # Convert tuples to a single string or empty string
        return " | ".join(map(str, obj)) if any(obj) else ""  # Join values or replace empty tuples with ""
    elif isinstance(obj, list):  # Process lists recursively
        return [clean_data(v) for v in obj]
    elif isinstance(obj, float) and np.isnan(obj):  # Replace NaN with ""
        return ""
    else:
        return obj

