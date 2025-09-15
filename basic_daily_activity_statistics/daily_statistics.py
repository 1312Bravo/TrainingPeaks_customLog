import pandas as pd
import numpy as np

# Set up repo root path
import os
import sys
repo_root = os.path.abspath(os.getcwd())  
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Import help functions
from src import help_functions as hf

# -----------------------------------------------------
# GO: Get & Prepare single day daily statistics
# -----------------------------------------------------

def get_prepare_single_day_daily_statistics(garminClient, selectedDate):

    # Download data
    overall_stats = garminClient.get_stats(selectedDate.isoformat()) 
    trainingReadiness_stats = garminClient.get_training_readiness(selectedDate.isoformat()) 
    trainingStatus_stats = garminClient.get_training_status(selectedDate.isoformat()) 

    # Sleep scores
    restingHeartRate = overall_stats.get("restingHeartRate", np.nan) if overall_stats else np.nan
    sleepScore = trainingReadiness_stats[0].get("sleepScore", np.nan) if trainingReadiness_stats else np.nan
    sleepingSeconds = overall_stats.get("sleepingSeconds", np.nan) if overall_stats else np.nan

    # HRV data
    hrv_stats = garminClient.get_hrv_data(selectedDate.isoformat())
    hrv_lastNightAvg = hrv_stats["hrvSummary"].get("lastNightAvg", np.nan) if hrv_stats else np.nan
    hrv_baselineInterval = [
        hrv_stats["hrvSummary"]["baseline"].get("balancedLow", np.nan),
        hrv_stats["hrvSummary"]["baseline"].get("balancedUpper", np.nan)
    ] if hrv_stats else [np.nan, np.nan]

    # Active times
    highlyActiveSeconds = overall_stats.get("highlyActiveSeconds", np.nan) if overall_stats else np.nan
    activeSeconds = overall_stats.get("activeSeconds", np.nan) if overall_stats else np.nan
    sedentarySeconds = overall_stats.get("sedentarySeconds", np.nan) if overall_stats else np.nan
    floorsAscendedInMeters = overall_stats.get("floorsAscendedInMeters", np.nan) if overall_stats else np.nan

    # VO2Max and similar
    latest_vo2Max = (((trainingStatus_stats or {}).get("mostRecentVO2Max") or {}).get("generic") or {}  ).get("vo2MaxPreciseValue", np.nan)
    latest_hillScore = garminClient.get_hill_score(selectedDate.isoformat()) and garminClient.get_hill_score(selectedDate.isoformat()).get("overallScore", np.nan) or np.nan
    lastest_enduranceScore = garminClient.get_endurance_score(selectedDate.isoformat()) and garminClient.get_endurance_score(selectedDate.isoformat()).get("overallScore", np.nan) or np.nan

    # Monthly Training Load
    device_data = next(iter((trainingStatus_stats.get("mostRecentTrainingLoadBalance", {}) or {}).get("metricsTrainingLoadBalanceDTOMap", {}).values()), {}) if trainingStatus_stats else np.nan
    monthlyLoadAerobicLow = device_data.get("monthlyLoadAerobicLow", np.nan) if trainingStatus_stats else np.nan
    monthlyLoadAerobicHigh = device_data.get("monthlyLoadAerobicHigh", np.nan) if trainingStatus_stats else np.nan
    monthlyLoadAnaerobic = device_data.get("monthlyLoadAnaerobic", np.nan) if trainingStatus_stats else np.nan

    # All in one
    dailyScores = {

        "Year": selectedDate.year,
        "Month": selectedDate.month,
        "Day": selectedDate.day,
        "Weekday": selectedDate.strftime("%A"),

        "Resting HR": restingHeartRate,
        "Sleep score": sleepScore,
        "Sleep time [h]": round(sleepingSeconds / 3600, 2),
        "HRV": hrv_lastNightAvg,
        "HRV baseline lower": hrv_baselineInterval[0],
        "HRV baseline upper": hrv_baselineInterval[1],

        "Meters ascended [m]": round(floorsAscendedInMeters),
        "Highly active time [h]": round(highlyActiveSeconds / 3600, 2), 
        "Active time [h]": round(activeSeconds / 3600, 2),
        "Sedentary time [h]": round(sedentarySeconds / 3600, 2),

        "vo2Max": latest_vo2Max,
        "Hill score": latest_hillScore,
        "Endurance score": lastest_enduranceScore,
        "Low aerobic load": round(monthlyLoadAerobicLow,0),
        "High aerobic load": round(monthlyLoadAerobicHigh,0),
        "Anaerobic load": round(monthlyLoadAnaerobic,0),

        }
    
    # Return
    dailyScores = hf.replace_nan_with_empty_string(dailyScores)
    return dailyScores
