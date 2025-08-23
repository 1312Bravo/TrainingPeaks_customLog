import pandas as pd
import numpy as np

from src.help_functions import replace_nan_with_empty_string

def singleDay_activityStats(garminClient, selectedDate):

    # Download data
    activity_stats = garminClient.get_activities_by_date(selectedDate.isoformat(), selectedDate.isoformat())
    nr_activities = len(activity_stats)
    activityScores = {}

    # No activity ~> Rest day
    if not activity_stats:
        singleActivity_activityScores = {

            "Year": selectedDate.year,
            "Month": selectedDate.month,
            "Day": selectedDate.day,
            "Weekday": selectedDate.strftime("%A"),

            "Description": "Rest",
            "Activity type": np.nan,
            "Start time": np.nan,
            "Location": np.nan,

            "Distance [km]": np.nan,
            "Duration [h]": np.nan,
            "Elevation gain [m]":np.nan,
            "Average pace [min/km] or speed [km/h]": np.nan,
            "Gradient adjusted pace [min/km]": np.nan,
            "Average heart rate": np.nan, 
            "Maximum heart rate": np.nan, 
            "Normalized power [w]":  np.nan,
            "Calories [kcal]": np.nan,

            "Aerobic training effect":np.nan,
            "Aerobic training effect message": np.nan,
            "Anaerobic training effect": np.nan,
            "Anaerobic training effect message": np.nan,
            "Training effect label": np.nan,
            "Training load": np.nan,
            "Vo2Max value": np.nan,

            "Time in Z1 [h]": np.nan,
            "Time in Z2 [h]": np.nan,
            "Time in Z3 [h]": np.nan,
            "Time in Z4 [h]": np.nan,
            "Time in Z5 [h]": np.nan,

            "10% heart rate [1]": np.nan,
            "10% heart rate [2]": np.nan,
            "10% heart rate [3]": np.nan,
            "10% heart rate [4]": np.nan,
            "10% heart rate [5]": np.nan,
            "10% heart rate [6]": np.nan,
            "10% heart rate [7]": np.nan,
            "10% heart rate [8]": np.nan,
            "10% heart rate [9]": np.nan,
            "10% heart rate [10]": np.nan,

            }
        
        singleActivity_activityScores = replace_nan_with_empty_string(singleActivity_activityScores)
        activityScores["activity_{}".format(0)] = singleActivity_activityScores

    else:

        for i in reversed(range(nr_activities)):

            # About
            activity_name = activity_stats[i].get("activityName", "Unknown") 
            activity_typeName = activity_stats[i]["activityType"]["typeKey"].replace("_", " ").title() 
            activity_parenttypeId = 1 if "running" in activity_typeName.lower() else 2 if "cycling" in activity_typeName.lower() else 3
            activity_startTime = activity_stats[i]["startTimeLocal"][11:16] if activity_stats else "00:00"
            activity_locationName = activity_stats[i].get("locationName", "Unknown") 

            # Overall metrics
            activity_distance = activity_stats[i].get("distance", np.nan) 
            activity_duration = activity_stats[i].get("movingDuration", np.nan) 
            activity_elevationGain = activity_stats[i].get("elevationGain", np.nan) 
            activity_averageSpeed = activity_stats[i].get("averageSpeed", np.nan) 
            activity_avgGradeAdjustedSpeed = activity_stats[i].get("avgGradeAdjustedSpeed", np.nan) 
            activity_calories = activity_stats[i].get("calories", np.nan) 
            activity_normalizedPower = activity_stats[i].get("normPower", np.nan) 

            # Heart rate
            activity_averageHR = activity_stats[i].get("averageHR", np.nan) 
            activity_maxHR = activity_stats[i].get("maxHR", np.nan) 

            # Training effect
            activity_aerobicTrainingEffect = activity_stats[i].get("aerobicTrainingEffect", np.nan) 
            activity_aerobicTrainingEffectMessage = activity_stats[i].get("aerobicTrainingEffectMessage", "").replace("_", " ").capitalize() 
            activity_anaerobicTrainingEffect = activity_stats[i].get("anaerobicTrainingEffect", np.nan) 
            activity_anaerobicTrainingEffectMessage = activity_stats[i].get("anaerobicTrainingEffectMessage", "").replace("_", " ").capitalize()
            activity_trainingEffectLabel = activity_stats[i].get("trainingEffectLabel", "").replace("_", " ").capitalize() 
            activity_trainingLoad = activity_stats[i].get("activityTrainingLoad", np.nan) 
            activity_vO2MaxValue = activity_stats[i].get("vO2MaxValue", np.nan) 

            # HR Zones
            activity_stats_hrZones = garminClient.get_activity_hr_in_timezones(activity_stats[i]["activityId"]) 
            if activity_stats_hrZones != []:
                activity_zones_df = pd.DataFrame()
                for singleZone in activity_stats_hrZones:
                    singleZone_df = pd.DataFrame({
                        "zone": [singleZone.get("zoneNumber", np.nan)],
                        "duration": [singleZone.get("secsInZone", np.nan)], 
                        "lowBoundary": [singleZone.get("zoneLowBoundary", np.nan)]
                        })
                    activity_zones_df = pd.concat([activity_zones_df, singleZone_df], ignore_index=True)
                activity_zones_df["highBoundary"] = ((activity_zones_df["lowBoundary"].shift(-1))-1).fillna(220).astype(int)
            else:
                activity_zones_df = pd.DataFrame({
                    "zone": [1,2,3,4,5],
                    "duration": [np.nan,np.nan,np.nan,np.nan,np.nan],
                    "lowBoundary:": [np.nan,np.nan,np.nan,np.nan,np.nan],
                    "highBoundary": [np.nan,np.nan,np.nan,np.nan,np.nan],
                })

            # Splits
            activity_stats_splits = garminClient.get_activity_splits(activity_stats[i]["activityId"])
            if activity_stats_splits != []:
                activity_stats_splits_df = pd.DataFrame() 
                for singleSplit in activity_stats_splits["lapDTOs"]:
                    singleSplit_df = pd.DataFrame({
                        "type": [singleSplit.get("intensityType", np.nan)],
                        "distance": [singleSplit.get("distance", np.nan)], 
                        "duration": [singleSplit.get("movingDuration", np.nan)], 
                        "elevationGain": [singleSplit.get("elevationGain", np.nan)], 
                        "elevationLoss": [singleSplit.get("elevationLoss", np.nan)], 
                        "averageHR": [singleSplit.get("averageHR", np.nan)], 
                        "maxHR": [singleSplit.get("maxHR", np.nan)], 
                        "avgSpeed": [singleSplit.get("averageMovingSpeed", np.nan)], 
                        "avgGradeAdjustedSpeed": [singleSplit.get("avgGradeAdjustedSpeed", np.nan)], 
                        "averageRunCadence": [singleSplit.get("averageRunCadence", np.nan)], 
                        "normalizedPower": [singleSplit.get("normalizedPower", np.nan)], 
                        "totalWork": [singleSplit.get("totalWork", np.nan)]
                        })
                    activity_stats_splits_df = pd.concat([activity_stats_splits_df, singleSplit_df], ignore_index=True)
            else:
                activity_stats_splits_df = pd.DataFrame()

            # Second by second ~ Aggregation
            activity_stats_activityMetrics = garminClient.get_activity_details(activity_stats[i]["activityId"])
            metricValues = activity_stats_activityMetrics["activityDetailMetrics"]
            metricDescriptors = activity_stats_activityMetrics["metricDescriptors"]

            if activity_parenttypeId == 1: # Running

                sumMovingDuration_index = next((d["metricsIndex"] for d in metricDescriptors if d["key"] == "sumMovingDuration"), np.nan) # factor = 1000.0 [ms] ~> x / 1000 -> s
                directSpeed_index = next((d["metricsIndex"] for d in metricDescriptors if d["key"] == "directSpeed"), np.nan) # factor = 0.1 [dm/s] ~> x / 0.1 -> m/s
                directVerticalSpeed_index = next((d["metricsIndex"] for d in metricDescriptors if d["key"] == "directVerticalSpeed"), np.nan)  # factor = 0.1 [dm/s] ~> x / 0.1 -> m/s
                directGradeAdjustedSpeed_index = next((d["metricsIndex"] for d in metricDescriptors if d["key"] == "directGradeAdjustedSpeed"), np.nan) # factor = 0.1 [dm/s] ~> x / 0.1 -> m/s
                directHeartRate_index = next((d["metricsIndex"] for d in metricDescriptors if d["key"] == "directHeartRate"), np.nan) # factor = 1.0 [bpm]  ~> x / 1 -> bpm

                elapsed_times = [] # seconds
                speed_values = [] # min/km
                verticalSpeed_values = [] # m/h
                gradeAdjustedSpeed_values = [] # min/km
                heartrate_values = [] # bpm

                previous_time_s = 0
                for entry in metricValues:

                    try:
                        current_time_s = entry["metrics"][sumMovingDuration_index]
                        elapsed_time_s = (current_time_s - previous_time_s) 
                        elapsed_times += [elapsed_time_s]
                        previous_time_s = current_time_s
                    except:
                        elapsed_times += [np.nan]

                    try:
                        speed_ms = entry["metrics"][directSpeed_index]
                        speed_minkm = (1000 / speed_ms) / 60 if speed_ms > 0 else 0
                        speed_values += [speed_minkm] 
                    except:
                        speed_values += [np.nan]

                    try:
                        verticalSpeed_ms = entry["metrics"][directVerticalSpeed_index]
                        speed_mh = verticalSpeed_ms * 3600
                        verticalSpeed_values += [speed_mh] 
                    except:
                        verticalSpeed_values += [np.nan]

                    try:
                        gradespeed_ms = entry["metrics"][directGradeAdjustedSpeed_index]
                        gradespeed_minkm = (1000 / gradespeed_ms) / 60 if gradespeed_ms > 0 else 0
                        gradeAdjustedSpeed_values += [gradespeed_minkm] 
                    except:
                        gradeAdjustedSpeed_values += [np.nan]

                    try:
                        heartrate_values += [entry["metrics"][directHeartRate_index]]
                    except:
                        heartrate_values += [np.nan]

                activity_metrics = pd.DataFrame({
                    "elapsed_time": elapsed_times,
                    "speed_value": speed_values,
                    "verticalSpeed_value": verticalSpeed_values,
                    "gradeAdjustedSpeed_value": gradeAdjustedSpeed_values,
                    "heartrate_value": heartrate_values,
                    })
                
                activity_metrics_secondGap = activity_metrics.loc[activity_metrics.index.repeat(activity_metrics["elapsed_time"].astype(int))].reset_index(drop=True)
                activity_metrics_secondGap["elapsed_time"] = 1
                activity_metrics_secondGap["cumsum_time_min"] = ((activity_metrics_secondGap["elapsed_time"].cumsum() / 60) // 1).astype(int)
                activity_metrics_agg = activity_metrics_secondGap.groupby("cumsum_time_min")[["heartrate_value", "speed_value", "gradeAdjustedSpeed_value", "verticalSpeed_value"]].mean().reset_index(drop=True).quantile([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])

            elif activity_parenttypeId == 2: # Cycling

                sumMovingDuration_index = next((d["metricsIndex"] for d in (metricDescriptors or []) if d["key"] == "sumMovingDuration"), np.nan) # factor = 1000.0 [ms] ~> x / 1000 -> s
                directSpeed_index = next((d["metricsIndex"] for d in (metricDescriptors or []) if d["key"] == "directSpeed"), np.nan) # factor = 0.1 [dm/s] ~> x / 0.1 -> m/s
                directPower_index = next((d["metricsIndex"] for d in (metricDescriptors or []) if d["key"] == "directPower"), np.nan) # factor = 1.0 [w] ~> x / 1 -> w 
                directHeartRate_index = next((d["metricsIndex"] for d in (metricDescriptors or []) if d["key"] == "directHeartRate"), np.nan) # factor = 1.0 [bpm]  ~> x / 1 -> bpm

                elapsed_times = [] # seconds
                power_values = [] # watts
                speed_values = [] # min/km
                heartrate_values = [] # bpm

                previous_time_s = 0
                for entry in (metricValues or []):

                    try:
                        current_time_s = entry["metrics"][sumMovingDuration_index]
                        elapsed_time_s = (current_time_s - previous_time_s) 
                        elapsed_times += [elapsed_time_s]
                        previous_time_s = current_time_s
                    except:
                        elapsed_times += [np.nan]

                    try:
                        speed_ms = entry["metrics"][directSpeed_index] 
                        speed_kmh = speed_ms * 3.6
                        speed_values += [speed_kmh] 
                    except:
                        speed_values += [np.nan]

                    try:
                        power_values += [entry["metrics"][directPower_index]]
                    except:
                        power_values += [np.nan]

                    try:
                        heartrate_values += [entry["metrics"][directHeartRate_index]]
                    except:
                        heartrate_values += [np.nan]

                activity_metrics = pd.DataFrame({
                    "elapsed_time": elapsed_times,
                    "power_value": power_values,
                    "speed_value": speed_values,
                    "heartrate_value": heartrate_values,
                    })
                
                activity_metrics_secondGap = activity_metrics.loc[activity_metrics.index.repeat(activity_metrics["elapsed_time"].astype(int))].reset_index(drop=True)
                activity_metrics_secondGap["elapsed_time"] = 1
                activity_metrics_secondGap["cumsum_time_min"] = ((activity_metrics_secondGap["elapsed_time"].cumsum() / 60) // 1).astype(int)
                activity_metrics_agg = activity_metrics_secondGap.groupby("cumsum_time_min")[["heartrate_value", "speed_value", "power_value"]].mean().reset_index(drop=True).quantile([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])

            else: # Other, not known
                activity_metrics_agg = pd.DataFrame({
                    "heartrate_value": np.repeat(np.nan, 10),
                    "speed_value": np.repeat(np.nan, 10),
                    "power_value": np.repeat(np.nan, 10),
                })

            # All in one
            singleActivity_activityScores = {

                "Year": selectedDate.year,
                "Month": selectedDate.month,
                "Day": selectedDate.day,
                "Weekday": selectedDate.strftime("%A"),

                "Description": activity_name,
                "Activity type": activity_typeName,
                "Start time": activity_startTime,
                "Location": activity_locationName,

                "Distance [km]": round(activity_distance / 1000,1),
                "Duration [h]": round(activity_duration / 3600,2),
                "Elevation gain [m]": round(activity_elevationGain,0),
                "Average pace [min/km] or speed [km/h]": round(60 / (activity_averageSpeed * 3.6), 2) if activity_parenttypeId == 1 else round(activity_averageSpeed * 3.6, 2) if activity_parenttypeId == 2 else np.nan,
                "Gradient adjusted pace [min/km]": round(60 / (activity_avgGradeAdjustedSpeed * 3.6), 2) if activity_parenttypeId == 1 else np.nan,
                "Average heart rate": round(activity_averageHR,0) if not np.isnan(activity_averageHR) else np.nan, 
                "Maximum heart rate": round(activity_maxHR,0) if not np.isnan(activity_maxHR) else np.nan, 
                "Normalized power [w]":  round(activity_normalizedPower,0) if not np.isnan(activity_normalizedPower) else np.nan,
                "Calories [kcal]": round(activity_calories,0) if not np.isnan(activity_calories) else np.nan,

                "Aerobic training effect": round(activity_aerobicTrainingEffect,1),
                "Aerobic training effect message": activity_aerobicTrainingEffectMessage,
                "Anaerobic training effect": round(activity_anaerobicTrainingEffect,1),
                "Anaerobic training effect message": activity_anaerobicTrainingEffectMessage,
                "Training effect label": activity_trainingEffectLabel,
                "Training load": round(activity_trainingLoad,1),
                "Vo2Max value": round(activity_vO2MaxValue,1),

                "Time in Z1 [h]": float(round(activity_zones_df.query("zone == 1")["duration"].iloc[0] / 3600, 2)),
                "Time in Z2 [h]": float(round(activity_zones_df.query("zone == 2")["duration"].iloc[0] / 3600, 2)),
                "Time in Z3 [h]": float(round(activity_zones_df.query("zone == 3")["duration"].iloc[0] / 3600, 2)),
                "Time in Z4 [h]": float(round(activity_zones_df.query("zone == 4")["duration"].iloc[0] / 3600, 2)),
                "Time in Z5 [h]": float(round(activity_zones_df.query("zone == 5")["duration"].iloc[0] / 3600, 2)),

                "10% heart rate [1]": float(round(activity_metrics_agg.iloc[0]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[0]["heartrate_value"]) else float(np.nan),
                "10% heart rate [2]": float(round(activity_metrics_agg.iloc[1]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[1]["heartrate_value"]) else float(np.nan),
                "10% heart rate [3]": float(round(activity_metrics_agg.iloc[2]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[2]["heartrate_value"]) else float(np.nan),
                "10% heart rate [4]": float(round(activity_metrics_agg.iloc[3]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[3]["heartrate_value"]) else float(np.nan),
                "10% heart rate [5]": float(round(activity_metrics_agg.iloc[4]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[4]["heartrate_value"]) else float(np.nan),
                "10% heart rate [6]": float(round(activity_metrics_agg.iloc[5]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[5]["heartrate_value"]) else float(np.nan),
                "10% heart rate [7]": float(round(activity_metrics_agg.iloc[6]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[6]["heartrate_value"]) else float(np.nan),
                "10% heart rate [8]": float(round(activity_metrics_agg.iloc[7]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[7]["heartrate_value"]) else float(np.nan),
                "10% heart rate [9]": float(round(activity_metrics_agg.iloc[8]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[8]["heartrate_value"]) else float(np.nan),
                "10% heart rate [10]": float(round(activity_metrics_agg.iloc[9]["heartrate_value"],0)) if len(activity_metrics_agg) > 0 and not np.isnan(activity_metrics_agg.iloc[8]["heartrate_value"]) else float(np.nan),

            }

            # Multiple activies
            singleActivity_activityScores = replace_nan_with_empty_string(singleActivity_activityScores)
            activityScores["activity_{}".format(i)] = singleActivity_activityScores

    # Return
    return activityScores