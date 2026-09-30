# Bayesian Performance Prediction — Working Plan

This file is intentionally strict and should only be updated when we explicitly agree to add or change something.

## Agreed Project Goal

Build an interpretable Bayesian monitoring model that estimates a slow-moving latent running-fitness state and a faster short-term fatigue state from repeated running-performance, training-load, and recovery observations.

The purpose is to understand whether fitness is probably improving, stagnating, or declining, while quantifying uncertainty and separating underlying fitness from temporary fatigue. The initial goal is personal fitness monitoring and learning Bayesian statistics, not predicting a race finish time.

The project should develop step by step. We will begin with methods that are understandable and useful, then decide where the evidence and our learning lead us.

## Bayesian Learning Scope

Bayesian statistics is a family of methods rather than one single technique. We will learn and use a manageable progression:

1. Bayesian updating and uncertainty as the foundation.
2. Bayesian regression for relationships between performance, training, and recovery.
3. Hierarchical models when information needs to be shared across workout types, routes, or periods.
4. State-space or dynamic models for hidden fitness and fatigue that evolve over time.
5. Change-point or trend models later, if they help identify meaningful stagnation or shifts.

We will not begin with the most advanced methods. Gaussian processes, particle filters, and Bayesian causal models are possible future topics, but they should not complicate this project before we have a clear basic model.

Those methods can become separate future projects related to this one, where we can learn more advanced Bayesian modeling deliberately.

## Core Model Concept

True fitness is not directly observed. We observe imperfect signals such as pace, heart rate, elevation, training load, sleep, and HRV.

The main conceptual structure is:

```text
Observed performance
≈ latent fitness
− short-term fatigue
+ workout and context effects
+ measurement noise
```

The model should estimate at least two hidden states:

- Latent fitness: a relatively slow-moving estimate of underlying running ability.
- Short-term fatigue/readiness: a faster-moving state that can temporarily hide or reduce fitness.

The main fitness metric should be this estimated latent state, not an arbitrary average of VO2 max, HRV, training load, pace, and sleep. Those variables have different roles and should not automatically be collapsed into one composite score.

## Intended Outputs

The model should eventually help answer questions such as:

- What is my current estimated latent fitness?
- Is fitness probably improving over the last 4, 8, or 12 weeks?
- What is the credible interval around the current estimate?
- Is the posterior probability of improvement meaningfully greater than 50%?
- Is fitness stagnating, or is the apparent stagnation explained by noise?
- Is short-term fatigue unusually high?
- Is recovery worsening while fitness is improving?
- How confident should I be in the current estimate?

The intended interpretation is something like:

> Fitness is probably improving, but uncertainty remains moderate. Short-term fatigue is also elevated, so recent performance may underestimate underlying fitness.

This is more useful for training decisions than a precise predicted race time.

## Available Data

### Daily Statistics Urh — `Raw Daily Data`

The daily sheet produces one record per day with the following groups of metrics.

#### Calendar

- Year
- Month
- Day
- Weekday

#### Recovery and readiness

- Resting HR
- Sleep score
- Sleep time [h]
- HRV
- HRV baseline lower
- HRV baseline upper

Sleep score and HRV can be missing on some days. The HRV baseline bounds provide a useful reference for interpreting the daily HRV value.

#### Daily activity and lifestyle

- Meters ascended [m]
- Highly active time [h]
- Active time [h]
- Sedentary time [h]

#### Garmin fitness indicators

- vo2Max
- Hill score
- Endurance score

#### Training-load indicators

- Low aerobic load
- High aerobic load
- Anaerobic load

Important interpretation: these load fields come from Garmin's monthly training-load balance. Although they are written once per day, they should not automatically be interpreted as that day's training load.

### Activity Statistics Urh — `Raw Activity Data`

The activity sheet contains activity rows as well as rest rows. It includes running, trail running, cycling, hiking, and swimming activities.

#### Activity identity

- Year, Month, Day, Weekday
- Description
- Activity type
- Start time
- Location

#### Training volume

- Distance [km]
- Duration [h]
- Elevation gain [m]
- Calories [kcal]

#### Performance and intensity

- Average pace [min/km] or speed [km/h]
- Gradient adjusted pace [min/km]
- Average heart rate
- Maximum heart rate
- Normalized power [w]

#### Training response

- Aerobic training effect
- Aerobic training effect message
- Anaerobic training effect
- Anaerobic training effect message
- Training effect label
- Training load
- Vo2Max value

#### Heart-rate distribution

- Time in Z1 [h]
- Time in Z2 [h]
- Time in Z3 [h]
- Time in Z4 [h]
- Time in Z5 [h]
- 10% heart rate [1] through 10% heart rate [10]

The ten heart-rate fields are quantiles from activity-level, minute-based heart-rate summaries.

### Activity Statistics Urh — `HASR-TL`

`HASR-TL` is a derived activity-level training-load analysis based mainly on Training load, Duration, and Training load per minute.

#### Session context and classification

- Year, Month, Day, Weekday
- Description
- Activity Type
- Start time
- Aggregate variable
- Session Baseline Overall Rank
- Session Baseline Class
- Session Baseline Class Rank

#### Combined training-load indicators

- HASR-TL
- HASR-TL Recent
- HASR-TL Baseline

#### Recent and baseline session buckets

- Recent Easy
- Baseline Easy
- Recent Easy prop. [%]
- Baseline Easy prop. [%]
- Recent Hard
- Baseline Hard
- Recent Hard prop. [%]
- Baseline Hard prop. [%]
- Recent Long
- Baseline Long
- Recent Long prop. [%]
- Baseline Long prop. [%]

The current calculation uses a 90-day baseline window, a 21-day recent window, exponentially decaying weights, a 70th-percentile training-load-per-minute threshold for Hard sessions, and a 75th-percentile duration threshold for Long sessions. HASR-TL combines Easy, Hard, and Long load with weights of 15%, 45%, and 40% respectively.

## Initial Modeling Interpretation

The available metrics can be organized into four roles:

| Role | Candidate metrics |
| --- | --- |
| Performance observations | Running pace, gradient-adjusted pace, duration, distance, average HR |
| Training input | Training load, duration, elevation, aerobic/anaerobic effect, HR zones |
| Recovery and fatigue | Resting HR, HRV relative to baseline, sleep score, sleep time |
| Fitness and readiness | VO2 max, hill score, endurance score, HASR-TL recent/baseline |

## Current Recommendations and Evaluations

For an initial performance model, the strongest candidate metrics are likely:

- Running gradient-adjusted pace as the main performance measurement.
- Running duration and distance.
- Elevation gain.
- Average heart rate.
- Training load and recent/baseline load.
- Resting HR.
- HRV deviation from its baseline range.
- Sleep score and sleep time.
- Recent training volume and intensity distribution.

The first performance model should probably use running-only observations. Cycling and other activities could contribute to general fatigue later, but mixing modalities directly into the first performance measurement would make interpretation more difficult.

VO2 max, Hill score, and Endurance score are useful Garmin-derived indicators and possible auxiliary covariates. They should not automatically be treated as independent ground-truth measurements of performance.

HASR-TL is already derived from Training load and Duration. Including raw load, raw duration, all HASR components, and HASR-TL together could double-count the same information. The model should choose a clear representation rather than include every related column by default.

## Important Data Considerations

- Pace and gradient-adjusted pace are primarily meaningful for running.
- Average speed is used for cycling and is stored in the same activity field as running pace.
- Normalized power is more useful for cycling than running.
- Activity-level VO2 max is not available consistently across all activity types.
- Rest rows should not be treated as ordinary performance observations.
- Missing values need to be handled explicitly rather than silently converted to zero.
- A daily aggregation layer will be needed before using activity-level data in a daily latent-state model.
- Recovery variables may need lagging so that future information does not leak into historical predictions.

## Scope Status

The data inventory, project goal, Bayesian learning scope, and initial modeling direction above are agreed working principles. The detailed model specification, feature definitions, validation approach, and implementation steps will be decided incrementally as we build and learn.
