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

## Target Layers

The project should distinguish between several related but different targets.

### Observed performance measurement

Gradient-adjusted pace is useful, but it is not sufficient by itself. The quality of a run also depends on factors such as:

- Average heart rate.
- Training load before the activity.
- Recent fatigue and recovery.
- Temperature and weather, if available.
- Time of day.
- Total duration and distance.
- Elevation and terrain.
- Activity type, route, and workout context.

The first observed performance measurement should therefore be a context-adjusted or context-conditioned performance measure rather than raw gradient-adjusted pace alone.

Some context is already available in the current data. Temperature and weather are not currently present in the three source dataframes and would need to be added later or treated as unobserved context.

We should be cautious about manually normalizing every factor into one score. A better direction may be to model the raw performance observation together with its context and infer what the performance would mean under a common reference context. This allows the uncertainty in the adjustments to be represented by the Bayesian model.

### Latent fitness state

The context-adjusted performance observation is still not the same thing as fitness. It is one noisy measurement that helps estimate fitness.

Latent fitness is the underlying, slowly changing ability that explains many performance observations over time after accounting for context, fatigue, and noise. It is the main state we ultimately want to track.

### Future-run prediction

The model should also eventually predict the performance distribution of a future run when its intended run type and context are specified.

This may allow predictions such as the expected performance of the next:

- Easy or Z2 run.
- Tempo or threshold run.
- VO2max or high-intensity run.
- Long run.

These categories are examples, not final definitions. They must be defined from the available data and should not be assumed to have enough comparable observations before we inspect them.

## How Bayesian Statistics Enters the Problem

Bayesian statistics is not just a normalization formula. It can be introduced at several levels:

- Uncertain effects of heart rate, training load, time of day, duration, elevation, and other context variables.
- Uncertainty about the underlying fitness level.
- Uncertainty about the current fatigue state.
- Uncertainty in the measurement noise of individual runs.
- Uncertainty about the expected performance of a future run.
- Probability statements about improvement, stagnation, or decline.

The model can use prior distributions for these unknown quantities, combine them with the observed data through a likelihood, and produce posterior distributions. The posterior uncertainty can then flow into the fitness curve and future-run predictions.

The preferred direction is therefore to avoid prematurely constructing a hand-designed normalized fitness score. Instead, we should investigate whether a Bayesian measurement model can estimate a context-adjusted performance level and a latent fitness state together.

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
- What is the current fitness curve for the overall running signal?
- Do different running types show different fitness curves?
- What performance should be expected for the next run of a specified type?

The intended interpretation is something like:

> Fitness is probably improving, but uncertainty remains moderate. Short-term fatigue is also elevated, so recent performance may underestimate underlying fitness.

This is more useful for training decisions than a precise predicted race time.

## Fitness Curves and Run Types

The project should aim to produce a fitness curve over time rather than only a single current number.

There may eventually be:

- One overall running-fitness curve.
- Separate curves for different running types or aerobic groups.
- A shared component across all run types plus type-specific differences.

Possible run types include easy/Z2, tempo/threshold, VO2max/high intensity, and long running. The final categories should be based on actual workout labels, heart-rate information, duration, training effects, and data availability.

The first research effort should focus on one clearly defined type or one overall signal. Multiple curves should be introduced only after we understand whether the data supports comparable observations within each group.

The curves and future-run predictions are related but not identical:

- A fitness curve is an estimate of the current underlying state.
- A future-run prediction combines that state with expected fatigue, run type, and context.
- A fitness curve may improve while the next predicted run remains modest because fatigue is high.
- A prediction may be uncertain even when the current fitness estimate is stable because the future context is uncertain.

The two outputs should generally inform each other, but they answer different questions and should be evaluated separately.

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

The initial direction is not to treat gradient-adjusted pace as a complete performance metric. It should be used as the main raw performance signal inside a model that accounts for relevant context.

Candidate performance and context variables are:

- Running gradient-adjusted pace as the main performance measurement.
- Running duration and distance.
- Elevation gain.
- Average heart rate.
- Training load and recent/baseline load.
- Time of day.
- Activity type, route, and workout description.
- Temperature and weather if they become available.
- Resting HR.
- HRV deviation from its baseline range.
- Sleep score and sleep time.
- Recent training volume and intensity distribution.

The first performance model should probably use running-only observations. Cycling and other activities could contribute to general fatigue later, but mixing modalities directly into the first performance measurement would make interpretation more difficult.

The initial goal is to find a performance representation that is reasonably comparable across sessions. This may be one overall running signal plus additional type-specific signals rather than one universal metric for every workout.

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

## Questions to Resolve Before Modeling

- What should count as the first comparable performance observation?
- Should the first model use all running sessions or a narrower class of steady, tempo, threshold, or benchmark efforts?
- Which three or four run types are sufficiently well represented to support separate prediction curves?
- Should type-specific curves be separate models initially, or should they share information through a hierarchical structure later?
- How should temperature and weather be obtained if they are important context variables?
- What practical performance difference should count as meaningful improvement or stagnation?
- Should future-run validation target the next individual run, the next comparable run of the same type, or a short-window summary?

## Scope Status

The data inventory, project goal, target-layer distinction, Bayesian role, fitness-curve idea, and multi-run-type direction above are now part of the working plan. The exact performance representation, run categories, normalization or measurement model, and validation target remain open for discussion before implementation.
