from .aerobic_power_intervals import aerobic_power_intervals
from .downhill_conditioning_run import downhill_conditioning_run
from .easy_run import easy_run
from .hiking_or_power_hiking_practice import hiking_or_power_hiking_practice
from .long_run import long_run
from .progression_run import progression_run
from .race_simulation_run import race_simulation_run
from .recovery_run import recovery_run
from .short_hill_repeats import short_hill_repeats
from .steady_run import steady_run
from .strength_endurance_hills import strength_endurance_hills
from .strides import strides
from .tempo_run import tempo_run
from .threshold_intervals import threshold_intervals

SESSION_CARDS = [
    easy_run,
    recovery_run,
    long_run,
    progression_run,
    steady_run,
    tempo_run,
    threshold_intervals,
    aerobic_power_intervals,
    short_hill_repeats,
    strength_endurance_hills,
    strides,
    race_simulation_run,
    hiking_or_power_hiking_practice,
    downhill_conditioning_run,
]

