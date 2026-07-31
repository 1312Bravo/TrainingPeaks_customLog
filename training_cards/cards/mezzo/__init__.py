from .aerobic_power_block import aerobic_power_block
from .easy_volume_block import easy_volume_block
from .endurance_development_block import endurance_development_block
from .fueling_practice_block import fueling_practice_block
from .long_endurance_block import long_endurance_block
from .race_practice_block import race_practice_block
from .recovery_block import recovery_block
from .strength_endurance_block import strength_endurance_block
from .threshold_development_block import threshold_development_block

MEZZO_CARDS = [
    easy_volume_block,
    endurance_development_block,
    strength_endurance_block,
    threshold_development_block,
    aerobic_power_block,
    long_endurance_block,
    race_practice_block,
    fueling_practice_block,
    recovery_block,
]

