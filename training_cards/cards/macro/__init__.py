from .base_development import base_development
from .build_phase import build_phase
from .peak_and_taper import peak_and_taper
from .race_specific_preparation import race_specific_preparation
from .recovery_and_reset import recovery_and_reset
from .return_to_consistency import return_to_consistency

MACRO_CARDS = [
    return_to_consistency,
    base_development,
    build_phase,
    race_specific_preparation,
    peak_and_taper,
    recovery_and_reset,
]

