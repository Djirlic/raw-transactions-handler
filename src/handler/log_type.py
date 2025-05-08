from enum import Enum


class LogType(str, Enum):
    REFINEMENT = "refinement"
    QUARANTINE = "quarantine"
