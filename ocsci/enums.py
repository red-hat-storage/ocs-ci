from enum import Enum


class TestStatus(Enum):
    PASSED = 0
    FAILED = 1
    NOT_EXECUTED = 2
    SKIPPED = 3
