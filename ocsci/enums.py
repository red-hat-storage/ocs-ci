from enum import Enum


class TestStatus(Enum):
    PASSED = 0
    FAILED = 1
    NOT_EXECUTED = 2
    SKIPPED = 3


class ReturnCode(Enum):
    UNSUPPORTED_WINDOWS_RUN = 1
