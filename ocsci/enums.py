from enum import Enum


class StatusOfTest(Enum):
    # As we have function to transform RC to StatusOfTest Enum we should add new
    # statuses with negative number to avoid miss tranforming from Unix RC to
    # some of our statuses!
    PASSED = 0
    FAILED = 1
    NOT_EXECUTED = -1
    SKIPPED = -2


class ReturnCode(Enum):
    UNSUPPORTED_WINDOWS_RUN = 1
