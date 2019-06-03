class CommandFailed(Exception):
    pass


class UnsupportedOSType(Exception):
    pass


class CephHealthException(Exception):
    pass


# TODO: Delete once moved to pytest
class UnknownStatusOfTestException(Exception):
    pass


class ClassCreationException(Exception):
    pass


class TimeoutExpiredError(Exception):
    message = 'Timed Out'

    def __init__(self, *value):
        self.value = value

    def __str__(self):
        return f"{self.message}: {self.value}"
