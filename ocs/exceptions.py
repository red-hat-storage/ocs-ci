class CommandFailed(Exception):
    pass


class UnsupportedOSType(Exception):
    pass


class CephHealthException(Exception):
    pass


class UnknownTestStatusException(Exception):
    pass


class TimeoutExpiredError(Exception):
    message = 'Timed Out'

    def __init__(self, *value):
        self.value = value

    def __str__(self):
        return f"{self.message}: {self.value}"


class PullSecretFileNotFound(Exception):
    pass
