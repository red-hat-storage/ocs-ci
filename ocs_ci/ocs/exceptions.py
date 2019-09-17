class CommandFailed(Exception):
    pass


class UnsupportedOSType(Exception):
    pass


class CephHealthException(Exception):
    pass


class UnexpectedBehaviour(Exception):
    pass


class ClassCreationException(Exception):
    pass


class ResourceLeftoversException(Exception):
    pass


class TimeoutExpiredError(Exception):
    message = 'Timed Out'

    def __init__(self, *value):
        self.value = value

    def __str__(self):
        return f"{self.message}: {self.value}"


class TimeoutException(Exception):
    pass


class MonCountException(Exception):
    pass


class MDSCountException(Exception):
    pass


class DeploymentPlatformNotSupported(Exception):
    pass


class UnavailableBuildException(Exception):
    pass


class PerformanceException(Exception):
    pass


class ResourceWrongStatusException(Exception):

    def __init__(self, resource_name, describe_out):
        self.resource_name = resource_name
        self.describe_out = describe_out

    def __str__(self):
        return f"Resource {self.resource_name} describe output: {self.describe_out}"


class UnavailableResourceException(Exception):
    pass


<<<<<<< HEAD
class TagNotFoundException(Exception):
    pass


class ResourceNameNotSpecifiedException(Exception):
    pass


class ResourceInUnexpectedState(Exception):
=======
class VMMaxDisksReachedException(Exception):
>>>>>>> Adding constants and exception required for vSphere platform
    pass


class SameNamePrefixClusterAlreadyExistsException(Exception):
    pass
