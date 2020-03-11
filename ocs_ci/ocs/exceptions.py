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


class TagNotFoundException(Exception):
    pass


class ResourceNameNotSpecifiedException(Exception):
    pass


class ResourceInUnexpectedState(Exception):
    pass


class VMMaxDisksReachedException(Exception):
    pass


class SameNamePrefixClusterAlreadyExistsException(Exception):
    pass


class MissingRequiredConfigKeyError(Exception):
    pass


class NotSupportedFunctionError(Exception):
    pass


class NonUpgradedImagesFoundError(Exception):
    pass


class UnexpectedImage(Exception):
    pass


class UnexpectedVolumeType(Exception):
    pass


class FailedToAddNodeException(Exception):
    pass


class FailedToRemoveNodeException(Exception):
    pass


class FailedToDeleteInstance(Exception):
    pass


class NoInstallPlanForApproveFoundException(Exception):
    pass


class NoobaaConditionException(Exception):
    pass


class NodeNotFoundError(Exception):
    pass


class ResourceNotFoundError(Exception):
    pass


class ChannelNotFound(Exception):
    pass


class UnsupportedPlatformError(Exception):
    pass


class UnsupportedBrowser(Exception):
    pass


class OpenshiftConsoleSuiteNotDefined(Exception):
    pass


class ServiceUnavailable(Exception):
    pass
