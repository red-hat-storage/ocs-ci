class CommandFailed(Exception):
    pass


class UnsupportedOSType(Exception):
    pass


class CephHealthException(Exception):
    pass


class NoobaaHealthException(Exception):
    pass


class UnexpectedBehaviour(Exception):
    pass


class ClassCreationException(Exception):
    pass


class ResourceLeftoversException(Exception):
    pass


class TimeoutExpiredError(Exception):
    message = "Timed Out"

    def __init__(self, value, custom_message=None):
        self.value = value
        self.custom_message = custom_message

    def __str__(self):
        if self.custom_message is None:
            self.message = f"{self.__class__.message}: {self.value}"
        else:
            self.message = self.custom_message
        return self.message


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
    def __init__(
        self, resource_or_name, describe_out=None, column=None, expected=None, got=None
    ):
        if isinstance(resource_or_name, str):
            self.resource = None
            self.resource_name = resource_or_name
        else:
            self.resource = resource_or_name
            self.resource_name = self.resource.name
        self.describe_out = describe_out

    def __str__(self):
        if self.resource:
            msg = f"{self.resource.kind} resource {self.resource_name}"
        else:
            msg = f"Resource {self.resource_name}"
        if self.column:
            msg += f" in column {self.column}"
        if self.got:
            msg += f" was in state {self.got}"
        if self.expected:
            msg += f" but expected {self.expected}"
        if self.describe_out:
            msg += f" describe output: {self.describe_out}"
        return msg


class UnavailableResourceException(Exception):
    pass


class TagNotFoundException(Exception):
    pass


class ResourceNameNotSpecifiedException(Exception):
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


class CSVNotFound(Exception):
    pass


class UnsupportedPlatformError(Exception):
    pass


class UnsupportedPlatformVersionError(Exception):
    pass


class UnsupportedFeatureError(Exception):
    pass


class UnsupportedBrowser(Exception):
    pass


class OpenshiftConsoleSuiteNotDefined(Exception):
    pass


class ServiceUnavailable(Exception):
    pass


class InvalidStatusCode(Exception):
    pass


class NoBucketPolicyResponse(Exception):
    pass


class PSIVolumeCreationFailed(Exception):
    pass


class PSIVolumeNotInExpectedState(Exception):
    pass


class PSIVolumeDeletionFailed(Exception):
    pass


class FlexyDataNotFound(Exception):
    pass


class PendingCSRException(Exception):
    pass


class RDMDiskNotFound(Exception):
    pass


class ExternalClusterDetailsException(Exception):
    pass


class CredReqSecretNotFound(Exception):
    pass


class RhcosImageNotFound(Exception):
    pass


class FipsNotInstalledException(Exception):
    pass


class StorageNotSufficientException(Exception):
    pass


class PoolNotFound(Exception):
    pass


class PoolDataNotErased(Exception):
    pass


class PvcNotDeleted(Exception):
    pass


class MemoryNotSufficientException(Exception):
    pass


class CPUNotSufficientException(Exception):
    pass


class PoolNotCompressedAsExpected(Exception):
    pass


class PoolNotReplicatedAsNeeded(Exception):
    pass


class ImageIsNotDeletedOrNotFound(Exception):
    pass


class VaultDeploymentError(Exception):
    pass


class VaultOperationError(Exception):
    pass


class KMSNotSupported(Exception):
    pass


class KMSConnectionDetailsError(Exception):
    pass


class KMSTokenError(Exception):
    pass


class KMSResourceCleaneupError(Exception):
    pass


class UnhealthyBucket(Exception):
    pass


class NotFoundError(Exception):
    pass
