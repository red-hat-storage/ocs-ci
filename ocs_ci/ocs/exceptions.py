class CommandFailed(Exception):
    pass


class UnexpectedDeploymentConfiguration(Exception):
    pass


class UnsupportedOSType(Exception):
    pass


class CephHealthException(Exception):
    pass


class NoobaaHealthException(Exception):
    pass


class NoobaaCliChecksumFailedException(Exception):
    pass


class UnexpectedBehaviour(Exception):
    pass


class UnexpectedInput(Exception):
    pass


class ClassCreationException(Exception):
    pass


class ResourceLeftoversException(Exception):
    pass


class ObjectsStillBeingDeletedException(Exception):
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
        self.column = column
        self.expected = expected
        self.got = got

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


class NotSupportedException(Exception):
    pass


class NonUpgradedImagesFoundError(Exception):
    pass


class NotAllPodsHaveSameImagesError(Exception):
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


class PassThroughEnabledDeviceNotFound(Exception):
    pass


class ExternalClusterDetailsException(Exception):
    pass


class ExternalClusterRGWAdminOpsUserException(Exception):
    pass


class ExternalClusterExporterRunFailed(Exception):
    pass


class ExternalClusterObjectStoreUserCreationFailed(Exception):
    pass


class ExternalClusterRGWEndPointMissing(Exception):
    pass


class ExternalClusterRGWEndPointPortMissing(Exception):
    pass


class ExternalClusterCephfsMissing(Exception):
    pass


class ExternalClusterCephSSHAuthDetailsMissing(Exception):
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


class PoolDidNotReachReadyState(Exception):
    pass


class PoolStateIsUnknow(Exception):
    pass


class PoolNotDeleted(Exception):
    pass


class PoolDataNotErased(Exception):
    pass


class PoolSizeWrong(Exception):
    pass


class PoolCompressionWrong(Exception):
    pass


class PoolNotDeletedFromUI(Exception):
    pass


class PoolCephValueNotMatch(Exception):
    pass


class StorageClassNotDeletedFromUI(Exception):
    pass


class PvcNotDeleted(Exception):
    pass


class StorageclassNotCreated(Exception):
    pass


class StorageclassIsNotDeleted(Exception):
    pass


class ResourceNotDeleted(Exception):
    pass


class PageNotLoaded(Exception):
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


class HPCSDeploymentError(Exception):
    pass


class KMIPDeploymentError(Exception):
    pass


class KMIPOperationError(Exception):
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


class ResourcePoolNotFound(Exception):
    pass


class ClientDownloadError(Exception):
    pass


class NotAllNodesCreated(Exception):
    pass


class TemplateNotFound(Exception):
    pass


class PVNotSufficientException(Exception):
    pass


class IPAMReleaseUpdateFailed(Exception):
    pass


class IPAMAssignUpdateFailed(Exception):
    pass


class NodeHasNoAttachedVolume(Exception):
    pass


class NotSupportedProxyConfiguration(Exception):
    pass


class OCSWorkerScaleFailed(Exception):
    pass


class OSDScaleFailed(Exception):
    pass


class PVCNotCreated(Exception):
    pass


class PodNotCreated(Exception):
    pass


class RBDSideCarContainerException(Exception):
    pass


class ElasticSearchNotDeployed(Exception):
    pass


class ManagedServiceAddonDeploymentError(Exception):
    pass


class ManagedServiceSecurityGroupNotFound(Exception):
    pass


class ConfigurationError(Exception):
    pass


class DRPrimaryNotFoundException(Exception):
    pass


class InteractivePromptException(Exception):
    pass


class BenchmarkTestFailed(Exception):
    pass


class ACMClusterDeployException(Exception):
    pass


class ACMClusterImportException(Exception):
    pass


class RDRDeploymentException(Exception):
    pass


class MDRDeploymentException(Exception):
    pass


class ACMClusterDestroyException(Exception):
    pass


class ACMClusterConfigurationException(Exception):
    pass


class WrongVersionExpression(ValueError):
    pass


class ClusterNotFoundException(Exception):
    pass


class AlertingError(Exception):
    pass


class AuthError(Exception):
    pass


class UnknownCloneTypeException(Exception):
    pass


class CephToolBoxNotFoundException(Exception):
    pass


class UnsupportedWorkloadError(Exception):
    pass


class RebootEventNotFoundException(Exception):
    pass


class ConnectivityFail(Exception):
    pass


class ROSAProdAdminLoginFailedException(Exception):
    pass


class Md5CheckFailed(Exception):
    pass


class ZombieProcessFoundException(Exception):
    pass


class LvSizeWrong(Exception):
    pass


class LvDataPercentSizeWrong(Exception):
    pass


class LvThinUtilNotChanged(Exception):
    pass


class ThinPoolUtilityWrong(Exception):
    pass


class LVMOHealthException(Exception):
    pass


class VolumesExistError(Exception):
    pass


class LeftoversExistError(Exception):
    pass


class ExternalClusterNodeRoleNotFound(Exception):
    pass


class UnexpectedODFAccessException(Exception):
    pass


class UnknownOperationForTerraformVariableUpdate(Exception):
    pass


class TerrafromFileNotFoundException(Exception):
    pass


class IncorrectUiOptionRequested(Exception):
    def __init__(self, text, func=None):
        super().__init__(text)
        if func is not None:
            func()


class ReturnedEmptyResponseException(Exception):
    pass


class ArchitectureNotSupported(Exception):
    pass


class MissingDecoratorError(Exception):
    pass


class PDBNotCreatedException(Exception):
    pass


class UnableUpgradeConnectionException(Exception):
    pass


class NoThreadingLockUsedError(Exception):
    pass


class VSLMNotFoundException(Exception):
    pass


class VolumePathNotFoundException(Exception):
    pass


class OperationFailedToCompleteException(Exception):
    pass


class HyperConvergedHealthException(Exception):
    pass


class OpenShiftAPIResponseException(Exception):
    def __init__(self, response):
        self.response = response

    def __str__(self):
        msg = f"{self.response.status_code} {self.response.reason} ({self.response.text.strip()})"
        return msg


class HostValidationFailed(Exception):
    pass


class SameNameClusterAlreadyExistsException(Exception):
    pass


class NoRunningCephToolBoxException(Exception):
    pass


class UsernameNotFoundException(Exception):
    pass


class MultiStorageClusterExternalCephHealth(Exception):
    pass


class StorageSizeNotReflectedException(Exception):
    pass


class ClusterNotInSTSModeException(Exception):
    pass


class APIRequestError(Exception):
    pass


class ACMObservabilityNotEnabled(Exception):
    pass


class ProviderModeNotFoundException(Exception):
    pass
