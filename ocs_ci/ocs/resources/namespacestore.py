import logging

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import (
    create_resource,
    create_unique_resource_name,
    storagecluster_independent_check,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, get_attr_chain

log = logging.getLogger(__name__)


class NamespaceStore:
    """
    A class that represents NamespaceStore objects

    """

    def __init__(
        self,
        name,
        method,
        uls_name=None,
        secret_name=None,
        mcg_obj=None,
    ):
        self.name = name
        self.method = method
        self.uls_name = uls_name
        self.secret_name = secret_name
        self.mcg_obj = mcg_obj

    def delete(self, retry=True):
        """
        Deletes the current namespacestore by using OC/CLI commands

        Args:
            retry (bool): Whether to retry the deletion if it fails

        """
        log.info(f"Cleaning up namespacestore {self.name}")

        def _oc_deletion_flow():
            try:
                OCP(
                    kind="namespacestore",
                    namespace=config.ENV_DATA["cluster_namespace"],
                ).delete(resource_name=self.name)
                return True
            except CommandFailed as e:
                if "not found" in str(e).lower():
                    log.warning(f"Namespacestore {self.name} was already deleted.")
                    return True
                elif all(
                    err in e.args[0]
                    for err in ["cannot complete because pool", "in", "state"]
                ):
                    if retry:
                        log.warning(
                            f"Deletion of {self.name} failed due to its state; Retrying"
                        )
                        return False
                    else:
                        raise
                else:
                    raise

        def _cli_deletion_flow():
            try:
                self.mcg_obj.exec_mcg_cmd(f"namespacestore delete {self.name}")
                return True
            except CommandFailed as e:
                if "being used by one or more buckets" in str(e).lower():
                    log.warning(
                        f"Deletion of {self.name} failed because it's being used by a bucket. "
                        "Retrying..."
                    )
                else:
                    log.warning(f"Deletion of self.name failed. Error:\n{str(e)}")
                return False

        cmdMap = {
            "oc": _oc_deletion_flow,
            "cli": _cli_deletion_flow,
        }
        if retry:
            sample = TimeoutSampler(
                timeout=120,
                sleep=20,
                func=cmdMap[self.method],
            )
            if not sample.wait_for_func_status(result=True):
                err_msg = f"Failed to delete {self.name}"
                log.error(err_msg)
                raise TimeoutExpiredError(err_msg)
        else:
            cmdMap[self.method]()

        log.info(f"Verifying whether namespacestore {self.name} exists after deletion")
        ns_deleted_successfully = False

        if self.method == "oc":
            try:
                OCP(
                    kind=constants.NAMESPACESTORE,
                    namespace=config.ENV_DATA["cluster_namespace"],
                    resource_name=self.name,
                ).get()
            except CommandFailed as e:
                if "not found" in str(e).lower():
                    log.info(f"Namespacestore {self.name} was deleted.")
                    ns_deleted_successfully = True
                else:
                    raise
        elif self.method == "cli":
            if self.name not in self.mcg_obj.exec_mcg_cmd("namespacestore list").stdout:
                ns_deleted_successfully = True

        assert (
            ns_deleted_successfully
        ), f"Namespacestore {self.name} was not deleted successfully"

    def cli_verify_health(self):
        """
        Verify the health of the namespace store by running the `noobaa namespacestore status` command

        Returns:
            bool: Based on whether the namespace store is healthy or not
        """
        try:
            self.mcg_obj.exec_mcg_cmd(f"namespacestore status {self.name}")
            return True
        except CommandFailed as e:
            if "Not Found" in str(e) or "NotFound" in str(e):
                return False

    def oc_verify_health(self):
        """
        Verify the health of the namespace store by checking the status of the CR

        Returns:
            bool: Based on whether the namespace store is healthy or not

        """
        return (
            OCP(
                kind="namespacestore",
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=self.name,
            ).get()["status"]["phase"]
            == constants.STATUS_READY
        )

    def verify_health(self, timeout=180, interval=5):
        """
        Health verification function that tries to verify
        a namespacestores's health until a given time limit is reached

        Args:
            timeout (int): Timeout for the check, in seconds
            interval (int): Interval to wait between checks, in seconds

        Returns:
            (bool): True if the bucket is healthy, False otherwise

        """
        log.info(f"Waiting for {self.name} to be healthy")
        try:
            for health_check in TimeoutSampler(
                timeout, interval, getattr(self, f"{self.method}_verify_health")
            ):
                if health_check:
                    log.info(f"{self.name} is healthy")
                    return True
                else:
                    log.info(f"{self.name} is unhealthy. Rechecking.")
        except TimeoutExpiredError:
            log.error(
                f"{self.name} did not reach a healthy state within {timeout} seconds."
            )
            assert (
                False
            ), f"{self.name} did not reach a healthy state within {timeout} seconds."


def cli_create_namespacestore(
    nss_name,
    platform,
    mcg_obj,
    uls_name=None,
    cld_mgr=None,
    nss_tup=None,
):
    """
    Create a namespace filesystem namespacestore using YAMLs

    Args:
        nss_name (str): Name of the namespacestore
        platform (str): Platform to create the namespacestore on
        mcg_obj (MCG): An MCG object used for executing the MCG CLI commands
        uls_name (str): Name of the ULS bucket / PVC to use for the namespacestore
        cld_mgr (CloudManager): CloudManager object used for supplying the needed connection credentials
        nss_tup (tuple): A tuple containing the NSFS namespacestore details, in this order:
            pvc_name (str): Name of the PVC that will host the namespace filesystem
            pvc_size (int): Size in Gi of the PVC that will host the namespace filesystem
            sub_path (str): The path to a sub directory inside the PVC FS which the NSS will use as the root directory
            fs_backend (str): The file system backend type - CEPH_FS | GPFS | NFSv4. Defaults to None.

    """
    nss_creation_cmd = "namespacestore create "
    NSS_MAPPING = {
        constants.AWS_PLATFORM: lambda: (
            f"aws-s3 {nss_name} "
            f"--access-key {get_attr_chain(cld_mgr, 'aws_client.access_key')} "
            f"--secret-key {get_attr_chain(cld_mgr, 'aws_client.secret_key')} "
            f"--target-bucket {uls_name}"
        ),
        constants.AZURE_PLATFORM: lambda: (
            f"azure-blob {nss_name} "
            f"--account-key {get_attr_chain(cld_mgr, 'azure_client.credential')} "
            f"--account-name {get_attr_chain(cld_mgr, 'azure_client.account_name')} "
            f"--target-blob-container {uls_name}"
        ),
        constants.AZURE_WITH_LOGS_PLATFORM: lambda: (
            f"azure-blob {nss_name} "
            f"--secret-name {get_attr_chain(cld_mgr, 'azure_with_logs_client.secret.name')} "
            f"--target-blob-container {uls_name}"
        ),
        constants.RGW_PLATFORM: lambda: (
            f"s3-compatible {nss_name} "
            f"--endpoint {get_attr_chain(cld_mgr, 'rgw_client.endpoint')} "
            f"--access-key {get_attr_chain(cld_mgr, 'rgw_client.access_key')} "
            f"--secret-key {get_attr_chain(cld_mgr, 'rgw_client.secret_key')} "
            f"--target-bucket {uls_name}"
        ),
        constants.IBM_COS_PLATFORM: lambda: (
            f"s3-compatible {nss_name} "
            f"--endpoint {get_attr_chain(cld_mgr, 'ibmcos_client.endpoint')} "
            f"--access-key {get_attr_chain(cld_mgr, 'ibmcos_client.access_key')} "
            f"--secret-key {get_attr_chain(cld_mgr, 'ibmcos_client.secret_key')} "
            f"--target-bucket {uls_name}"
        ),
        constants.NAMESPACE_FILESYSTEM: lambda: (
            f"nsfs {nss_name} "
            f"--pvc-name {uls_name} "
            + (f"--sub-path {nss_tup[2]}" if nss_tup[2] else "")
            + (f"--fs-backend {nss_tup[3]} " if nss_tup[3] else "")
        ),
    }
    nss_creation_cmd += NSS_MAPPING[platform.lower()]()
    mcg_obj.exec_mcg_cmd(nss_creation_cmd, use_yes=True)


def oc_create_namespacestore(
    nss_name,
    platform,
    mcg_obj,
    uls_name=None,
    cld_mgr=None,
    nss_tup=None,
    nsfs_pvc_name=None,
):
    """
    Create a namespacestore using the MCG CLI

    Args:
        nss_name (str): Name of the namespacestore
        platform (str): Platform to create the namespacestore on
        mcg_obj (MCG): A redundant MCG object, used for uniformity between OC and CLI calls
        uls_name (str): Name of the ULS bucket to use for the namespacestore
        cld_mgr (CloudManager): CloudManager object used for supplying the needed connection credentials
        nss_tup (tuple): A tuple containing the NSFS namespacestore details, in this order:
            pvc_name (str): Name of the PVC that will host the namespace filesystem
            pvc_size (int): Size in Gi of the PVC that will host the namespace filesystem
            sub_path (str): The path to a sub directory inside the PVC FS which the NSS will use as the root directory
            fs_backend (str): The file system backend type - CEPH_FS | GPFS | NFSv4. Defaults to None.

    """
    nss_data = templating.load_yaml(constants.MCG_NAMESPACESTORE_YAML)
    nss_data["metadata"]["name"] = nss_name
    nss_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]

    NSS_MAPPING = {
        constants.AWS_PLATFORM: lambda: {
            "type": "aws-s3",
            "awsS3": {
                "targetBucket": uls_name,
                "secret": {
                    "name": get_attr_chain(cld_mgr, "aws_client.secret.name"),
                    "namespace": nss_data["metadata"]["namespace"],
                },
            },
        },
        constants.AZURE_PLATFORM: lambda: {
            "type": "azure-blob",
            "azureBlob": {
                "targetBlobContainer": uls_name,
                "secret": {
                    "name": get_attr_chain(cld_mgr, "azure_client.secret.name"),
                    "namespace": nss_data["metadata"]["namespace"],
                },
            },
        },
        constants.AZURE_WITH_LOGS_PLATFORM: lambda: {
            "type": "azure-blob",
            "azureBlob": {
                "targetBlobContainer": uls_name,
                "secret": {
                    "name": get_attr_chain(
                        cld_mgr, "azure_with_logs_client.secret.name"
                    ),
                    "namespace": nss_data["metadata"]["namespace"],
                },
            },
        },
        constants.RGW_PLATFORM: lambda: {
            "type": "s3-compatible",
            "s3Compatible": {
                "targetBucket": uls_name,
                "endpoint": get_attr_chain(cld_mgr, "rgw_client.endpoint"),
                "signatureVersion": "v2",
                "secret": {
                    "name": get_attr_chain(cld_mgr, "rgw_client.secret.name"),
                    "namespace": nss_data["metadata"]["namespace"],
                },
            },
        },
        constants.NAMESPACE_FILESYSTEM: lambda: {
            "type": "nsfs",
            "nsfs": {
                "pvcName": uls_name,
                "subPath": nss_tup[2] if nss_tup[2] else "",
            },
        },
    }

    if (
        platform.lower() == constants.NAMESPACE_FILESYSTEM
        and len(nss_tup) == 4
        and nss_tup[3]
    ):
        NSS_MAPPING[platform.lower()]["nsfs"]["fsBackend"] = nss_tup[3]

    nss_data["spec"] = NSS_MAPPING[platform.lower()]()
    create_resource(**nss_data)


def template_pvc(
    name,
    namespace=config.ENV_DATA["cluster_namespace"],
    storageclass=constants.CEPHFILESYSTEM_SC,
    access_mode=constants.ACCESS_MODE_RWX,
    size=20,
):
    """
    Create a PVC using the MCG CLI

    Args:
        name (str): Name of the PVC
        namespace (str): Namespace to create the PVC in
        access_mode (str): Access mode for the PVC
        size (str): Size of the PVC in GiB

    """
    pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
    pvc_data["metadata"]["name"] = name
    pvc_data["metadata"]["namespace"] = namespace
    pvc_data["spec"]["accessModes"] = [access_mode]
    pvc_data["spec"]["resources"]["requests"]["storage"] = f"{size}Gi"
    pvc_data["spec"]["storageClassName"] = (
        constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
        if storagecluster_independent_check()
        else storageclass
    )
    return pvc_data


def namespace_store_factory(
    request, cld_mgr, mcg_obj_session, cloud_uls_factory_session, pvc_factory_session
):
    """
    Create a NamespaceStore factory.
    Calling this fixture lets the user create namespace stores.

    Args:
        request (object): Pytest built-in fixture
        cld_mgr (CloudManager): Cloud Manager object containing all
            connections to clouds
        mcg_obj (MCG): MCG object containing data and utils
            related to MCG
        cloud_uls_factory: Factory for creation of underlying storage

    Returns:
        func: Factory method - allows the user to create namespace stores

    """
    created_nss = []

    cmdMap = {
        "cli": cli_create_namespacestore,
        "oc": oc_create_namespacestore,
    }

    def _create_nss(method, nss_dict):
        """
        Tracks creation and cleanup of all the namespace stores that were created in the current scope

        Args:
            method (str): String for selecting method of namespace store creation (CLI/OC)
            nss_dict (dict): Dictionary containing storage provider as key and a list of tuples
            as value.
            Namespace store dictionary examples - 'CloudName': [(amount, region), (amount, region)]
            i.e. - 'aws': [(3, us-west-1),(2, eu-west-2)]

        Returns:
            list: A list of the NamespaceStore objects created by the factory in the current scope

        """
        current_call_created_nss = []
        for platform, nss_lst in nss_dict.items():
            for nss_tup in nss_lst:
                for _ in range(nss_tup[0] if isinstance(nss_tup[0], int) else 1):
                    if platform.lower() == "nsfs":
                        uls_name = nss_tup[0] or create_unique_resource_name(
                            constants.PVC.lower(), platform
                        )
                        pvc_factory_session(
                            custom_data=template_pvc(uls_name, size=nss_tup[1])
                        )
                    else:
                        uls_name = list(
                            cloud_uls_factory_session({platform: [(1, nss_tup[1])]})[
                                platform
                            ]
                        )[0]
                    nss_name = create_unique_resource_name(constants.MCG_NSS, platform)
                    # Create the actual namespace resource
                    cmdMap[method.lower()](
                        nss_name, platform, mcg_obj_session, uls_name, cld_mgr, nss_tup
                    )
                    nss_obj = NamespaceStore(
                        name=nss_name,
                        method=method.lower(),
                        mcg_obj=mcg_obj_session,
                        uls_name=uls_name,
                    )
                    created_nss.append(nss_obj)
                    current_call_created_nss.append(nss_obj)
                    nss_obj.verify_health()
        return current_call_created_nss

    def nss_cleanup():
        for nss in created_nss:
            nss.delete()

    request.addfinalizer(nss_cleanup)

    return _create_nss
