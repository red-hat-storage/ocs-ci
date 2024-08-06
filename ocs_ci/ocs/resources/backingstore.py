import logging

from ocs_ci.ocs import constants

from ocs_ci.ocs.bucket_utils import (
    oc_create_aws_backingstore,
    oc_create_google_backingstore,
    oc_create_azure_backingstore,
    oc_create_pv_backingstore,
    oc_create_ibmcos_backingstore,
    oc_create_rgw_backingstore,
    cli_create_google_backingstore,
    cli_create_azure_backingstore,
    cli_create_pv_backingstore,
    cli_create_ibmcos_backingstore,
    cli_create_aws_backingstore,
    cli_create_rgw_backingstore,
    cli_create_aws_sts_backingstore,
)
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    ObjectsStillBeingDeletedException,
    CommandFailed,
    UnavailableResourceException,
    UnknownCloneTypeException,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    storagecluster_independent_check,
)
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class BackingStore:
    """
    A class that represents BackingStore objects

    """

    def __init__(
        self,
        name,
        method,
        type,
        uls_name=None,
        secret_name=None,
        mcg_obj=None,
        vol_num=None,
        vol_size=None,
    ):
        self.name = name
        self.method = method
        self.type = type
        self.uls_name = uls_name
        self.secret_name = secret_name
        self.mcg_obj = mcg_obj
        self.vol_num = vol_num
        self.vol_size = vol_size

    def delete(self, retry=True, timeout=120):
        """
        Deletes the current backingstore by using OC/CLI commands

        Args:
            retry (bool): Whether to retry the deletion if it fails
            timeout (int): Timeout to wait if retry is true
        """

        log.info(f"Cleaning up backingstore {self.name}")
        # If the backingstore utilizes a PV, save its PV name for deletion verification
        if self.type == "pv":
            try:
                backingstore_pvc = OCP(
                    kind=constants.PVC,
                    selector=f"pool={self.name}",
                    namespace=config.ENV_DATA["cluster_namespace"],
                ).get()["items"][0]
            except IndexError:
                log.error(
                    f"Could not find the OCP object for {self.name}, proceeding without removal"
                )
                return True
            except Exception as e:
                raise e
            pv_name = backingstore_pvc["spec"]["volumeName"]

        def _oc_deletion_flow():
            try:
                OCP(
                    kind=constants.BACKINGSTORE,
                    namespace=config.ENV_DATA["cluster_namespace"],
                ).delete(resource_name=self.name)
                return True
            except CommandFailed as e:
                if "not found" in e.args[0].lower():
                    log.warning(f"Backingstore {self.name} was already deleted.")
                    return True
                elif all(
                    err in e.args[0]
                    for err in [
                        "cannot complete because objects in Backingstore",
                        "are still being deleted, Please try later",
                    ]
                ) or all(
                    err in e.args[0]
                    for err in [
                        "cannot complete because pool",
                        'in "CONNECTED_BUCKET_DELETING" state',
                    ]
                ):
                    log.error(
                        "Backingstore deletion failed because the objects are still getting deleted; Retrying"
                    )
                    raise ObjectsStillBeingDeletedException
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
                self.mcg_obj.exec_mcg_cmd(f"backingstore delete {self.name}")
                return True
            except CommandFailed as e:
                if "being used by one or more buckets" in str(e).lower():
                    log.warning(
                        f"Deletion of {self.name} failed because it's being used by a bucket. "
                        "Retrying..."
                    )
                    return False

        cmdMap = {
            "oc": _oc_deletion_flow,
            "cli": _cli_deletion_flow,
        }

        if retry:
            # The first attempt to delete will determine if we need to increase the timeout
            try:
                cmdMap[self.method]()
            except ObjectsStillBeingDeletedException:
                timeout = 19800
            except CommandFailed:
                pass

            sample = TimeoutSampler(
                timeout=timeout,
                sleep=20,
                func=cmdMap[self.method],
            )
            if not sample.wait_for_func_status(result=True):
                err_msg = f"Failed to delete {self.name}"
                log.error(err_msg)
                raise TimeoutExpiredError(err_msg)
        else:
            cmdMap[self.method]()

        # Verify deletion was successful
        log.info(f"Verifying whether backingstore {self.name} exists after deletion")
        bs_deleted_successfully = False

        try:
            if self.method == "oc":
                OCP(
                    kind="backingstore",
                    namespace=config.ENV_DATA["cluster_namespace"],
                    resource_name=self.name,
                ).get()
            elif self.method == "cli":
                self.mcg_obj.exec_mcg_cmd(f"backingstore status {self.name}")

        except CommandFailed as e:
            if "Not Found" in str(e) or "NotFound" in str(e):
                bs_deleted_successfully = True
            else:
                raise

        assert (
            bs_deleted_successfully
        ), f"Backingstore {self.name} was not deleted successfully"

        def _wait_for_pv_backingstore_resource_deleted(namespace=None):
            """
            wait for pv backing store resources to be deleted at the end of test teardown

            Args:
                backingstore_name (str): backingstore name
                namespace (str): backing store's namespace

            """
            namespace = namespace or config.ENV_DATA["cluster_namespace"]
            sample = TimeoutSampler(
                timeout=120,
                sleep=15,
                func=_check_resources_deleted,
                namespace=namespace,
            )
            if not sample.wait_for_func_status(result=True):
                err_msg = f"{self.name} was not deleted properly, leftovers were found"
                log.error(err_msg)
                raise TimeoutExpiredError(err_msg)

        def _check_resources_deleted(namespace=None):
            """
            check if resources of the pv pool backingstore deleted properly

            Args:
                namespace (str): backing store's namespace

            Returns:
                bool: True if pvc(s) were deleted

            """
            try:
                OCP(kind=constants.PV, resource_name=pv_name).get()
                log.warning(f"Found PV leftovers belonging to {self.name}")
                return False
            except CommandFailed as e:
                if "not found" in str(e):
                    pass
                else:
                    raise
            pvcs = get_all_pvcs(namespace=namespace, selector=f"pool={self.name}")
            pods = get_pods_having_label(namespace=namespace, label=f"pool={self.name}")
            return len(pvcs["items"]) == 0 and len(pods) == 0

        if self.type == "pv":
            log.info(f"Waiting for backingstore {self.name} resources to be deleted")
            _wait_for_pv_backingstore_resource_deleted()


def backingstore_factory(request, cld_mgr, mcg_obj, cloud_uls_factory):
    """
    Create a Backing Store factory.
    Calling this fixture creates a new Backing Store(s).

    Args:
        request (object): Pytest built-in fixture
        cld_mgr (CloudManager): Cloud Manager object containing all
            connections to clouds
        mcg_obj (MCG): MCG object containing data and utils
            related to MCG
        cloud_uls_factory: Factory for underlying storage creation

    Returns:
        func: Factory method - each call to this function creates
            a backingstore

    """
    created_backingstores = []

    cmdMap = {
        "oc": {
            "aws": oc_create_aws_backingstore,
            "gcp": oc_create_google_backingstore,
            "azure": oc_create_azure_backingstore,
            "ibmcos": oc_create_ibmcos_backingstore,
            "rgw": oc_create_rgw_backingstore,
            "pv": oc_create_pv_backingstore,
        },
        "cli": {
            "aws": cli_create_aws_backingstore,
            "gcp": cli_create_google_backingstore,
            "azure": cli_create_azure_backingstore,
            "ibmcos": cli_create_ibmcos_backingstore,
            "rgw": cli_create_rgw_backingstore,
            "pv": cli_create_pv_backingstore,
            "aws-sts": cli_create_aws_sts_backingstore,
        },
    }

    def _create_backingstore(method, uls_dict, timeout=600):
        """
        Tracks creation and cleanup of all the backing stores that were created in the scope

        Args:
            method (str): String for selecting method of backing store creation (CLI/OC)
            uls_dict (dict): Dictionary containing storage provider as key and a list of tuples
            as value.
            Cloud backing stores form - 'CloudName': [(amount, region), (amount, region)]
            i.e. - 'aws': [(3, us-west-1),(2, eu-west-2)]
            PV form - 'pv': [(amount, size_in_gb, storagecluster), ...]
            i.e. - 'pv': [(3, 32, ocs-storagecluster-ceph-rbd),(2, 100, ocs-storagecluster-ceph-rbd)]
            timeout (int): Timeout until backingstore reaches desired state
        Returns:
            list: A list of backingstore names.

        """
        current_call_created_backingstores = []
        if method.lower() not in cmdMap:
            raise RuntimeError(
                f"Invalid method type received: {method}. "
                f'available types: {", ".join(cmdMap.keys())}'
            )
        for cloud, uls_lst in uls_dict.items():
            for uls_tup in uls_lst:
                # Todo: Replace multiple .append calls, create names in advance, according to amountoc
                if cloud.lower() not in cmdMap[method.lower()]:
                    raise RuntimeError(
                        f"Invalid cloud type received: {cloud}. "
                        f'available types: {", ".join(cmdMap[method.lower()].keys())}'
                    )
                if cloud == "pv":
                    if len(uls_tup) == 3:
                        vol_num, size, storagecluster = uls_tup
                        req_cpu, req_mem, lim_cpu, lim_mem = (None, None, None, None)
                    else:
                        (
                            vol_num,
                            size,
                            storagecluster,
                            req_cpu,
                            req_mem,
                            lim_cpu,
                            lim_mem,
                        ) = uls_tup
                    if (
                        storagecluster == constants.DEFAULT_STORAGECLASS_RBD
                        and storagecluster_independent_check()
                    ):
                        storagecluster = (
                            constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
                        )
                    backingstore_name = create_unique_resource_name(
                        resource_description="backingstore", resource_type=cloud.lower()
                    )
                    backingstore_obj = BackingStore(
                        name=backingstore_name,
                        method=method.lower(),
                        type="pv",
                        mcg_obj=mcg_obj,
                        vol_num=vol_num,
                        vol_size=size,
                    )
                    current_call_created_backingstores.append(backingstore_obj)
                    created_backingstores.append(backingstore_obj)
                    if method.lower() == "cli":
                        cmdMap[method.lower()][cloud.lower()](
                            mcg_obj,
                            backingstore_name,
                            vol_num,
                            size,
                            storagecluster,
                            req_cpu=req_cpu,
                            req_mem=req_mem,
                            lim_cpu=lim_cpu,
                            lim_mem=lim_mem,
                        )
                    else:
                        cmdMap[method.lower()][cloud.lower()](
                            backingstore_name, vol_num, size, storagecluster
                        )
                else:
                    _, region = uls_tup
                    uls_dict = cloud_uls_factory({cloud: [uls_tup]})
                    for uls_name in uls_dict[cloud.lower()]:
                        backingstore_name = create_unique_resource_name(
                            resource_description="backingstore",
                            resource_type=cloud.lower(),
                        )
                        backingstore_obj = BackingStore(
                            name=backingstore_name,
                            method=method.lower(),
                            type="cloud",
                            uls_name=uls_name,
                            mcg_obj=mcg_obj,
                        )
                        current_call_created_backingstores.append(backingstore_obj)
                        created_backingstores.append(backingstore_obj)
                        if method.lower() == "cli":
                            cmdMap[method.lower()][cloud.lower()](
                                mcg_obj, cld_mgr, backingstore_name, uls_name, region
                            )
                        elif method.lower() == "oc":
                            cmdMap[method.lower()][cloud.lower()](
                                cld_mgr, backingstore_name, uls_name, region
                            )
                        mcg_obj.check_backingstore_state(
                            backingstore_name, constants.BS_OPTIMAL, timeout=timeout
                        )
                        # TODO: Verify OC\CLI BS health by using the appropriate methods

        return current_call_created_backingstores

    def backingstore_cleanup():
        for backingstore in created_backingstores:
            try:
                backingstore.delete()
            except CommandFailed as e:
                if "not found" in str(e).lower():
                    log.warning(
                        f"Backingstore {backingstore.name} could not be found in cleanup."
                        "\nSkipping deletion."
                    )
                else:
                    raise

    request.addfinalizer(backingstore_cleanup)

    return _create_backingstore


def clone_bs_dict_from_backingstore(
    protype_backingstore_name,
    namespace=None,
):
    """
    Create a backingstore of the same kind and specs as an existing backingstore.

    Args:
        protype_backingstore_name (str): Name of the existing backingstore to clone
        backingstore_factory (function): an backingstore factory instance
        mcg_obj (MCG): MCG object containing data and utils related to MCG
        method(str): Method to use for creating the backingstore (oc or cli)
        namespace (str): Namespace of the backingstore to clone

    Raises:
        UnavailableResourceException: If the backingstore to clone does not exist
        UnaknownCloneTypeException: If the prototype backingstore is of an unknown type

    Returns:
        clone_bs_dict (dict): A dictionary containing the specs needed to create a copy of the
        prototype backingstore

    """
    if not namespace:
        namespace = config.ENV_DATA["cluster_namespace"]

    # Validate the prototype backingstore exists
    protoype_backingstore = OCP(
        kind="backingstore",
        namespace=namespace,
        resource_name=protype_backingstore_name,
    )
    if not protoype_backingstore.data:
        raise UnavailableResourceException(
            f"Backingstore {protype_backingstore_name} does not exist"
        )

    # Determine from the prototype the kind and specs of the new backingstore
    prototype_bs_platform_name = protoype_backingstore.data["spec"]["type"]
    clone_bs_dict = {}

    if prototype_bs_platform_name == constants.BACKINGSTORE_TYPE_AWS:
        target_region = protoype_backingstore.data["spec"]["awsS3"]["region"]
        clone_bs_dict = {"aws": [(1, target_region)]}

    elif prototype_bs_platform_name == constants.BACKINGSTORE_TYPE_AZURE:
        clone_bs_dict = {"azure": [(1, None)]}

    elif prototype_bs_platform_name == constants.BACKINGSTORE_TYPE_GOOGLE:
        clone_bs_dict = {"gcp": [(1, None)]}

    elif prototype_bs_platform_name == constants.BACKINGSTORE_TYPE_IBMCOS:
        clone_bs_dict = {"ibmcos": [(1, None)]}

    elif prototype_bs_platform_name == constants.BACKINGSTORE_TYPE_S3_COMP:
        clone_bs_dict = {"rgw": [(1, None)]}

    elif prototype_bs_platform_name == constants.BACKINGSTORE_TYPE_PV_POOL:
        pvpool_storageclass = (
            constants.THIN_CSI_STORAGECLASS
            if config.ENV_DATA["mcg_only_deployment"]
            else constants.DEFAULT_STORAGECLASS_RBD
        )
        prototype_pvpool_data = protoype_backingstore.data["spec"]["pvPool"]
        num_volumes = prototype_pvpool_data["numVolumes"]
        size_str = prototype_pvpool_data["resources"]["requests"]["storage"]
        prototype_pv_size = int(size_str[:-2])  # Remove the 'Gi' suffix
        clone_pv_size = max(constants.MIN_PV_BACKINGSTORE_SIZE_IN_GB, prototype_pv_size)
        clone_bs_dict = {"pv": [(num_volumes, clone_pv_size, pvpool_storageclass)]}

    else:
        raise UnknownCloneTypeException(prototype_bs_platform_name)

    return clone_bs_dict
