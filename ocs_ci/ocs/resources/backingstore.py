import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed

from ocs_ci.ocs.bucket_utils import (
    oc_create_aws_backingstore,
    oc_create_google_backingstore,
    oc_create_azure_backingstore,
    oc_create_pv_backingstore,
    oc_create_ibmcos_backingstore,
    cli_create_google_backingstore,
    cli_create_azure_backingstore,
    cli_create_pv_backingstore,
    cli_create_ibmcos_backingstore,
    cli_create_aws_backingstore,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
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

    def delete(self):
        log.info(f"Cleaning up backingstore {self.name}")
        # If the backingstore utilizes a PV, save its PV name for deletion verification
        if self.type == "pv":
            backingstore_pvc = OCP(
                kind=constants.PVC, selector=f"pool={self.name}"
            ).get()["items"][0]
            pv_name = backingstore_pvc["spec"]["volumeName"]

        if self.method == "oc":
            OCP(
                kind="backingstore", namespace=config.ENV_DATA["cluster_namespace"]
            ).delete(resource_name=self.name)
        elif self.method == "cli":

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

            sample = TimeoutSampler(
                timeout=120,
                sleep=20,
                func=_cli_deletion_flow,
            )
            if not sample.wait_for_func_status(result=True):
                log.error(f"Failed to {self.name}")
                raise TimeoutExpiredError

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
                log.error(f"{self.name} was not deleted properly, leftovers were found")
                raise TimeoutExpiredError

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
            "pv": oc_create_pv_backingstore,
        },
        "cli": {
            "aws": cli_create_aws_backingstore,
            "gcp": cli_create_google_backingstore,
            "azure": cli_create_azure_backingstore,
            "ibmcos": cli_create_ibmcos_backingstore,
            "pv": cli_create_pv_backingstore,
        },
    }

    def _create_backingstore(method, uls_dict):
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

        Returns:
            list: A list of backingstore names.

        """
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
                    vol_num, size, storagecluster = uls_tup
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
                    created_backingstores.append(
                        BackingStore(
                            name=backingstore_name,
                            method=method.lower(),
                            type="pv",
                            mcg_obj=mcg_obj,
                            vol_num=vol_num,
                            vol_size=size,
                        )
                    )
                    if method.lower() == "cli":
                        cmdMap[method.lower()][cloud.lower()](
                            mcg_obj, backingstore_name, vol_num, size, storagecluster
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
                        created_backingstores.append(
                            BackingStore(
                                name=backingstore_name,
                                method=method.lower(),
                                type="cloud",
                                uls_name=uls_name,
                                mcg_obj=mcg_obj,
                            )
                        )
                        if method.lower() == "cli":
                            cmdMap[method.lower()][cloud.lower()](
                                mcg_obj, cld_mgr, backingstore_name, uls_name, region
                            )
                        elif method.lower() == "oc":
                            cmdMap[method.lower()][cloud.lower()](
                                cld_mgr, backingstore_name, uls_name, region
                            )
                        mcg_obj.check_backingstore_state(
                            backingstore_name, constants.BS_OPTIMAL
                        )
                        # TODO: Verify OC\CLI BS health by using the appropriate methods

        return created_backingstores

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
