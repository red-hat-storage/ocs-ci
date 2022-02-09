import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.rgw import RGW

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
            if self.name not in self.mcg_obj.exec_mcg_cmd("namespacestore list"):
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

    def verify_health(self, timeout=60, interval=5):
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


def namespace_store_factory(request, cld_mgr, mcg_obj, cloud_uls_factory):
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

    cmdMap = {"oc": mcg_obj.create_namespace_store, "cli": ""}  # TODO

    try:
        rgw_endpoint = RGW().get_credentials()[0]
    except CommandFailed:
        rgw_endpoint = None
    endpointMap = {
        constants.AWS_PLATFORM: constants.MCG_NS_AWS_ENDPOINT,
        constants.AZURE_PLATFORM: constants.MCG_NS_AZURE_ENDPOINT,
        constants.RGW_PLATFORM: rgw_endpoint,
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
                # Create the actual namespace resource
                nss_name = create_unique_resource_name(constants.MCG_NSS, platform)

                target_bucket_name = cmdMap[method.lower()](
                    nss_name, nss_tup[1], cld_mgr, cloud_uls_factory, platform
                )

                # TODO: Check platform exists in endpointMap

                sample = TimeoutSampler(
                    timeout=60,
                    sleep=5,
                    func=mcg_obj.check_ns_resource_validity,
                    ns_resource_name=nss_name,
                    target_bucket_name=target_bucket_name,
                    endpoint=endpointMap[platform],
                )
                if not sample.wait_for_func_status(result=True):
                    err_msg = f"{nss_name} failed its verification check"
                    log.error(err_msg)
                    raise TimeoutExpiredError(err_msg)

                nss_obj = NamespaceStore(
                    name=nss_name,
                    method=method.lower(),
                    mcg_obj=mcg_obj,
                    uls_name=target_bucket_name,
                )

                nss_obj.verify_health()

                created_nss.append(nss_obj)
                current_call_created_nss.append(nss_obj)

        return current_call_created_nss

    def nss_cleanup():
        for nss in created_nss:
            nss.delete()

    request.addfinalizer(nss_cleanup)

    return _create_nss
