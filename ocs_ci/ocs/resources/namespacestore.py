import logging
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.exceptions import UnsupportedPlatformError
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

    def delete(self):
        log.info(f"Cleaning up namespacestore {self.name}")

        if self.method == "oc":
            OCP(
                kind="namespacestore", namespace=config.ENV_DATA["cluster_namespace"]
            ).delete(resource_name=self.name)
        elif self.method == "cli":

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
                        return False

            sample = TimeoutSampler(
                timeout=120,
                sleep=20,
                func=_cli_deletion_flow,
            )
            if not sample.wait_for_func_status(result=True):
                log.error(f"Failed to {self.name}")
                raise TimeoutExpiredError

        log.info(f"Verifying whether namespacestore {self.name} exists after deletion")
        ns_deleted_successfully = False

        try:
            if self.method == "oc":
                OCP(
                    kind="namespacestore",
                    namespace=config.ENV_DATA["cluster_namespace"],
                    resource_name=self.name,
                ).get()
            elif self.method == "cli":
                self.mcg_obj.exec_mcg_cmd(f"namespacestore status {self.name}")

        except CommandFailed as e:
            if "Not Found" in str(e) or "NotFound" in str(e):
                ns_deleted_successfully = True
            else:
                raise

        assert (
            ns_deleted_successfully
        ), f"Namespacestore {self.name} was not deleted successfully"


def namespace_store_factory(request, cld_mgr, mcg_obj, cloud_uls_factory):
    """
    Create a namespace store factory. Calling this fixture creates a new namespace store.

    """
    created_nss = []

    cmdMap = {"oc": mcg_obj.create_namespace_store, "cli": ""}  # TODO

    endpointMap = {
        constants.AWS_PLATFORM: constants.MCG_NS_AWS_ENDPOINT,
        constants.AZURE_PLATFORM: constants.MCG_NS_AZURE_ENDPOINT,
        constants.MCG_NS_AZURE_ENDPOINT: RGW().get_credentials()[0],
    }

    def _create_nss(method, nss_dict):
        current_call_created_nss = []
        for platform, nss_lst in nss_dict.items():
            for nss_tup in nss_lst:
                # Create the actual namespace resource
                nss_name = create_unique_resource_name(constants.MCG_NSS, platform)

                target_bucket_name = cmdMap[method](
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
                    log.error(f"{nss_name} failed its verification check")
                    raise TimeoutExpiredError

                nss_obj = NamespaceStore(
                    name=nss_name,
                    method="oc",
                    mcg_obj=mcg_obj,
                )

                created_nss.append(nss_obj)
                current_call_created_nss.append(nss_obj)

        return current_call_created_nss

    def nss_cleanup():
        for nss in created_nss:
            try:
                nss.delete()
            except CommandFailed as e:
                if "not found" in str(e).lower():
                    log.warning(
                        f"Namespacestore {nss.name} could not be found in cleanup."
                        "\nSkipping deletion."
                    )
                else:
                    raise

    request.addfinalizer(nss_cleanup)

    return _create_nss
