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

    def _create_nss(platform=constants.AWS_PLATFORM):

        # Create the actual namespace resource
        rand_nss_name = create_unique_resource_name(constants.MCG_NSS, platform)
        if platform == constants.RGW_PLATFORM:
            region = None
        else:
            # TODO: fix this when https://github.com/red-hat-storage/ocs-ci/issues/3338
            # is resolved
            region = "us-east-2"

        target_bucket_name = mcg_obj.create_namespace_store(
            rand_nss_name,
            region,
            cld_mgr,
            cloud_uls_factory,
            platform,
        )

        log.info(f"Check validity of NS store {rand_nss_name}")
        if platform == constants.AWS_PLATFORM:
            endpoint = constants.MCG_NS_AWS_ENDPOINT
        elif platform == constants.AZURE_PLATFORM:
            endpoint = constants.MCG_NS_AZURE_ENDPOINT
        elif platform == constants.RGW_PLATFORM:
            rgw_conn = RGW()
            endpoint, _, _ = rgw_conn.get_credentials()
        else:
            raise UnsupportedPlatformError(f"Unsupported Platform: {platform}")

        sample = TimeoutSampler(
            timeout=60,
            sleep=5,
            func=mcg_obj.check_ns_resource_validity,
            ns_resource_name=rand_nss_name,
            target_bucket_name=target_bucket_name,
            endpoint=endpoint,
        )
        if not sample.wait_for_func_status(result=True):
            log.error(f"Failed to check validty for ${rand_nss_name}")
            raise TimeoutExpiredError

        created_nss.append(
            NamespaceStore(
                name=rand_nss_name,
                method="oc",
                mcg_obj=mcg_obj,
            )
        )
        return target_bucket_name, rand_nss_name

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
