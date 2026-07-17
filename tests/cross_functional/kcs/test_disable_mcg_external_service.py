import pytest
import logging

from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import wait_for_noobaa_db_ready
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    polarion_id,
    skipif_external_mode,
    magenta_squad,
)

logger = logging.getLogger(__name__)


@magenta_squad
class TestDisableMCGExternalService:
    @pytest.fixture()
    def patch_storagecluster_object(self, request):

        logger.test_step("Get NooBaa and StorageCluster objects")
        noobaa_ocp_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

        storagecluster_obj = OCP(
            kind="storagecluster",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        )

        logger.test_step("Patch StorageCluster to disable LoadBalancer service")
        storagecluster_obj.patch(
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
            params='{"spec":{ "multiCloudGateway": {"disableLoadBalancerService": true }}}',
            format_type="merge",
        )

        logger.test_step("Scale up NooBaa endpoints (min=2, max=4)")
        noobaa_ocp_obj.patch(
            resource_name="noobaa",
            params='{"spec": {"multiCloudGateway": {"endpoints": {"minCount": 2,"maxCount": 4}}}}',
            format_type="merge",
        )
        wait_for_noobaa_db_ready()
        logger.info("NooBaa DB is ready after endpoint scale-up")

        def finalizer():
            logger.test_step("Restore StorageCluster LoadBalancer service")
            storagecluster_obj.patch(
                resource_name=constants.DEFAULT_STORAGE_CLUSTER,
                params='{"spec":{ "multiCloudGateway": {"disableLoadBalancerService": false }}}',
                format_type="merge",
            )

            logger.test_step("Restore NooBaa endpoints to defaults (min=1, max=2)")
            noobaa_ocp_obj.patch(
                resource_name="noobaa",
                params='{"spec": {"multiCloudGateway": {"endpoints": {"minCount": 1,"maxCount": 2}}}}',
                format_type="merge",
            )
            wait_for_noobaa_db_ready()
            logger.info("NooBaa DB is ready after restoring defaults")

        request.addfinalizer(finalizer)
        return noobaa_ocp_obj

    @tier2
    @polarion_id("OCS-4932")
    @skipif_external_mode
    def test_disable_mcg_external_service(self, patch_storagecluster_object):
        """
        Test KCS https://access.redhat.com/articles/6970745
        Make sure disableLoadBalancerService is not reconciled and verify it works as expected
        """
        logger.test_step("Verify disableLoadBalancerService is reconciled")
        disable_lb = str(
            patch_storagecluster_object.get()["spec"]["disableLoadBalancerService"]
        )
        logger.assertion(
            f"disableLoadBalancerService: expected='True', actual='{disable_lb}'"
        )
        assert (
            disable_lb == "True"
        ), "disableLoadBalancerService is reconciled back to false"

        logger.test_step("Verify services are switched to ClusterIP")
        service_obj = OCP(
            kind="service", namespace=config.ENV_DATA["cluster_namespace"]
        )
        services = ["s3", "sts"]

        def check_svc_type():
            for svc in services:
                if (
                    str(service_obj.get(resource_name=svc)["spec"]["type"])
                    != "ClusterIP"
                ):
                    return False
            return True

        sample = TimeoutSampler(timeout=60, sleep=10, func=check_svc_type)
        svc_switched = sample.wait_for_func_status(result=True)
        logger.assertion(
            f"Services type check: services={services}, "
            f"expected_type='ClusterIP', all_switched={svc_switched}"
        )
        assert svc_switched, f"Services {services} isn't switched to ClusterIP service"
        logger.info(f"Services {services} switched to ClusterIP")
