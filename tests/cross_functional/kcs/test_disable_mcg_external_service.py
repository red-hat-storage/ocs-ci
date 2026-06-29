import pytest
import logging

from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
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
        logger.info("Getting NooBaa object")
        noobaa_ocp_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

        logger.info("Getting StorageCluster object")
        storagecluster_obj = OCP(
            kind="storagecluster",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
        )

        logger.info(
            f"Patching StorageCluster {constants.DEFAULT_STORAGE_CLUSTER} to disable LoadBalancer service"
        )
        storagecluster_obj.patch(
            resource_name=constants.DEFAULT_STORAGE_CLUSTER,
            params='{"spec":{ "multiCloudGateway": {"disableLoadBalancerService": true }}}',
            format_type="merge",
        )
        logger.info("StorageCluster patched: disableLoadBalancerService=true")

        logger.info("Scaling up NooBaa endpoints: minCount=2, maxCount=4")
        noobaa_ocp_obj.patch(
            resource_name="noobaa",
            params='{"spec": {"multiCloudGateway": {"endpoints": {"minCount": 2,"maxCount": 4}}}}',
            format_type="merge",
        )
        logger.info("NooBaa endpoints scaled up successfully")

        def finalizer():
            logger.info(
                "Teardown: Reverting StorageCluster to default LoadBalancer configuration"
            )
            storagecluster_obj.patch(
                resource_name=constants.DEFAULT_STORAGE_CLUSTER,
                params='{"spec":{ "multiCloudGateway": {"disableLoadBalancerService": false }}}',
                format_type="merge",
            )

            logger.info(
                "Teardown: Reverting NooBaa endpoints to default: minCount=1, maxCount=2"
            )
            noobaa_ocp_obj.patch(
                resource_name="noobaa",
                params='{"spec": {"multiCloudGateway": {"endpoints": {"minCount": 1,"maxCount": 2}}}}',
                format_type="merge",
            )
            logger.info("Teardown completed: All configurations reverted")

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
        logger.test_step(
            "Verify disableLoadBalancerService configuration is not reconciled"
        )
        noobaa_spec = patch_storagecluster_object.get()["spec"]
        actual_value = str(noobaa_spec["disableLoadBalancerService"])
        logger.assertion(
            f"Verify disableLoadBalancerService setting: expected=True, actual={actual_value}"
        )
        assert (
            actual_value == "True"
        ), "disableLoadBalancerService is reconciled back to false"
        logger.info(
            "disableLoadBalancerService configuration preserved (not reconciled)"
        )

        logger.test_step("Verify MCG services switched to ClusterIP type")
        service_obj = OCP(
            kind="service", namespace=config.ENV_DATA["cluster_namespace"]
        )
        services = ["s3", "sts"]
        logger.info(f"Checking service types for: {services}")

        def check_svc_type():
            for svc in services:
                svc_type = str(service_obj.get(resource_name=svc)["spec"]["type"])
                logger.debug(f"Service {svc} type: {svc_type}")
                if svc_type != "ClusterIP":
                    return False
            return True

        logger.info(
            f"Waiting up to 60s for services {services} to switch to ClusterIP type"
        )
        sample = TimeoutSampler(timeout=60, sleep=10, func=check_svc_type)
        logger.assertion(f"Verify services switched to ClusterIP: services={services}")
        assert sample.wait_for_func_status(
            result=True
        ), f"Services {services} isn't switched to ClusterIP service"
        logger.info(f"Services {services} successfully switched to ClusterIP type")
