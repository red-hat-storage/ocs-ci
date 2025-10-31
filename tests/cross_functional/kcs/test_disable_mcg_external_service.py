import pytest
import logging

from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.ocp import OCP
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

        # get noobaa object
        noobaa_ocp_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

        # get storagecluster object
        storagecluster_obj = OCP(
            kind="storagecluster",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="ocs-storagecluster",
        )

        # patch storagecluster object
        storagecluster_obj.patch(
            resource_name="ocs-storagecluster",
            params='{"spec":{ "multiCloudGateway": {"disableLoadBalancerService": true }}}',
            format_type="merge",
        )

        # scale up noobaa endpoints
        noobaa_ocp_obj.patch(
            resource_name="noobaa",
            params='{"spec": {"multiCloudGateway": {"endpoints": {"minCount": 2,"maxCount": 4}}}}',
            format_type="merge",
        )

        def finalizer():

            storagecluster_obj.patch(
                resource_name="ocs-storagecluster",
                params='{"spec":{ "multiCloudGateway": {"disableLoadBalancerService": false }}}',
                format_type="merge",
            )

            noobaa_ocp_obj.patch(
                resource_name="noobaa",
                params='{"spec": {"multiCloudGateway": {"endpoints": {"minCount": 1,"maxCount": 2}}}}',
                format_type="merge",
            )

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
        # verify disableLoadBalancerService is reconciled
        assert (
            str(patch_storagecluster_object.get()["spec"]["disableLoadBalancerService"])
            == "True"
        ), "disableLoadBalancerService is reconciled back to false"

        # verify that services are now Cluster IP
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
        assert sample.wait_for_func_status(
            result=True
        ), f"Services {services} isn't switched to ClusterIP service"
        logger.info(f"Services {services} switched to ClusterIP")
