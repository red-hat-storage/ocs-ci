import pytest
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    polarion_id,
    bugzilla,
    skipif_external_mode,
)

logger = logging.getLogger(__name__)


class TestDisableMCGExternalService:
    @pytest.fixture()
    def patch_noobaa_object(self, request):

        # get noobaa object
        noobaa_ocp_obj = OCP(
            kind="noobaa",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name="noobaa",
        )

        # patch noobaa object
        noobaa_ocp_obj.patch(
            resource_name="noobaa",
            params='{"spec": {"disableLoadBalancerService": true }}',
            format_type="merge",
        )

        # scale up noobaa endpoints
        noobaa_ocp_obj.patch(
            resource_name="noobaa",
            params='{"spec": {"multiCloudGateway": {"endpoints": {"minCount": 2,"maxCount": 4}}}}',
            format_type="merge",
        )

        def finalizer():
            noobaa_ocp_obj.patch(
                resource_name="noobaa",
                params='{"spec": {"disableLoadBalancerService": false }}',
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
    @bugzilla("2186171")
    @polarion_id("OCS-4932")
    @skipif_external_mode
    def test_disable_mcg_external_service(self, patch_noobaa_object):
        """
        Test KCS https://access.redhat.com/articles/6970745
        Make sure disableLoadBalancerService is not reconciled and verify it works as expected
        """
        # verify disableLoadBalancerService is reconciled
        assert (
            str(patch_noobaa_object.get()["spec"]["disableLoadBalancerService"])
            == "True"
        ), "disableLoadBalancerService is reconciled back to false"

        # verify that services are now Cluster IP
        service_obj = OCP(
            kind="service", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        services = ["s3", "sts"]
        for svc in services:
            assert (
                str(service_obj.get(resource_name=svc)["spec"]["type"]) == "ClusterIP"
            ), f"Service {svc} isn't switched to ClusterIP service"
        logger.info(f"Services {services} switched to ClusterIP")
