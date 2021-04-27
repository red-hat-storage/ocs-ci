import logging
import pytest

from ocs_ci.framework.testlib import (
    MCGTest,
    skipif_ocs_version,
    vsphere_platform_required,
    scale,
)
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@scale
class TestScaleNamespace(MCGTest):
    """
    Test creation of a namespace scale resource
    """

    @skipif_ocs_version("<4.6")
    @pytest.mark.polarion_id("OCS-2516")
    def test_scale_namespace_bucket_creation(self, ns_resource_factory, bucket_factory):
        """
        Test namespace bucket creation using the MCG RPC.
        """
        # Create the namespace resource and verify health
        ns_resource_name = ns_resource_factory(platform=constants.AWS_PLATFORM)[1]

        # Create the namespace buckets on top of the namespace resource
        bucket_factory(
            amount=100,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name,
            read_ns_resources=[ns_resource_name],
        )

    @skipif_ocs_version("<4.6")
    @pytest.mark.polarion_id("OCS-2517")
    @vsphere_platform_required
    def test_scale_namespace_bucket_creation_with_rgw(
        self, ns_resource_factory, bucket_factory, rgw_deployments
    ):
        """
        Test namespace bucket creation using the MCG RPC.
        """
        # Create the namespace resource and verify health
        ns_resource_name = ns_resource_factory(platform=constants.RGW_PLATFORM)[1]

        # Create the namespace buckets on top of the namespace resource
        bucket_factory(
            amount=100,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name,
            read_ns_resources=[ns_resource_name],
        )
