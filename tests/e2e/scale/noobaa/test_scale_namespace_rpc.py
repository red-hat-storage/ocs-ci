import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import orange_squad
from ocs_ci.framework.testlib import (
    E2ETest,
    skipif_ocs_version,
    on_prem_platform_required,
    scale,
    mcg,
)
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)


@orange_squad
@mcg
@skipif_ocs_version("!=4.6")
@scale
class TestScaleNamespace(E2ETest):
    """
    Test creation of a namespace scale resource
    """

    @pytest.mark.parametrize(
        argnames=["platform"],
        argvalues=[
            pytest.param(
                constants.AWS_PLATFORM, marks=pytest.mark.polarion_id("OCS-2516")
            ),
            pytest.param(
                constants.AZURE_PLATFORM, marks=pytest.mark.polarion_id("OCS-2523")
            ),
        ],
    )
    def test_scale_namespace_bucket_creation(
        self, ns_resource_factory, bucket_factory, platform
    ):
        """
        Test namespace bucket creation using the MCG RPC.
        """
        # Create the namespace resource and verify health
        ns_resource_name = ns_resource_factory(platform=platform)[1]

        # Create the namespace buckets on top of the namespace resource
        bucket_factory(
            amount=100,
            interface="mcg-namespace",
            write_ns_resource=ns_resource_name,
            read_ns_resources=[ns_resource_name],
        )

    @pytest.mark.polarion_id("OCS-2517")
    @on_prem_platform_required
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
