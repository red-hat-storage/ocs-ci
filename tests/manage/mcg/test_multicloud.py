import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import MCGTest

logger = logging.getLogger(__name__)


class TestMultiRegion(MCGTest):
    """
    Test the multi region functionality
    """

    @tier1
    @pytest.mark.parametrize(
        argnames="backingstore_tup",
        argvalues=[
            pytest.param(("cli", {"aws": [(1, "eu-central-1")]})),
            pytest.param(("oc", {"aws": [(1, "eu-central-1")]})),
            pytest.param(("cli", {"azure": [(1, None)]})),
            pytest.param(("oc", {"azure": [(1, None)]})),
            pytest.param(("cli", {"gcp": [(1, None)]})),
            pytest.param(("oc", {"gcp": [(1, None)]})),
        ],
        # A test ID list for describing the parametrized tests
        # <CLOUD_PROVIDER>-<METHOD>-<AMOUNT-OF-BACKINGSTORES>
        ids=[
            "AWS-CLI-1",
            "AWS-OC-1",
            "AZURE-CLI-1",
            "AZURE-OC-1",
            "GCP-CLI-1",
            "GCP-OC-1",
        ],
    )
    def test_multiregion_backingstore_creation(
        self, backingstore_factory, backingstore_tup
    ):
        """
        Test MCG backingstore creation
        """

        backingstore_factory(*backingstore_tup)

    @tier1
    @pytest.mark.parametrize(
        argnames="backingstore_tup",
        argvalues=[
            pytest.param(("cli", {"aws": [(1, "eu-central-1")]})),
            pytest.param(("oc", {"aws": [(1, "eu-central-1")]})),
            pytest.param(("cli", {"azure": [(1, None)]})),
            pytest.param(("oc", {"azure": [(1, None)]})),
            pytest.param(("cli", {"gcp": [(1, None)]})),
            pytest.param(("oc", {"gcp": [(1, None)]})),
        ],
        # A test ID list for describing the parametrized tests
        # <CLOUD_PROVIDER>-<METHOD>-<AMOUNT-OF-BACKINGSTORES>
        ids=[
            "AWS-CLI-1",
            "AWS-OC-1",
            "AZURE-CLI-1",
            "AZURE-OC-1",
            "GCP-CLI-1",
            "GCP-OC-1",
        ],
    )
    def test_multiregion_backingstore_deletion(
        self, backingstore_factory, backingstore_tup
    ):
        """
        Test MCG backingstore deletion
        """

        for backingstore in backingstore_factory(*backingstore_tup):
            backingstore.delete()
