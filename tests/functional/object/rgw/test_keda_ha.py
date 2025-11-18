import pytest

from ocs_ci.framework.pytest_customization.marks import (
    skipif_disconnected_cluster,
    tier1,
)
from tests.conftest import install_helm_class


@skipif_disconnected_cluster
@pytest.mark.usefixtures(install_helm_class.__name__)
class TestKedaHA:
    """
    Test RGW's integration with Keda autoscaler for high availability
    """

    @tier1
    def test_rgw_keda_ha(self):
        """
        Test RGW's integration with Keda autoscaler for high availability
        """
        pass
