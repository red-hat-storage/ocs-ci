import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import bugzilla, skipif_ocs_version
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.quay_operator import QuayOperator

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def quay_operator(request):

    quay_operator = QuayOperator()

    def teardown():
        quay_operator.teardown()

    request.addfinalizer(teardown)
    return quay_operator


@workloads
class TestQuayWorkload(E2ETest):
    """
    Tests Quay operator
    """

    @bugzilla("1947796")
    @bugzilla("1959331")
    @bugzilla("1959333")
    @pytest.mark.polarion_id("OCS-2596")
    @skipif_ocs_version("<4.6")
    def test_quay(self, quay_operator, mcg_obj):
        """
        Test verifies quay operator deployment and
        whether single OB/OBC are created/bound.
        """
        # Deploy quay operator
        quay_operator.setup_quay_operator()

        # Create quay registry
        quay_operator.create_quay_registry()

        # Verify quay registry OBC is bound and only one bucket is created
        count = 0
        for bucket in mcg_obj.s3_resource.buckets.all():
            if bucket.name.startswith("quay-datastore"):
                count += 1
        assert count == 1, "More than one quay datastore buckets are created"
        assert (
            OCP(
                kind="obc",
                namespace=quay_operator.namespace,
                resource_name=f"{quay_operator.quay_registry_name}-quay-datastore",
            ).get()["status"]["phase"]
            == "Bound"
        )
