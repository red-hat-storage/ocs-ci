import logging

from ocs_ci.framework.testlib import (
    ManageTest,
    managed_service_required,
)
from ocs_ci.ocs import constants, managed_service


logger = logging.getLogger(__name__)


@managed_service_required
class TestAcceptanceManagedService(ManageTest):
    """
    Test Acceptance Managed Service

    """

    def test_acceptance_managed_service(
        self, pvc_factory, pod_factory, storageclass_factory, teardown_factory
    ):
        managed_service.rwo_dynamic_pvc(
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            interface_type=constants.CEPHFILESYSTEM,
            storageclass_factory=storageclass_factory,
            reclaim_policy=constants.RECLAIM_POLICY_DELETE,
            pvc_size=1,
        )

        managed_service.pvc_to_pvc_clone(
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            teardown_factory=teardown_factory,
            interface_type=constants.CEPHFILESYSTEM,
        )

        managed_service.pvc_snapshot(
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            teardown_factory=teardown_factory,
            interface=constants.CEPHFILESYSTEM,
        )
