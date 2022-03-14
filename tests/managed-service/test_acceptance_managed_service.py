import logging


from ocs_ci.framework.testlib import managed_service_required
from ocs_ci.ocs.perftests import PASTest

log = logging.getLogger(__name__)


@managed_service_required
class TestAcceptanceManagedService(PASTest):
    """
    Test Acceptance Managed Service

    """

    def test_acceptance_managed_service(self, workload_storageutilization_05p_rbd):
        """
        test_acceptance_managed_service

        """
        for cluster_name, fio_results in workload_storageutilization_05p_rbd.items():
            msg = "fio report should be available"
            assert fio_results["result"] is not None, f"cluster {cluster_name}-{msg}"
            fio = fio_results["result"]["fio"]
            assert len(fio["jobs"]) == 1, "single fio job was executed"
            msg = "no errors should be reported by fio when writing data"
            assert fio["jobs"][0]["error"] == 0, msg
