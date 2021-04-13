import logging
import pytest
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.testlib import ManageTest, tier2

log = logging.getLogger(__name__)


@tier2
@pytest.mark.polarion_id("OCS-2481")
@pytest.mark.bugzilla("1859033")
class TestDeleteRookCephMonPod(ManageTest):
    """
    Tries to delete rook-ceph-operator pod.
    This operation creates a new pod 'rook-ceph-detect-version'
    Try to delete the 'rook-ceph-detect-version' pod while is created

    Note, this test performs the operations 5 times to get better odds
    since it's a race issue
    """

    def test_delete_rook_ceph_mon_pod(self):
        for i in range(5):
            rook_operator_pod = pod.get_ocs_operator_pod(
                ocs_label=constants.OPERATOR_LABEL,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            assert rook_operator_pod, "No rook operator pod found"
            log.info(f"Found rook-operator pod {rook_operator_pod.name}. Deleting it.")

            operator_deleted = rook_operator_pod.delete(wait=False)
            assert operator_deleted, f"Failed to delete pod {rook_operator_pod.name}"
            try:
                for pod_list in TimeoutSampler(
                    30,
                    1,
                    pod.get_pods_having_label,
                    constants.ROOK_CEPH_DETECT_VERSION_LABEL,
                    namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                ):
                    if len(pod_list) > 0:
                        self.rook_detect_pod_name = (
                            pod_list[0].get("metadata").get("name")
                        )
                        self.rook_detect_pod_obj = pod.get_pod_obj(
                            self.rook_detect_pod_name,
                            constants.OPENSHIFT_STORAGE_NAMESPACE,
                        )
                        break
            except TimeoutExpiredError:
                assert True, "rook-ceph-detect-version pod not found"

            log.info(
                f"Found rook-ceph-detect-version pod {self.rook_detect_pod_name}. Deleting it"
            )
            rook_detect_deleted = self.rook_detect_pod_obj.delete(wait=True)
            assert (
                rook_detect_deleted
            ), f"Failed to delete pod {self.rook_detect_pod_name}"
            self.rook_detect_pod_obj.ocp.wait_for_delete(self.rook_detect_pod_name)

        # Make sure there's no detect-version pod leftover
        try:
            for pod_list in TimeoutSampler(
                30,
                1,
                pod.get_pods_having_label,
                constants.ROOK_CEPH_DETECT_VERSION_LABEL,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            ):
                if len(pod_list) == 0:
                    break
                else:
                    log.info(
                        f"Pod {pod_list[0].get('metadata').get('name')} found. waiting for it to be deleted"
                    )
        except TimeoutExpiredError:
            assert True, "rook-ceph-detect-version pod still exists"
