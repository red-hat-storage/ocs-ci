import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    brown_squad,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError, CommandFailed
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.testlib import ManageTest, tier2

log = logging.getLogger(__name__)


@brown_squad
@tier2
@skipif_external_mode
@pytest.mark.polarion_id("OCS-2481")
@pytest.mark.bugzilla("1859033")
class TestDeleteRookCephMonPod(ManageTest):
    """
    Tries to delete rook-ceph-operator pod.
    This operation creates a new pod 'rook-ceph-detect-version'
    Try to delete the 'rook-ceph-detect-version' pod while is created

    Note, this test performs the operations 10 times to get better odds
    since it's a race issue
    """

    num_of_deletions = 0

    def test_delete_rook_ceph_mon_pod(self):
        for i in range(30):
            self.rook_detect_pod_name = None
            rook_operator_pod = pod.get_ocs_operator_pod(
                ocs_label=constants.OPERATOR_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
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
                    namespace=config.ENV_DATA["cluster_namespace"],
                ):
                    if len(pod_list) > 0:
                        self.rook_detect_pod_name = (
                            pod_list[0].get("metadata").get("name")
                        )
                        rook_detect_pod_list = pod.get_pod_objs(
                            pod_names=[self.rook_detect_pod_name],
                            namespace=config.ENV_DATA["cluster_namespace"],
                        )
                        if len(rook_detect_pod_list) > 0:
                            log.info(
                                f"Found rook-ceph-detect-version pod {self.rook_detect_pod_name}. Deleting it"
                            )
                            self.rook_detect_pod_obj = rook_detect_pod_list[0]
                            rook_detect_deleted = False
                            try:
                                rook_detect_deleted = self.rook_detect_pod_obj.delete(
                                    wait=True
                                )
                            except CommandFailed:
                                log.warning(
                                    f"{self.rook_detect_pod_name} pod not found"
                                )
                            else:
                                log.info(f"Deletion status: {rook_detect_deleted}")
                                assert (
                                    rook_detect_deleted
                                ), f"Failed to delete pod {self.rook_detect_pod_name}"
                                self.rook_detect_pod_obj.ocp.wait_for_delete(
                                    self.rook_detect_pod_name
                                )
                                self.num_of_deletions += 1
            except TimeoutExpiredError:
                log.warning("rook-ceph-detect-version pod not found")

        # Make sure there's no detect-version pod leftover
        try:
            for pod_list in TimeoutSampler(
                60,
                1,
                pod.get_pods_having_label,
                constants.ROOK_CEPH_DETECT_VERSION_LABEL,
                namespace=config.ENV_DATA["cluster_namespace"],
            ):
                if len(pod_list) == 0:
                    break
                else:
                    log.info(
                        f"Pod {pod_list[0].get('metadata').get('name')} found. waiting for it to be deleted"
                    )
        except TimeoutExpiredError:
            assert True, "rook-ceph-detect-version pod still exists"
        log.info(f"Num of deletions: {self.num_of_deletions}/30")
        assert (
            self.num_of_deletions > 0
        ), "All (20) attempts to delete rook-ceph-detect-version pod failed."
