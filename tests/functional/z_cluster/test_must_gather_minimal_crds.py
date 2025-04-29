import logging
import pytest

from ocs_ci.framework.testlib import ManageTest
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    tier1,
    tier2,
    skipif_external_mode,
    skipif_ms_consumer,
    skipif_hci_client,
    stretchcluster_required_skipif,
)
from ocs_ci.ocs.must_gather.must_gather import MustGather
from ocs_ci.ocs.must_gather import const_must_gather

logger = logging.getLogger(__name__)


@brown_squad
class TestMustGather(ManageTest):
    @pytest.mark.parametrize(
        argnames=[
            "ceph",
            "ceph_logs",
            "namespaced",
            "clusterscoped",
            "noobaa",
            "dr",
            "node_selector",
            "host_network",
        ],
        argvalues=[
            pytest.param(
                *[False, False, False, False, False, False, False, False],
                marks=[
                    pytest.mark.polarion_id("OCS-6307"),
                    skipif_external_mode,
                    skipif_ms_consumer,
                    skipif_hci_client,
                    tier1,
                ],
            ),
            pytest.param(
                *[True, False, False, True, False, False, False, False],
                marks=[
                    pytest.mark.polarion_id("OCS-6312"),
                    skipif_external_mode,
                    skipif_ms_consumer,
                    skipif_hci_client,
                    tier2,
                ],
            ),
            pytest.param(
                *[False, True, False, False, False, False, False, False],
                marks=[
                    pytest.mark.polarion_id("OCS-6313"),
                    skipif_external_mode,
                    skipif_ms_consumer,
                    skipif_hci_client,
                    tier2,
                ],
            ),
            pytest.param(
                *[False, False, True, False, False, False, False, False],
                marks=[
                    pytest.mark.polarion_id("OCS-6314"),
                    skipif_external_mode,
                    skipif_ms_consumer,
                    skipif_hci_client,
                    tier2,
                ],
            ),
            pytest.param(
                *[False, False, False, True, False, False, False, False],
                marks=[
                    pytest.mark.polarion_id("OCS-6315"),
                    skipif_external_mode,
                    skipif_ms_consumer,
                    skipif_hci_client,
                    tier2,
                ],
            ),
            pytest.param(
                *[False, False, False, False, True, False, False, False],
                marks=[
                    pytest.mark.polarion_id("OCS-6311"),
                    skipif_external_mode,
                    skipif_ms_consumer,
                    skipif_hci_client,
                    tier2,
                ],
            ),
            pytest.param(
                *[False, False, False, False, False, True, False, False],
                marks=[
                    pytest.mark.polarion_id("OCS-6310"),
                    skipif_external_mode,
                    skipif_ms_consumer,
                    skipif_hci_client,
                    tier2,
                    stretchcluster_required_skipif,
                ],
            ),
            pytest.param(
                *[False, False, False, False, False, False, True, True],
                marks=[
                    pytest.mark.polarion_id("OCS-6514"),
                    skipif_external_mode,
                    skipif_ms_consumer,
                    skipif_hci_client,
                    tier2,
                ],
            ),
        ],
    )
    def test_must_gather_minimal_crd_modular(
        self,
        ceph,
        ceph_logs,
        namespaced,
        clusterscoped,
        noobaa,
        dr,
        node_selector,
        host_network,
    ):
        """
        Tests OCS must gather with --minimal-crd flag and modular flags

        Test Process:
        1.Collect mg with relevant flags for example
            oc adm must-gather --image=quay.io/rhceph-dev/ocs-must-gather:latest-4.15 -- /usr/bin/gather -c -cl -n
        2.Calculate the paths that should be included in the ocs mg dir
        3.Calculate the paths that should not be included in the ocs mg dir
        4.Verify paths exist in must gather directory
        5.Verify paths do not exist in must gather directory
        """
        mg_options = None
        if node_selector:
            mg_options = ' --node-selector="node-role.kubernetes.io/control-plane"'
        if host_network:
            mg_options = f"{mg_options} --host-network=true"
        flags_cmd = "/usr/bin/gather --minimal "
        paths_exist = list()
        paths_not_exist = list()
        paths_exist += const_must_gather.MINIMAL

        options = [
            (ceph, const_must_gather.CEPH_ONLY, "-c "),
            (ceph_logs, const_must_gather.CEPH_LOGS_ONLY, "-cl "),
            (namespaced, const_must_gather.NAMESPACED_ONLY, "-ns "),
            (clusterscoped, const_must_gather.CLUSTERSCOPED_ONLY, "-cs "),
            (noobaa, const_must_gather.NOOBAA_ONLY, "-n "),
            (dr, const_must_gather.DR_ONLY, "-d "),
        ]

        for flag, paths, param_value in options:
            if flag:
                paths_exist += paths
                flags_cmd += param_value
            else:
                paths_not_exist += paths
        mustgather_obj = MustGather()
        mustgather_obj.collect_must_gather(ocs_flags=flags_cmd, mg_options=mg_options)
        mustgather_obj.get_all_paths()
        folders_exist = mustgather_obj.verify_paths_in_dir(paths_exist)
        folders_not_exist = mustgather_obj.verify_paths_not_in_dir(paths_not_exist)
        assert len(folders_not_exist) + len(folders_exist) == 0, (
            f"\nMode: {flags_cmd}"
            f"\nThe folders don't exist [should exist]: {folders_exist} "
            f"\nThe folders exist [should not exist]: {folders_not_exist}"
        )
