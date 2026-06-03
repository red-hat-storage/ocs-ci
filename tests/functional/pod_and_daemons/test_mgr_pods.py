import logging

from ocs_ci.framework.testlib import (
    tier4a,
    skipif_ocs_version,
    BaseTest,
    post_upgrade,
    post_ocs_upgrade,
    polarion_id,
    skipif_external_mode,
    skipif_mcg_only,
)
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import green_squad

logger = logging.getLogger(__name__)


@tier4a
@skipif_ocs_version("<4.15")
@skipif_external_mode
@post_upgrade
@post_ocs_upgrade
class TestMgrPods(BaseTest):
    """
    This test class contains tests that would check mgr pods from odf-version 4.15
    1. Check if two MGR pods are deployed or not (mgr-a and mgr-b)
    2. Check that two MGR daemons are present
    3. Check for active MGR and try failing it to see active-standby MGR reaction.
    4. Check Active MGR pod reboot
    """

    @green_squad
    @polarion_id("OCS-5437")
    def test_two_mgr_pods_and_metadata(self):
        """
        Testing two mgr pods exists or not

        - Check if two mgr pods are deployed
            oc get pods | grep mgr
        - login to ceph-tools
            oc rsh <rook-ceph-tool-pod-name>
        - check mgr metadata
            ceph mgr metadata
        """
        logger.test_step("Verify two MGR pods are deployed")
        mgr_pods = pod.get_mgr_pods()
        logger.assertion(f"Number of MGR pods: expected=2, actual={len(mgr_pods)}")
        assert len(mgr_pods) == 2, "There should be 2 mgr pods"
        mgr_pods_names = set([pod.name for pod in mgr_pods])
        logger.assertion(
            f"Distinct MGR pod names count: expected=2, actual={len(mgr_pods_names)}"
        )
        assert len(mgr_pods_names) == 2, "There should be two distinct mgr pod names"
        logger.info(f"Found two distinct mgr pods: {', '.join(mgr_pods_names)}")

        logger.test_step("Check MGR metadata via ceph tools pod")
        toolbox = pod.get_ceph_tools_pod()
        try:
            mgr_metadata = toolbox.exec_cmd_on_pod("ceph mgr metadata")
        except exceptions.CommandFailed:
            logger.exception("Unable to run command on toolbox")

        mgr_metadata_names = list()
        for data in mgr_metadata:
            if data.get("pod_name") not in mgr_pods_names:
                logger.warning(
                    f"Different pod name found: {data.get('pod_name')} "
                    f"not in mgr pod name list: {', '.join(mgr_pods_names)}"
                )
            mgr_metadata_names.append(
                {"name": data.get("name"), "pod_name": data.get("pod_name")}
            )

        logger.test_step("Validate MGR metadata contains exactly 2 distinct entries")
        logger.assertion(
            f"MGR metadata entry count: expected=2, actual={len(mgr_metadata_names)}"
        )
        assert (
            len(mgr_metadata_names) == 2
        ), f"The metadata contains more than 2 entries of mgr: {mgr_metadata_names}"
        logger.assertion(
            f"MGR metadata names are distinct: "
            f"name[0]='{mgr_metadata_names[0]['name']}', name[1]='{mgr_metadata_names[1]['name']}'"
        )
        assert (
            mgr_metadata_names[0]["name"] != mgr_metadata_names[1]["name"]
        ), f"The mgr metadata has two entries with same name: {mgr_metadata_names}"

        logger.info(
            "Mgr metadata is correct with two distinct entries with two distinct names."
        )
        logger.debug(f"Name entries in mgr metadata: {mgr_metadata_names}")

    @green_squad
    @skipif_mcg_only
    @polarion_id("OCS-5438")
    def test_two_mgr_daemons_and_failure(self):
        """
        Testing two mgr pods exists or not

        - login to ceph-tools
            oc rsh <rook-ceph-tool-pod-name>
        - check mgr stat
            ceph mgr stat
        - fail mgr daemon
            ceph mgr fail <daemon name>
            ex. ceph mgr fail a
        - check mgr stat again and the passive(standby) should be active.
            ceph mgr stat
        """
        logger.test_step("Retrieve MGR daemon stats before failure")
        toolbox = pod.get_ceph_tools_pod()
        try:
            before_ceph_health = toolbox.exec_cmd_on_pod("ceph health")
            before_mgr_stat = toolbox.exec_cmd_on_pod("ceph mgr stat")
        except exceptions.CommandFailed:
            logger.exception("Unable to run command on toolbox")

        logger.info(
            f"Active MGR daemon: {before_mgr_stat.get('active_name')}, "
            f"available: {before_mgr_stat.get('available')}"
        )
        logger.debug(f"Ceph health before failure: {before_ceph_health}")

        logger.test_step(
            f"Fail the active MGR daemon: {before_mgr_stat.get('active_name')}"
        )
        try:
            toolbox.exec_cmd_on_pod(
                f"ceph mgr fail {before_mgr_stat.get('active_name')}"
            )
            after_ceph_health = toolbox.exec_cmd_on_pod("ceph health")
        except exceptions.CommandFailed:
            logger.exception("Unable to run command on toolbox")

        logger.debug(f"Ceph health after failure: {after_ceph_health}")

        logger.test_step("Verify standby MGR daemon became active after failure")
        after_mgr_stat = toolbox.exec_cmd_on_pod("ceph mgr stat")
        logger.info(
            f"Active MGR daemon after failure: {after_mgr_stat.get('active_name')}, "
            f"available: {after_mgr_stat.get('available')}"
        )

        logger.assertion(
            f"Active MGR changed after failure: "
            f"before='{before_mgr_stat.get('active_name')}', "
            f"after='{after_mgr_stat.get('active_name')}'"
        )
        assert before_mgr_stat.get("active_name") != after_mgr_stat.get(
            "active_name"
        ), (
            f"The mgr daemon before and after fail are the same: "
            f"before failure: {before_mgr_stat.get('active_name')}"
            f"after failure: {after_mgr_stat.get('active_name')}"
        )

    @polarion_id("OCS-5439")
    @green_squad
    @skipif_mcg_only
    def test_mgr_pod_reboot(self):
        """
        - Deoloy OCP and ODF
        - Check if two mgr pods are deployed
                oc get pods | grep mgr
        - Enable ceph tool and rsh to it
        - Check for the acitve mgr
                ceph mgr stat
        - reboot active pod
        - Enable ceph tool and rsh to it
        - Check for the acitve mgr
                ceph mgr stat
        """
        logger.test_step("Retrieve active MGR pod before reboot")
        toolbox = pod.get_ceph_tools_pod()
        try:
            before_ceph_health = toolbox.exec_cmd_on_pod("ceph health")
            active_mgr_pod_output = toolbox.exec_cmd_on_pod("ceph mgr stat")
            active_mgr_pod_suffix = active_mgr_pod_output.get("active_name")
        except exceptions.CommandFailed:
            logger.exception("Unable to run command on toolbox")

        logger.info(f"Active MGR pod: {active_mgr_pod_suffix}")
        logger.debug(f"Ceph health before reboot: {before_ceph_health}")

        logger.test_step(
            f"Restart active MGR pod: rook-ceph-mgr-{active_mgr_pod_suffix}"
        )
        mgr_pod = pod.get_mgr_pods()
        try:
            for index, pod_name in enumerate(mgr_pod):
                if f"rook-ceph-mgr-{active_mgr_pod_suffix}" in pod_name.name:
                    mgr_pod[index].delete(wait=True)
        except exceptions.CommandFailed:
            logger.exception("Unable to restart mgr pod")

        logger.test_step("Verify ceph health after MGR pod reboot")
        try:
            after_ceph_health = toolbox.exec_cmd_on_pod("ceph health")
        except exceptions.CommandFailed:
            logger.exception("Unable to run command on toolbox")
        logger.info(f"Ceph health after reboot: {after_ceph_health}")
