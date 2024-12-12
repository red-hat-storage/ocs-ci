import logging

from ocs_ci.framework.testlib import (
    tier4a,
    skipif_ocs_version,
    BaseTest,
    post_upgrade,
    post_ocs_upgrade,
    polarion_id,
    skipif_external_mode,
)
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import green_squad

log = logging.getLogger(__name__)


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
        log.info("Testing mgr pods in the openshift-storage namespace.")
        mgr_pods = pod.get_mgr_pods()
        assert len(mgr_pods) == 2, "There should be 2 mgr pods"
        mgr_pods_names = set([pod.name for pod in mgr_pods])
        assert len(mgr_pods_names) == 2, "There should be two distinct mgr pod names"
        log.info(
            f"There are two different distinct mgr pods with names: {', '.join(mgr_pods_names)}"
        )

        log.info("Checking mgr metadata.")
        toolbox = pod.get_ceph_tools_pod()
        try:
            mgr_metadata = toolbox.exec_cmd_on_pod("ceph mgr metadata")
        except exceptions.CommandFailed:
            log.error("Unable to run command on toolbox")

        mgr_metadata_names = list()
        for data in mgr_metadata:
            if data.get("pod_name") not in mgr_pods_names:
                log.error(
                    f"Different pod name found: {data.get('pod_name')} "
                    f"not in mgr pod name list: {', '.join(mgr_pods_names)}"
                )
            mgr_metadata_names.append(
                {"name": data.get("name"), "pod_name": data.get("pod_name")}
            )
        assert (
            len(mgr_metadata_names) == 2
        ), f"The metadata contains more than 2 entries of mgr: {mgr_metadata_names}"
        assert (
            mgr_metadata_names[0]["name"] != mgr_metadata_names[1]["name"]
        ), f"The mgr metadata has two entries with same name: {mgr_metadata_names}"

        log.info(
            "Mgr metadata is correct with two distinct entries with two distinct names."
        )
        log.info(f"Name entries in mgr metadata: {mgr_metadata_names}")

    @green_squad
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
        log.info("Testing the mgr daemon stats")

        log.info("Checking mgr stat.")
        toolbox = pod.get_ceph_tools_pod()
        try:
            before_ceph_health = toolbox.exec_cmd_on_pod("ceph health")
            before_mgr_stat = toolbox.exec_cmd_on_pod("ceph mgr stat")
        except exceptions.CommandFailed:
            log.error("Unable to run command on toolbox")

        log.info(
            f"Currently mgr daemon {before_mgr_stat.get('active_name')} is set at "
            f"available: {before_mgr_stat.get('available')}"
        )
        log.info(f"Ceph health Status is at: {before_ceph_health}")

        log.info(f"Failing the active mgr dameon: {before_mgr_stat.get('active_name')}")
        try:
            toolbox.exec_cmd_on_pod(
                f"ceph mgr fail {before_mgr_stat.get('active_name')}"
            )
            after_ceph_health = toolbox.exec_cmd_on_pod("ceph health")
        except exceptions.CommandFailed:
            log.error("Unable to run command on toolbox")

        log.info(f"Ceph health status is at: {after_ceph_health}")
        log.info("Checking mgr stat again.")
        after_mgr_stat = toolbox.exec_cmd_on_pod("ceph mgr stat")
        log.info(
            f"Currently mgr daemon {after_mgr_stat.get('active_name')} is set at "
            f"available: {after_mgr_stat.get('available')}"
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
        log.info("Testing the mgr daemon stats")

        log.info("Checking mgr stat.")
        toolbox = pod.get_ceph_tools_pod()
        try:
            before_ceph_health = toolbox.exec_cmd_on_pod("ceph health")
            active_mgr_pod_output = toolbox.exec_cmd_on_pod("ceph mgr stat")
            active_mgr_pod_suffix = active_mgr_pod_output.get("active_name")
        except exceptions.CommandFailed:
            log.error("Unable to run command on toolbox")

        log.info(f"The active MGR pod is {active_mgr_pod_suffix}")
        log.info(f"Ceph health Status is at: {before_ceph_health}")

        log.info(f"Restarting mgr pod, rook-ceph-mgr-{active_mgr_pod_suffix}")
        mgr_pod = pod.get_mgr_pods()
        try:
            for index, pod_name in enumerate(mgr_pod):
                if f"rook-ceph-mgr-{active_mgr_pod_suffix}" in pod_name.name:
                    mgr_pod[index].delete(wait=True)
        except exceptions.CommandFailed:
            log.error("Unable to restart mgr pod")

        try:
            after_ceph_health = toolbox.exec_cmd_on_pod("ceph health")
        except exceptions.CommandFailed:
            log.error("Unable to run command on toolbox")
        log.info(f"Ceph health after reboot {after_ceph_health}")
