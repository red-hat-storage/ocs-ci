import logging
import pytest
import yaml
import random
import time
import tempfile
from datetime import datetime

from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import verify_log_exist_in_pods_logs
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.exceptions import PodNotCreated, CommandFailed
from ocs_ci.ocs.resources import pod as res_pod
from ocs_ci.ocs.resources.pod import get_plugin_pods, get_pod_node
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    magenta_squad,
)

log = logging.getLogger(__name__)


@magenta_squad
@tier1
class TestSelinuxrelabel(E2ETest):
    def create_deploymentconfig_pod(self, pvc_name=None, **kwargs):
        """
        Create deployment pod.

        Returns:
            object: helpers.create_pod instance

        """
        if pvc_name is None:
            pvc_name = self.pvc_obj.name
        try:
            pod_obj = helpers.create_pod(
                interface_type=constants.CEPHFS_INTERFACE,
                pvc_name=pvc_name,
                namespace=self.ocp_project.namespace,
                sa_name=self.service_account_obj.name,
                pod_dict_path=constants.PERF_POD_YAML,
                **kwargs,
            )
        except Exception as e:
            log.exception(
                f"Pod attached to PVC {pod_obj.name} was not created, exception [{str(e)}]"
            )
            raise PodNotCreated("Pod attached to PVC was not created.")
        return pod_obj

    def get_app_pod_obj(self):
        """
        Get cephfs app pod

        Returns:
            object: app pod instance

        """
        pod_obj_list = res_pod.get_all_pods(
            selector=[self.pod_selector],
            selector_label=constants.DEPLOYMENTCONFIG,
        )
        pod_name = self.pod_selector + "-1-deploy"
        for pod_obj in pod_obj_list:
            if pod_name not in pod_obj.name:
                return pod_obj

    def get_pod_start_time(self, pod_name):
        """
        Get the time required for pod to come in a running state

        Args:
            pod_name (str): App pod name to look for.

        Returns:
            datetime: Time required for pod restart.

        """
        try:
            # Get the pod conditions
            pod = ocp.OCP(kind="pod")
            conditions = pod.exec_oc_cmd(
                f"get pod {pod_name} -n {self.ocp_project.namespace} -o jsonpath='{{.status.conditions}}'"
            )
            conditions = [
                {key: None if value == "null" else value for key, value in item.items()}
                for item in conditions
            ]

            # Get lastTransitionTime for different condition type of pod
            containers_ready_time = None
            pod_scheduled_time = None
            for condition in conditions:
                if condition["type"] == "ContainersReady":
                    containers_ready_time = datetime.strptime(
                        condition["lastTransitionTime"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                elif condition["type"] == "PodScheduled":
                    pod_scheduled_time = datetime.strptime(
                        condition["lastTransitionTime"], "%Y-%m-%dT%H:%M:%SZ"
                    )

            # Calculate the difference between ContainersReady and PodScheduled time
            if containers_ready_time and pod_scheduled_time:
                time_difference = containers_ready_time - pod_scheduled_time
                return time_difference.total_seconds()

        except CommandFailed as exc:
            log.exception(f"Error retrieving pod information for '{pod_name}': {exc}")

        return None

    def get_random_files(self, pod_obj):
        """
        Get random files list.

        Args:
            pod_obj (Pod object): App pod

        Returns:
            list : list of random files

        """
        # Get random files
        data_path = f"{constants.FLEXY_MNT_CONTAINER_DIR}"
        num_of_files = random.randint(3, 9)
        ocp_obj = ocp.OCP(kind=self.ocp_project.namespace)
        random_files = ocp_obj.exec_oc_cmd(
            f"exec -n {self.ocp_project.namespace} -it {pod_obj.name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n {num_of_files}"',
            timeout=300,
        )
        random_files = random_files.split()
        log.info(f"files are {random_files}")
        return random_files

    def get_csi_cephfs_pod_on_same_node(self, pod_obj):
        """
        Get csi-cephfs-pod on same node as rwop pod

        Args:
            pod_obj (Pod object): rwop pod
        """
        node = get_pod_node(pod_obj)
        plugin_pods = get_plugin_pods(interface=constants.CEPHFILESYSTEM)
        for pod in plugin_pods:
            plugin_pod_node = get_pod_node(pod)
            if plugin_pod_node.name == node.name:
                return pod.name

    def backup_pod(self, pod_obj):
        """
        Backup and recreate pod.

        Args:
            pvc_obj(PVC object): ocs_ci.ocs.resources.pvc.PVC instance kind.

        """
        # Backup existing PV
        log.info("Getting backup of existing pod")
        backup_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="test_", suffix=".yaml", delete=False
        )
        backup_file = backup_file.name
        backup_get = pod_obj.get()
        dump_data_to_temp_yaml(backup_get, backup_file)
        log.info(f"{backup_file} file for pod is created")

        # Edit backup PV yaml
        yaml.safe_load(backup_file)
        with open(backup_file, "r+") as backup:
            backup1 = yaml.safe_load(backup)
            backup1["spec"]["securityContext"]["seLinuxOptions"]["level"] = "s0:c27,c20"
        with open(backup_file, "w") as backup:
            yaml.dump(backup1, backup)
            log.info(f"Pod {backup_file} file is updated")

        pod_obj.delete(force=True)
        log.info(f"Pod {pod_obj.name} deleted")

        # Recreate PV from backup file
        run_cmd(f"oc apply -f {backup_file}")
        helpers.wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=600)
        # Leave pod for some time to run since file creation time is longer
        waiting_time = 10
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(waiting_time)
        log.info(f"Backup PV {pod_obj} created")

    @pytest.mark.parametrize("copies", [5])
    def test_selinux_relabel_on_rwop(
        self, pvc_factory, service_account_factory, copies
    ):
        """
        Steps:
            1. Create cephfs pvcs and attach pod with more than 100K files across multiple nested directories
            2. Take md5sum for them some random files and get pod restart time
            3. Apply the fix for SeLinux-relabeling
            4. Restart the pods which are hosting cephfs files in large numbers.
            5. Check data integrity.
            6. Check for relabeling - this should not be happening.

        Args:
            pvc_factory (function): A call to pvc_factory function
            service_account_factory (function): A call to service_account_factory function
            copies (int): number of copies to write kernel files in pod

        """

        # Create cephfs rwop pvc
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_mode=constants.ACCESS_MODE_RWOP,
            size="20",
        )
        self.ocp_project = self.pvc_obj.project

        # Create service_account to get privilege for deployment pods
        self.service_account_obj = service_account_factory(
            project=self.pvc_obj.project,
        )

        # Create deployment pod
        self.pod_obj = self.create_deploymentconfig_pod(
            command=["/opt/multiple_files.sh"],
            command_args=[f"{copies}", "/mnt"],
        )
        log.info(f"files copied to pod {self.pod_obj.name}")

        expected_log = "system_u:object_r:container_file_t"
        pod_name = self.get_csi_cephfs_pod_on_same_node(self.pod_obj)

        log.info(f"Check logs of csi-cephfsplugin-xxx/csi-cephplugin pods {pod_name}")
        sample = TimeoutSampler(
            timeout=100,
            sleep=5,
            func=verify_log_exist_in_pods_logs,
            pod_names=[pod_name],
            container="csi-cephfsplugin",
            expected_log=expected_log,
            all_containers_flag=False,
            since="120s",
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError(
                f"The expected log '{expected_log}' does not exist in {pod_name}"
            )

        # Leave pod for some time to run since file creation time is longer
        waiting_time = 10
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(waiting_time)

        # Backup file
        self.backup_pod(pod_obj=self.pod_obj)
        pod_attach_time_1 = self.get_pod_start_time(self.pod_obj.name)

        log.info(f"Time taken by pod to restart is {pod_attach_time_1}")
        log.info(
            f"Sleep 120 seconds so the logs in {self.pod_obj.name} will be updated"
        )
        time.sleep(120)
        # Create cephfs rwo pvc
        self.pvc_obj2 = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_mode=constants.ACCESS_MODE_RWO,
            size="20",
            project=self.ocp_project,
        )

        # Create service_account to get privilege for deployment pods
        self.service_account_obj2 = service_account_factory(
            project=self.pvc_obj2.project,
        )

        # Create deployment pod
        self.pod_obj2 = self.create_deploymentconfig_pod(
            command=["/opt/multiple_files.sh"],
            command_args=[f"{copies}", "/mnt"],
            pvc_name=self.pvc_obj2.name,
        )
        log.info(f"files copied to pod {self.pod_obj.name}")

        # Leave pod for some time to run since file creation time is longer
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(waiting_time)

        log.info("Verify logs does not show 'skipping sparsify operation' message.")
        pod_name2 = self.get_csi_cephfs_pod_on_same_node(self.pod_obj2)
        log_exist = verify_log_exist_in_pods_logs(
            pod_names=[pod_name2],
            container="csi-cephfsplugin",
            expected_log=expected_log,
            all_containers_flag=False,
            since="120s",
        )
        if log_exist:
            raise ValueError(
                f"The expected log '{expected_log}' exist in {self.pod_obj2.name} pods after reclaimspacejob deletion"
            )

        self.backup_pod(pod_obj=self.pod_obj2)
        pod_attach_time_2 = self.get_pod_start_time(self.pod_obj2.name)
        log.info(f"Time taken by pod to restart is {pod_attach_time_2}")

        assert pod_attach_time_1 < pod_attach_time_2
