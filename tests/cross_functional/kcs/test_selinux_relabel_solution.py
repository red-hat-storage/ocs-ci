import logging
import pytest
import yaml
import random
import time
import tempfile
from datetime import datetime

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import check_selinux_relabeling
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.exceptions import PodNotCreated, CommandFailed
from ocs_ci.ocs.resources import pod as res_pod
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    bugzilla,
    polarion_id,
    magenta_squad,
)

log = logging.getLogger(__name__)


@magenta_squad
@tier1
class TestSelinuxrelabel(E2ETest):
    def create_deploymentconfig_pod(self, **kwargs):
        """
        Create deployment pod.

        Returns:
            object: helpers.create_pod instance

        """
        try:
            pod_obj = helpers.create_pod(
                interface_type=constants.CEPHFS_INTERFACE,
                pvc_name=self.pvc_obj.name,
                namespace=config.ENV_DATA["cluster_namespace"],
                sa_name=self.service_account_obj.name,
                dc_deployment=True,
                pod_dict_path=constants.PERF_DC_YAML,
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
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=[self.pod_selector],
            selector_label=constants.DEPLOYMENTCONFIG,
        )
        pod_name = self.pod_selector + "-1-deploy"
        for pod_obj in pod_obj_list:
            if pod_name not in pod_obj.name:
                return pod_obj

    def apply_selinux_solution_on_existing_pvc(self, pvc_obj):
        """
        Apply selinux relabeling solution on existing PV.

        Args:
            pvc_obj(PVC object): ocs_ci.ocs.resources.pvc.PVC instance kind.

        """
        # Backup existing PV
        log.info("Getting backup of existing PV")
        pv_name = pvc_obj.get().get("spec").get("volumeName")
        backup_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="test_", suffix=".yaml", delete=False
        )
        backup_file = backup_file.name
        backup_get = pvc_obj.backed_pv_obj.get()
        dump_data_to_temp_yaml(backup_get, backup_file)
        log.info(f"{backup_file} file for PV is created")

        # Change the Reclaim policy of PV
        ocp_pv = ocp.OCP(kind=constants.PV)
        patch_success = ocp_pv.patch(
            resource_name=pv_name,
            params='{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}',
        )
        if patch_success:
            log.info(f"Reclaim policy of {pv_name} was changed.")
        else:
            log.exception(f"Reclaim policy of {pv_name} failed to be changed.")

        # Edit backup PV yaml
        yaml.safe_load(backup_file)
        with open(backup_file, "r+") as backup:
            backup1 = yaml.safe_load(backup)
            backup1["spec"]["csi"]["volumeAttributes"][
                "kernelMountOptions"
            ] = 'context="system_u:object_r:container_file_t:s0"'
        with open(backup_file, "w") as backup:
            yaml.dump(backup1, backup)
            log.info(f"PV {backup_file} file is updated")

        # Delete existing PV
        log.info("Deleting the existing PV")
        ocp_pv.delete(resource_name=pv_name, wait=False)
        ocp_pv.patch(
            resource_name=pv_name,
            params='{"metadata": {"finalizers":null}}',
            format_type="merge",
        )
        ocp_pv.wait_for_delete(resource_name=pv_name)
        log.info(f"PersistentVolume {pv_name} deleted")

        # Recreate PV from backup file
        run_cmd(f"oc apply -f {backup_file}")
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        log.info(f"Backup PV {pv_name} created")

        # Re-bind the PV and PVC by removing annonation from PVC
        params = [
            {
                "op": "remove",
                "path": "/metadata/annotations/pv.kubernetes.io~1bind-completed",
            }
        ]
        ocp_pvc = ocp.OCP(
            kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"]
        )
        ocp_pvc.patch(
            resource_name=self.pvc_obj.name,
            params=params,
            format_type="json",
        )
        log.info(f"PVC {self.pvc_obj.name} is modified")

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
            pod = ocp.OCP(kind="pod", namespace=config.ENV_DATA["cluster_namespace"])
            conditions = pod.exec_oc_cmd(
                f"get pod {pod_name} -n openshift-storage -o jsonpath='{{.status.conditions}}'"
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
        ocp_obj = ocp.OCP(kind=constants.OPENSHIFT_STORAGE_NAMESPACE)
        random_files = ocp_obj.exec_oc_cmd(
            f"exec -n {constants.OPENSHIFT_STORAGE_NAMESPACE} -it {pod_obj.name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n {num_of_files}"',
            timeout=300,
        )
        random_files = random_files.split()
        log.info(f"files are {random_files}")
        return random_files

    def teardown(self):
        """
        Cleanup the test environment
        """
        res_pod.delete_deploymentconfig_pods(self.pod_obj)

    @bugzilla("1988284")
    @polarion_id("OCS-5132")
    @pytest.mark.parametrize("copies", [5])
    def test_selinux_relabel_for_existing_pvc(
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
        self.ocp_project = ocp.OCP(
            kind=constants.NAMESPACE, namespace=config.ENV_DATA["cluster_namespace"]
        )

        # Create cephfs pvc
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=self.ocp_project,
            size="20",
        )

        # Create service_account to get privilege for deployment pods
        self.service_account_obj = service_account_factory(
            project=self.ocp_project,
        )

        # Create deployment pod
        self.pod_obj = self.create_deploymentconfig_pod(
            command=["/opt/multiple_files.sh"],
            command_args=[f"{copies}", "/mnt"],
        )
        log.info(f"files copied to pod {self.pod_obj.name}")
        self.pod_selector = self.pod_obj.labels.get(constants.DEPLOYMENTCONFIG)

        # Leave pod for some time to run since file creation time is longer
        waiting_time = 120
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(120)

        # Get the md5sum of some random files
        random_files = self.get_random_files(self.pod_obj)
        initial_md5sum = []
        for file_path in random_files:
            md5sum = res_pod.cal_md5sum(
                pod_obj=self.pod_obj,
                file_name=file_path,
            )
            initial_md5sum.append(md5sum)

        # Delete pod and Get time for pod restart
        self.pod_obj.delete(wait=True)
        self.pod_obj = self.get_app_pod_obj()
        try:
            wait_for_pods_to_be_running(
                pod_names=[self.pod_obj.name], timeout=600, sleep=15
            )
        except CommandFailed:
            log.exception(f"Pod {self.pod_obj.name} didn't reach to running state")

        pod_restart_time_before_fix = self.get_pod_start_time(
            pod_name=self.pod_obj.name
        )
        log.info(f"Time taken by pod to restart is {pod_restart_time_before_fix}")

        # Apply the fix/solution for “Existing PVCs”
        self.apply_selinux_solution_on_existing_pvc(self.pvc_obj)

        # Delete pod so that fix will be applied for new pod
        self.pod_obj = self.get_app_pod_obj()
        self.pod_obj.delete(wait=True)
        self.pod_obj = self.get_app_pod_obj()
        assert wait_for_pods_to_be_running(
            pod_names=[self.pod_obj.name], timeout=600, sleep=15
        ), f"Pod {self.pod_obj.name} didn't reach to running state"

        # Check SeLinux Relabeling is set to false
        check_selinux_relabeling(pod_obj=self.pod_obj)
        log.info(f"SeLinux Relabeling is not happening for the pvc {self.pvc_obj.name}")

        # Restart pod after applying fix
        self.pod_obj = self.get_app_pod_obj()
        self.pod_obj.delete(wait=True)
        self.pod_obj = self.get_app_pod_obj()
        assert wait_for_pods_to_be_running(
            pod_names=[self.pod_obj.name], timeout=600, sleep=15
        ), f"Pod {self.pod_obj.name} didn't reach to running state"

        # Check data integrity.
        final_md5sum = []
        for file_path in random_files:
            md5sum = res_pod.cal_md5sum(
                pod_obj=self.pod_obj,
                file_name=file_path,
            )
            final_md5sum.append(md5sum)

        assert (
            initial_md5sum == final_md5sum
        ), "Data integrity failed after applying fix."

        # Get pod restart time.
        pod_restart_time_after_fix = self.get_pod_start_time(pod_name=self.pod_obj.name)
        log.info(f"Time taken by pod to restart is {pod_restart_time_after_fix}")

        assert (
            pod_restart_time_before_fix > pod_restart_time_after_fix
        ), "Time taken for pod restart after fix is more than before fix."

    @polarion_id("OCS-5163")
    @pytest.mark.parametrize("copies", [5])
    def test_selinux_relabel_for_new_pvc(
        self,
        pvc_factory,
        service_account_factory,
        storageclass_factory,
        copies,
        teardown_factory,
    ):
        """
        Steps:
            1. Create storageclass with security context set.
            2. Create cephfs pvcs and attach pod with more than 100K files across multiple nested directories
            3. Take md5sum for them some random files and get pod restart time
            4. Restart the pod which is hosting cephfs files in large numbers.
            5. Check data integrity.
            6. Check for relabeling - this should not be happening.

        Args:
            pvc_factory (function): A call to pvc_factory function
            service_account_factory (function): A call to service_account_factory function
            storageclass_factory ((function): A call to storageclass_factory function
            copies (int): number of copies to write kernel files in pod

        """
        # Create storageclass with security context
        sc_name = "ocs-storagecluster-cephfs-selinux-relabel"
        self.storage_class = storageclass_factory(
            sc_name=sc_name,
            interface=constants.CEPHFILESYSTEM,
            kernelMountOptions='context="system_u:object_r:container_file_t:s0"',
        )

        # Create PVC using new storageclass
        self.ocp_project = ocp.OCP(
            kind=constants.NAMESPACE, namespace=config.ENV_DATA["cluster_namespace"]
        )
        self.pvc_obj = pvc_factory(
            project=self.ocp_project,
            storageclass=self.storage_class,
            size="20",
        )
        log.info(f"PVC {self.pvc_obj.name} created")
        teardown_factory(self.pvc_obj)

        # Create service_account to get privilege for deployment pods
        self.service_account_obj = service_account_factory(
            project=self.ocp_project,
        )

        # Create deployment pod
        self.pod_obj = self.create_deploymentconfig_pod(
            command=["/opt/multiple_files.sh"],
            command_args=[f"{copies}", "/mnt"],
        )

        log.info(f"pod {self.pod_obj.name} created")
        self.pod_selector = self.pod_obj.labels.get(constants.DEPLOYMENTCONFIG)

        # Get the md5sum of some random files
        random_files = self.get_random_files(self.pod_obj)
        initial_md5sum = []
        for file_path in random_files:
            md5sum = res_pod.cal_md5sum(
                pod_obj=self.pod_obj,
                file_name=file_path,
            )
            initial_md5sum.append(md5sum)

        # Delete app pod and measure pod restart time
        self.pod_obj.delete(wait=True)
        self.pod_obj = self.get_app_pod_obj()
        try:
            wait_for_pods_to_be_running(
                pod_names=[self.pod_obj.name], timeout=600, sleep=15
            )
        except CommandFailed:
            log.exception(f"Pod {self.pod_obj.name} didn't reach to running state")

        pod_restart_time_after_fix = self.get_pod_start_time(pod_name=self.pod_obj.name)
        log.info(f"Time taken by pod to restart is {pod_restart_time_after_fix}")

        # Check Data integrity
        final_md5sum = []
        for file_path in random_files:
            md5sum = res_pod.cal_md5sum(
                pod_obj=self.pod_obj,
                file_name=file_path,
            )
            final_md5sum.append(md5sum)
        assert (
            initial_md5sum == final_md5sum
        ), f"Data integrity failed after for PVC: {self.pvc_obj.name}"

        # Check SeLinux Relabeling is set to false
        check_selinux_relabeling(pod_obj=self.pod_obj)
        log.info(f"SeLinux Relabeling is skipped for the pvc {self.pvc_obj.name}")
