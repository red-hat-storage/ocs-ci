import logging
import pytest
import yaml
import random
import time
import tempfile
from datetime import datetime

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.exceptions import PodNotCreated
from ocs_ci.ocs.resources import pod as res_pod
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.templating import dump_data_to_temp_yaml


log = logging.getLogger(__name__)


class TestSelinuxrelabel(E2ETest):
    def create_deploymentconfig_pod(self, **kwargs):
        """
        create deployment pod.

        """

        # Create service_account to get privilege for deployment pods
        self.sa_name = helpers.create_serviceaccount(namespace=self.project_namespace)

        helpers.add_scc_policy(
            sa_name=self.sa_name.name,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        try:
            pod_objs = helpers.create_pod(
                interface_type=constants.CEPHFS_INTERFACE,
                pvc_name=self.pvc_obj.name,
                namespace=self.project_namespace,
                sa_name=self.sa_name.name,
                dc_deployment=True,
                pod_dict_path=constants.PERF_DC_LINUXTAR_FILES_YAML,
                **kwargs,
            )
        except Exception as e:
            log.exception(
                f"Pod attached to PVC {pod_objs.name} was not created, exception [{str(e)}]"
            )
            raise PodNotCreated("Pod attached to PVC was not created.")
        return pod_objs

    def get_cephfs_test_pod(self):
        """
        Returns cephfs app pods

        """
        pod_objs = res_pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=[self.pod_selector],
            selector_label="deploymentconfig",
        )
        return pod_objs

    def apply_selinux_solution(self, pvc_obj):
        """
        Apply selinux relabeling solution on existing PV

        """
        pv_name = pvc_obj.get().get("spec").get("volumeName")
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=self.project_namespace,
        )
        backup_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="test_", suffix=".yaml", delete=False
        )
        backup_file = backup_file.name
        backup_get = pvc_obj.backed_pv_obj.get()
        dump_data_to_temp_yaml(backup_get, backup_file)
        log.info(f"{backup_file} file created")

        ocp_pv = ocp.OCP(kind=constants.PV)
        patch_success = ocp_pv.patch(
            resource_name=pv_name,
            params='{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}',
        )
        if patch_success:
            log.info(f"Reclaim policy of {pv_name} was changed.")
        else:
            log.error(f"Reclaim policy of {pv_name} failed to be changed.")

        yaml.safe_load(backup_file)
        with open(backup_file, "r+") as backup:
            backup1 = yaml.safe_load(backup)
            backup1["spec"]["csi"]["volumeAttributes"][
                "kernelMountOptions"
            ] = 'context="system_u:object_r:container_file_t:s0"'
        with open(backup_file, "w") as backup:
            yaml.dump(backup1, backup)
            log.info(f"{backup_file} file is updated")

        ocp_pv.delete(resource_name=pv_name, wait=False)
        ocp_pv.patch(
            resource_name=pv_name,
            params='{"metadata": {"finalizers":null}}',
            format_type="merge",
        )
        ocp_pv.wait_for_delete(resource_name=ocp_obj)
        log.info(f"PersistentVolume {pv_name} deleted")

        run_cmd(f"oc apply -f {backup_file}")
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        log.info(f"Backup pv {pv_name} created")

        params = [
            {
                "op": "remove",
                "path": "/metadata/annotations/pv.kubernetes.io~1bind-completed",
            }
        ]
        ocp_pvc = ocp.OCP(kind=constants.PVC, namespace=self.project_namespace)
        ocp_pvc.patch(
            resource_name=self.pvc_obj.name,
            params=params,
            format_type="json",
        )

    def get_pod_start_time(self, pod_name):
        """
        Get the time required for pod to come in a running state
        Args:
            pod_name (str): Pod to look for

        Returns:
            datetime: time required for pod restart

        """
        try:
            # Get the pod conditions
            pod = ocp.OCP(kind="pod", namespace=self.project_namespace)
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

        except Exception as e:
            log.info(f"Error retrieving pod information for '{pod_name}': {e}")

        return None

    def teardown(self):
        """
        Cleanup the test environment
        """
        res_pod.delete_deploymentconfig_pods(self.pod_objs[0])
        self.pvc_obj.delete(wait=True)
        self.sa_name.delete()

    @pytest.mark.parametrize("copies", [5])
    def test_selinux_relabel(self, copies):
        """
        Steps:
            1. Create cephfs pvcs and attach pod with more than 100K files across multiple nested directories
            2. Take md5sum for them some random files and get pod restart time
            3. Apply the fix/solution from kcs https://access.redhat.com/solutions/6906261
            4. Restart the pods which are hosting cephfs files in large numbers.
            5. Check data integrity.
            6. Check for relabeling - this should not be happening.

        Args:
            copies: number of copies to write kernel files in pod

        """
        self.project_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE

        # Create cephfs pvc
        self.pvc_obj = helpers.create_pvc(
            namespace=self.project_namespace,
            sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
            size="20Gi",
        )

        # Create deployment pod
        self.pod_objs = self.create_deploymentconfig_pod(
            command=["/opt/multiple_files.sh"],
            command_args=[f"{copies}", "/mnt"],
        )
        log.info(f"files copied to pod {self.pod_objs.name}")
        self.pod_selector = self.pod_objs.labels.get("deploymentconfig")
        pod1_name = self.pod_selector + "-1-deploy"
        pod = ocp.OCP(kind="pod", namespace=self.project_namespace)
        pod.delete(resource_name=pod1_name, wait=True)

        # Leave pod for some time to run
        waiting_time = 120
        log.info(f"Waiting for {waiting_time} seconds")
        time.sleep(120)

        # Get the md5sum of some random files
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=self.project_namespace,
        )
        data_path = f"{constants.FLEXY_MNT_CONTAINER_DIR}"
        num_of_files = random.randint(3, 9)
        random_files = ocp_obj.exec_oc_cmd(
            f"exec -it {self.pod_objs.name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n {num_of_files}"',
            timeout=300,
        )
        random_files = random_files.split()
        log.info(f"files are {random_files}")
        initial_data_on_pod = []
        for file_path in random_files:
            md5sum = res_pod.cal_md5sum(
                pod_obj=self.pod_objs,
                file_name=file_path,
            )
            initial_data_on_pod.append(md5sum)

        # Delete pod and Get time for pod restart
        self.pod_objs.delete(wait=True)
        self.pod_objs = self.get_cephfs_test_pod()
        log.info(f"pod name is {self.pod_objs[0].name}")

        assert wait_for_pods_to_be_running(
            pod_names=[self.pod_objs[0].name], timeout=600, sleep=15
        )
        pod_restart_time1 = self.get_pod_start_time(pod_name=self.pod_objs[0].name)
        log.info(f"Time taken by pod to restart is  {pod_restart_time1}")

        # Apply the fix/solution for “Existing PVCs”
        self.apply_selinux_solution(self.pvc_obj)

        # Delete pod so that fix will be applied for new pod
        self.pod_objs = self.get_cephfs_test_pod()
        self.pod_objs[0].delete(wait=True)
        self.pod_objs = self.get_cephfs_test_pod()
        log.info(f"pod name is {self.pod_objs[0].name}")
        assert wait_for_pods_to_be_running(
            pod_names=[self.pod_objs[0].name], timeout=600, sleep=15
        )
        pod_restart_time2 = self.get_pod_start_time(pod_name=self.pod_objs[0].name)
        log.info(f"Time taken by pod to restart is  {pod_restart_time2}")

        # Get the node of cephfs pod
        self.pod_objs = self.get_cephfs_test_pod()
        node_name = res_pod.get_pod_node(pod_obj=self.pod_objs[0]).name
        oc_cmd = ocp.OCP(namespace=self.project_namespace)

        # Check SeLinux Relabeling is set to false
        cmd1 = "crictl inspect $(crictl ps --name perf -q)"
        output = oc_cmd.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd1])
        key = '"selinuxRelabel": false'
        assert key in output
        log.info(f"{key} is present in inspect logs of node")
        log.info(
            f"SeLinux Realabeling is not happening for the pvc {self.pvc_obj.name}"
        )

        # Get time for pod restart
        self.pod_objs = self.get_cephfs_test_pod()
        self.pod_objs[0].delete(wait=True)
        self.pod_objs = self.get_cephfs_test_pod()
        log.info(f"pod name is {self.pod_objs[0].name}")
        assert wait_for_pods_to_be_running(
            pod_names=[self.pod_objs[0].name], timeout=600, sleep=15
        )
        pod_restart_time2 = self.get_pod_start_time(pod_name=self.pod_objs[0].name)
        log.info(f"Time taken by pod to restart is  {pod_restart_time2}")

        assert pod_restart_time1 > pod_restart_time2

        # Check data integrity.
        final_data_on_pod = []
        for file_path in random_files:
            md5sum = res_pod.cal_md5sum(
                pod_obj=self.pod_objs[0],
                file_name=file_path,
            )
            final_data_on_pod.append(md5sum)

        assert initial_data_on_pod == final_data_on_pod
