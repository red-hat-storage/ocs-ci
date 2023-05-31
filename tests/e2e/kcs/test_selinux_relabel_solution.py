import logging
import pytest
import yaml
import random
import time
import tempfile

from ocs_ci.ocs import ocp, constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import PodNotCreated
from ocs_ci.ocs.resources import pod as res_pod
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.framework import config

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
            pod_obj = helpers.create_pod(
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
                f"Pod attached to PVC {pod_obj.name} was not created, exception [{str(e)}]"
            )
            raise PodNotCreated("Pod attached to PVC was not created.")
        return pod_obj

    def data_integrity_check(self, pod_obj, namespace):
        """
        Check data integrity on pod.
        """
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=namespace,
        )
        data_path = f"{constants.FLEXY_MNT_CONTAINER_DIR}"
        num_of_files = random.randint(1, 9)
        random_file = ocp_obj.exec_oc_cmd(
            f"exec -it {pod_obj.name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n {num_of_files}"',
            timeout=300,
        )
        log.info(f"files are {random_file}")
        ini_md5sum_pod_data = res_pod.cal_md5sum(pod_obj=pod_obj, file_name=random_file)

        # Respin pod
        pod_obj.delete(wait=True)
        assert wait_for_pods_to_be_running(
            pod_names=[pod_obj.name], timeout=600, sleep=15
        )
        pod_objs = res_pod.get_all_pods(
            namespace=namespace,
            selector=[self.pod_selector],
            selector_label="deploymentconfig",
        )
        for pod_obj in pod_objs:
            pod_name = pod_obj.name
            if "-1-deploy" not in pod_name:
                pod_obj1 = pod_obj
            fin_md5sum_pod_data = res_pod.cal_md5sum(
                pod_obj=pod_obj1, file_name=random_file
            )
            assert ini_md5sum_pod_data == fin_md5sum_pod_data

    def teardown(self):
        """
        Cleanup the test environment
        """
        res_pod.delete_deploymentconfig_pods(self.pod_objs[0])
        self.pvc_obj.delete()
        self.sa_name.delete()

    @pytest.mark.parametrize("copies", [5])
    def test_selinux_relabel(self, copies):
        """
        Steps:
            1. Create multiple cephfs pvcs and 100K files each across multiple nested directories
            2. Run the IOs for some random files and take md5sum for them
            3. Apply the fix/solution as mentioned in the “Existing PVs” section
            4. Restart the pods which are hosting cephfs files in large numbers
            5. Check data integrity.
            6. Check for relabeling - this should not be happening.

        """
        self.project_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        # Create pvc
        self.pvc_obj = helpers.create_pvc(
            namespace=self.project_namespace,
            sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
            size="20Gi",
        )

        # Create deployment pod
        self.pod_obj = self.create_deploymentconfig_pod(
            command=["/opt/multiple_files.sh"],
            command_args=[f"{copies}", "/mnt"],
        )
        log.info(f"files copied to pod {self.pod_obj}")
        self.pod_selector = self.pod_obj.labels.get("deploymentconfig")
        pod1_name = self.pod_selector + "-1-deploy"
        pod = ocp.OCP(kind="pod", namespace=self.project_namespace)
        pod.delete(resource_name=pod1_name, wait=True)

        # Check data integrity before applying selinux relabeling solution
        start_time1 = time.time()
        self.data_integrity_check(self.pod_obj, self.project_namespace)
        end_time1 = time.time()
        total_time1 = end_time1 - start_time1
        log.info(f"Time taken by pod to restart is  {total_time1}")

        # Apply the fix/solution for “Existing PVCs”
        pv_name = self.pvc_obj.get().get("spec").get("volumeName")
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=self.project_namespace,
        )
        backup_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="test_", suffix=".yaml", delete=False
        )
        backup_file = backup_file.name
        backup_get = self.pvc_obj.backed_pv_obj.get()
        dump_data_to_temp_yaml(backup_get, backup_file)
        log.info(f"{backup_file} file created")

        ocp_pv = ocp.OCP(kind=constants.PV)
        patch_success = ocp_pv.patch(
            resource_name=pv_name,
            params='{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}',
        )
        if patch_success:
            log.info("Reclaim policy of %s was changed.", pv_name)
        else:
            log.error("Reclaim policy of %s failed to be changed.", pv_name)

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
        log.info(f"pv {pv_name} deleted")

        run_cmd(f"oc apply -f {backup_file}")
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

        # wait for some time before running data integrity
        time.sleep(120)
        self.pod_objs = res_pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=[self.pod_selector],
            selector_label="deploymentconfig",
        )

        # Respin pods to see selinux relabeling solution appplied
        for pod in self.pod_objs:
            pod.delete(wait=True)
        assert wait_for_pods_to_be_running(timeout=600, sleep=15)

        # Get deployment pod obj
        self.pod_objs = res_pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=[self.pod_selector],
            selector_label="deploymentconfig",
        )

        # Get the node running this pod
        node_name = res_pod.get_pod_node(pod_obj=self.pod_objs[0]).name
        oc_cmd = ocp.OCP(namespace=self.project_namespace)

        # Check SeLinux Relabeling
        cmd1 = "crictl inspect $(crictl ps --name perf -q)"
        output = oc_cmd.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd1])
        key = '"selinuxRelabel": false'
        assert key in output
        log.info(f"{key} is present in inspect logs of node")
        log.info(
            f"SeLinux Realabeling is not happening for the pvc {self.pvc_obj.name}"
        )
        self.pod_objs = res_pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=[self.pod_selector],
            selector_label="deploymentconfig",
        )

        # Check data integrity and get time for pod restart
        start_time2 = time.time()
        self.data_integrity_check(self.pod_objs[0], self.project_namespace)
        end_time2 = time.time()
        total_time2 = end_time2 - start_time2
        log.info(f"Time taken by pod to restart is  {total_time2}")

        assert total_time1 > total_time2
