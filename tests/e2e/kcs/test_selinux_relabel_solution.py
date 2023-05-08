import logging
import yaml
import time

from ocs_ci.ocs import ocp, constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import PodNotCreated
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.ocs.resources.pod import (
    validate_pods_are_respinned_and_running_state,
    delete_deploymentconfig_pods,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.templating import dump_data_to_temp_yaml

log = logging.getLogger(__name__)


class TestSelinuxrelabel(E2ETest):
    def create_deploymentconfig_pod(self, **kwargs):
        """"""
        self.project_namespace = constants.OPENSHIFT_STORAGE_NAMESPACE
        # Create service_account to get privilege for deployment pods
        sa_name = helpers.create_serviceaccount(namespace=self.project_namespace)

        helpers.add_scc_policy(
            sa_name=sa_name.name,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        try:
            self.pod_obj = helpers.create_pod(
                interface_type=constants.CEPHFS_INTERFACE,
                pvc_name=self.pvc_obj.name,
                namespace=self.project_namespace,
                sa_name=sa_name.name,
                dc_deployment=True,
                **kwargs,
            )
        except Exception as e:
            log.exception(
                f"Pod attached to PVC {self.pod_obj.name} was not created, exception [{str(e)}]"
            )
            raise PodNotCreated("Pod attached to PVC was not created.")

    def data_integrity_check(self, pod_obj, pvc_namespace):
        pod_name = pod_obj.name
        log.info(f"pod obj name---- {pod_name}")
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=pvc_namespace,
        )

        data_path = f"{constants.FLEXY_MNT_CONTAINER_DIR}"
        random_file = ocp_obj.exec_oc_cmd(
            f"exec -it {pod_name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n 1"',
            timeout=300,
        )
        log.info(f"files are {random_file}")
        ini_md5sum_pod_data = pod.cal_md5sum(pod_obj=pod_obj, file_name=random_file)
        assert validate_pods_are_respinned_and_running_state([pod_obj])
        pod_objs = pod.get_all_pods(
            namespace=pvc_namespace, selector=["pod-test-"], selector_label="name"
        )
        for pod_obj in pod_objs:
            pod_name = pod_obj.name
            if "-1-deploy" not in pod_name:
                pod_obj1 = pod_obj
            fin_md5sum_pod_data = pod.cal_md5sum(
                pod_obj=pod_obj1, file_name=random_file
            )
            assert ini_md5sum_pod_data == fin_md5sum_pod_data

    def teardown(self):
        """
        Cleanup the test environment
        """
        delete_deploymentconfig_pods(self.pod_obj)
        delete_pvcs([self.pvc_obj])

    def test_selinux_relabel(self):
        """
        Steps:
            1. Create multiple cephfs pvcs(4) and 100K files each across multiple nested  directories
            2. Run the IOs for few vols with specific files and take md5sum for them
            3. Apply the fix/solution as mentioned in the “Existing PVs” section
            4. Restart the pods which are hosting cephfs files in large numbers
            5. Check for relabeling - this should not be happening.
            6. Check data integrity.

        """
        # Create pvc
        self.pvc_obj = helpers.create_pvc(
            namespace=self.project_namespace,
            sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
            size="20Gi",
        )

        copies = 3
        self.create_deploymentconfig_pod(
            command=["/opt/multiple_files.sh"],
            command_args=[f"{copies}", "/mnt"],
        )
        log.info(f"files copied to pod {self.pod_obj}")

        start_time1 = time.time()
        self.data_integrity_check(self.pod_obj, self.project_namespace)
        end_time1 = time.time()
        total_time1 = end_time1 - start_time1
        log.info(f"Time taken by pod to restart is  {total_time1}")

        # Apply the fix/solution for “Existing PVs”
        pv_name = self.pvc_obj.get().get("spec").get("volumeName")
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=self.project_namespace,
        )

        backup_file = "/tmp/backup.yaml"
        backup_get = self.pvc_obj.backed_pv_obj.get()
        dump_data_to_temp_yaml(backup_get, backup_file)
        log.info("backup file created")

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
        with open(r"/tmp/backup.yaml", "r+") as backup:
            backup1 = yaml.safe_load(backup)
            backup1["spec"]["csi"]["volumeAttributes"][
                "kernelMountOptions"
            ] = 'context="system_u:object_r:container_file_t:s0"'
        with open(r"/tmp/backup.yaml", "w") as backup:
            yaml.dump(backup1, backup)
            log.info(f"{backup_file} is updated")

        ocp_pv.delete(resource_name=pv_name, wait=False)
        ocp_pv.patch(
            resource_name=pv_name,
            params='{"metadata": {"finalizers":null}}',
            format_type="merge",
        )
        ocp_pv.wait_for_delete(resource_name=ocp_obj)
        log.info(f"pv {pv_name} deleted")

        run_cmd(f"oc apply -f {backup_file}")
        log.info(f"Backup pv {pv_name}created")

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

        # Get the node running this pod
        node_name = pod.get_pod_node(pod_obj=self.pod_obj).name
        oc_cmd = ocp.OCP(namespace=self.project_namespace)
        cmd1 = f"crictl inspect $(crictl ps --name perf -q)"
        output = oc_cmd.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd1])
        key = f'"selinuxRelabel": false'
        assert key in output

        start_time2 = time.time()
        self.data_integrity_check(self.pod_obj, self.project_namespace)
        end_time2 = time.time()
        total_time2 = end_time2 - start_time2
        log.info(f"Time taken by pod to restart is  {total_time2}")
        assert total_time1 > total_time2
