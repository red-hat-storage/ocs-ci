import logging
import os.path

import pytest
import yaml
from ocs_ci.ocs.resources import pod
import time
from tempfile import mkdtemp
import os
from ocs_ci.ocs.resources.pod import validate_pods_are_respinned_and_running_state
from ocs_ci.framework.testlib import E2ETest, ignore_leftovers
from ocs_ci.ocs import ocp, constants
from ocs_ci.helpers import helpers
import urllib.request
from ocs_ci.ocs.resources.pod import delete_deploymentconfig_pods
from ocs_ci.ocs.resources import pod as res_pod
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.templating import dump_data_to_temp_yaml


TARFILE = "file.gz"
ntar_loc = mkdtemp()

log = logging.getLogger(__name__)


class TestSelinuxrelabel(E2ETest):
    @pytest.fixture()
    def download_files(self):
        kernel_url = "https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-4.19.5.tar.gz"
        download_path = ntar_loc
        dir_path = os.path.join(os.getcwd(), download_path)
        print(dir_path)
        file_path = os.path.join(dir_path, "file.gz")
        print(file_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        urllib.request.urlretrieve(kernel_url, file_path)
        print(dir_path)
        return dir_path

    @pytest.fixture()
    def create_pvc_and_deploymentconfig_pod(self, request, pvc_factory):
        """"""

        def finalizer():
            delete_deploymentconfig_pods(pod_obj)

        request.addfinalizer(finalizer)

        # Create pvc
        pvc_obj = pvc_factory(size=20)

        # Create service_account to get privilege for deployment pods
        sa_name = helpers.create_serviceaccount(pvc_obj.project.namespace)

        helpers.add_scc_policy(
            sa_name=sa_name.name, namespace=pvc_obj.project.namespace
        )

        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.project.namespace,
            sa_name=sa_name.name,
            dc_deployment=True,
        )
        helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING
        )
        return pod_obj, pvc_obj

    def copy_files(self, pod_obj, pvc_obj):
        log.info(f"pod obj name---- {pod_obj.name}")
        pod_name = pod_obj.name

        log.info("cephfs pod created")
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=pvc_obj.project.namespace,
        )
        # Number of times we copy the kernel
        copies = 3
        tmploc = ntar_loc.split("/")[-1]
        ocp_obj.exec_oc_cmd(
            f"rsync {ntar_loc} {pod_name}:{constants.FLEXY_MNT_CONTAINER_DIR}",
            timeout=300,
        )
        ocp_obj.exec_oc_cmd(
            f"exec {pod_name} -- mkdir {constants.FLEXY_MNT_CONTAINER_DIR}/x"
        )
        for x in range(copies):
            ocp_obj.exec_oc_cmd(
                f"exec {pod_name} -- /bin/tar xf"
                f" {constants.FLEXY_MNT_CONTAINER_DIR}/{tmploc}/{TARFILE}"
                f" -C {constants.FLEXY_MNT_CONTAINER_DIR}/x/x{x}",
                timeout=3600,
            )
        log.info("cephfs test files created on pod")

    def calculate_md5sum(self, pod_obj, pvc_obj):
        pod_name = pod_obj.name
        data_path = f'"{constants.FLEXY_MNT_CONTAINER_DIR}/x"'
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=pvc_obj.project.namespace,
        )
        random_file = ocp_obj.exec_oc_cmd(
            f"exec -it {pod_name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n 1"',
            timeout=300,
        )

        md5sum_pod_data = pod.cal_md5sum(pod_obj=pod_obj, file_name=random_file)
        return md5sum_pod_data

    def data_integrity_check(self, pod_obj, pvc_namespace):
        pod_name = pod_obj.name
        log.info(f"pod obj name---- {pod_name}")
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=pvc_namespace,
        )

        data_path = f"{constants.FLEXY_MNT_CONTAINER_DIR}/x"
        random_file = ocp_obj.exec_oc_cmd(
            f"exec -it {pod_name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n 1"',
            timeout=300,
        )
        print(f"files are {random_file}")
        ini_md5sum_pod_data = pod.cal_md5sum(pod_obj=pod_obj, file_name=random_file)
        start_time = time.time()
        assert validate_pods_are_respinned_and_running_state([pod_obj])
        end_time = time.time()
        total_time = end_time - start_time
        log.info(f"Time taken by pod to restart is  {total_time}")
        pod_objs = pod.get_all_pods(
            namespace=pvc_namespace, selector=["pod-test-rbd"], selector_label="name"
        )
        for pod_obj in pod_objs:
            pod_name = pod_obj.name
            if "-1-deploy" not in pod_name:
                pod_obj1 = pod_obj
            fin_md5sum_pod_data = pod.cal_md5sum(
                pod_obj=pod_obj1, file_name=random_file
            )
            assert ini_md5sum_pod_data == fin_md5sum_pod_data
        return total_time

    @ignore_leftovers
    def test_selinux_relabel(
        self,
        create_pvc_and_deploymentconfig_pod,
        download_files,
        snapshot_factory,
        snapshot_restore_factory,
        pod_factory,
    ):
        """
        Steps:
            1. Create multiple cephfs pvcs(4) and 100K files each across multiple nested  directories
            2. Have some snapshots created.
            3. Run the IOs for few vols with specific files and take md5sum for them
            4. Apply the fix/solution as mentioned in the “Existing PVs” section
            5. Restart the pods which are hosting cephfs files in large numbers
            6. Check for relabeling - this should not be happening.
            7. Check data integrity.

        """
        download_files
        pod_obj, pvc_obj = create_pvc_and_deploymentconfig_pod
        self.copy_files(pod_obj=pod_obj, pvc_obj=pvc_obj)
        log.info("files copied to pod")

        snap_obj = snapshot_factory(pvc_obj=pvc_obj, wait=False)
        log.info(f"snapshot created {snap_obj}")
        self.data_integrity_check(pod_obj, pvc_obj)

        # Apply the fix/solution in the “Existing PVs” section
        pv_name = pvc_obj.get().get("spec").get("volumeName")
        project_namespace = pvc_obj.project.namespace
        print(pv_name)
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=project_namespace,
        )

        backup_file = "/tmp/backup.yaml"
        backup_get = pvc_obj.backed_pv_obj.get()
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

        ocp_pv.delete(resource_name=pv_name, wait=False)
        ocp_pv.patch(
            resource_name=pv_name,
            params='{"metadata": {"finalizers":null}}',
            format_type="merge",
        )
        ocp_pv.wait_for_delete(resource_name=ocp_obj)
        log.info("pv deleted")
        run_cmd(f"oc apply -f {backup_file}")
        log.info("Backup pv created")

        # Get the node running this pod
        node_name = res_pod.get_pod_node(pod_obj=pod_obj).name
        oc_cmd = ocp.OCP(namespace=project_namespace)
        cmd1 = f"crictl inspect $(crictl ps --name fedora -q)"
        output = oc_cmd.exec_oc_debug_cmd(node=node_name, cmd_list=[cmd1])
        key = f'"selinuxRelabel": false'
        assert key in output

        self.data_integrity_check(pod_obj, project_namespace)
        log.info(f"Creating a PVC from snapshot [restore] {snap_obj.name}")
        restore_snap_obj = snapshot_restore_factory(snapshot_obj=snap_obj)
        log.info(f"snapshot restore created {restore_snap_obj}")
        pod_restore_obj = pod_factory(
            pvc=restore_snap_obj, status=constants.STATUS_RUNNING
        )
        log.info(f"pod restore created {pod_restore_obj.get()}")
