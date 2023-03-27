import logging
import os.path
import yaml
from shutil import rmtree
from ocs_ci.ocs.resources import pod
import time
from tempfile import mkdtemp, NamedTemporaryFile

from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.framework import config
from ocs_ci.framework.testlib import E2ETest, ignore_leftovers
from ocs_ci.ocs import ocp, constants
from ocs_ci.utility.utils import run_cmd
from ocs_ci.helpers import helpers

TARFILE = "cephfs.tar.gz"
SIZE = "20Gi"
TFILES = 40
SAMPLE_TEXT = b"QWERTYUIOP ASDFGJKL ZXCVBNM"
ntar_loc = mkdtemp()

log = logging.getLogger(__name__)


class TestSelinuxrelabel(E2ETest):
    def create_files(self):
        log.info(f"Creating {TFILES} files ")
        onetenth = TFILES / 10
        endoften = onetenth - 1

        tarfile = os.path.join(ntar_loc, TARFILE)
        one_dir = mkdtemp()
        log.info(f"dir crated{one_dir}")
        n = 0

        while n in range(0, 11):
            first_dir = mkdtemp(suffix="Fdir", dir=one_dir)
            log.info(f"created dir {first_dir}")
            for i in range(0, 11):
                new_dir = mkdtemp(suffix="Dir", dir=first_dir)
                log.info(f"Dir created {new_dir}")
                test_file_list = []
                for j in range(0, TFILES):
                    tmpfile = NamedTemporaryFile(dir=new_dir, delete=False)
                    fname = tmpfile.name
                    with tmpfile:
                        tmpfile.write(SAMPLE_TEXT)
                    if i % onetenth == endoften:
                        dispv = i + 1
                        log.info(f"{dispv} local files created")
                        test_file_list.append(fname.split(os.sep)[-1])
                first_dir = new_dir
            n = n + 1
        run_cmd(f"tar cfz {tarfile} -C {one_dir} .", timeout=1800)
        rmtree(one_dir)
        return tarfile

    def create_pvc_pod(self):
        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        self.namespace = "openshift-storage"
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=self.namespace
        )
        self.pod_obj = ocp.OCP(kind="Pod", namespace=self.namespace)
        self.pvc_obj = ocp.OCP(kind=constants.PVC, namespace=self.namespace)
        self.pv_obj = ocp.OCP(kind=constants.PV, namespace=self.namespace)
        self.nfs_sc = "ocs-storagecluster-cephfs"

        self.nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            pvc_name="nfs-pvc-2",
            size="10Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode="Filesystem",
        )
        log.info(f"{self.nfs_pvc_obj} created")

        # Create deployment config for app pod
        log.info("----create deployment config----")
        deployment_config = """
                                apiVersion: apps.openshift.io/v1
                                kind: DeploymentConfig
                                metadata:
                                  name: nfs-test-pod-2
                                  namespace: openshift-storage
                                  labels:
                                    app: nfs-test-pod
                                spec:
                                  template:
                                    metadata:
                                      labels:
                                        name: nfs-test-pod-2
                                    spec:
                                      restartPolicy: Always
                                      volumes:
                                      - name: vol
                                        persistentVolumeClaim:
                                          claimName: nfs-pvc-2
                                      containers:
                                      - name: fedora-2
                                        image: fedora
                                        command: ['/bin/bash', '-ce', 'tail -f /dev/null']
                                        imagePullPolicy: IfNotPresent
                                        securityContext:
                                          capabilities: {}
                                          privileged: true
                                        volumeMounts:
                                        - mountPath: /mnt
                                          name: vol
                                        livenessProbe:
                                          exec:
                                            command:
                                            - 'sh'
                                            - '-ec'
                                            - 'df /mnt'
                                          initialDelaySeconds: 5
                                          periodSeconds: 5
                                  replicas: 1
                                  triggers:
                                    - type: ConfigChange
                                  paused: false
                                """
        deployment_config_data = yaml.safe_load(deployment_config)
        helpers.create_resource(**deployment_config_data)
        time.sleep(60)

        assert self.pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector=f"name=nfs-test-pod-2",
            dont_allow_other_resources=True,
            timeout=60,
        )
        pod_objs = pod.get_all_pods(
            namespace=self.namespace, selector=["nfs-test-pod-2"], selector_label="name"
        )

        pod_obj = pod_objs[0]
        log.info(f"pod obj name----{pod_obj.name}")
        pod_name = pod_obj.name

        log.info("cephfs pod created")
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        tmploc = ntar_loc.split("/")[-1]
        ocp_obj.exec_oc_cmd(
            f"rsync {ntar_loc} {pod_name}:{constants.FLEXY_MNT_CONTAINER_DIR}",
            timeout=300,
        )
        ocp_obj.exec_oc_cmd(
            f"exec {pod_name} -- mkdir {constants.FLEXY_MNT_CONTAINER_DIR}/x"
        )
        ocp_obj.exec_oc_cmd(
            f"exec {pod_name} -- /bin/tar xf"
            f" {constants.FLEXY_MNT_CONTAINER_DIR}/{tmploc}/{TARFILE}"
            f" -C {constants.FLEXY_MNT_CONTAINER_DIR}/x",
            timeout=3600,
        )
        log.info("cephfs test files created on pod")

    def calculate_md5sum(pod_obj):
        pod_name = pod_obj.name
        data_path = f'"{constants.FLEXY_MNT_CONTAINER_DIR}/x"'
        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        random_file = ocp_obj.exec_oc_cmd(
            f"exec -it {pod_name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n 10"',
            timeout=300,
        )

        md5sum_pod_data = pod.cal_md5sum(pod_obj=pod_obj, file_name=random_file)
        return md5sum_pod_data

    def data_integrity_check(self):
        pod_objs = pod.get_all_pods(
            namespace=self.namespace, selector=["nfs-test-pod-2"], selector_label="name"
        )

        pod_obj = pod_objs[0]
        log.info(f"pod obj name----{pod_obj.name}")
        pod_name = pod_obj.name

        ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        data_path = f'"{constants.FLEXY_MNT_CONTAINER_DIR}/x"'
        random_file = ocp_obj.exec_oc_cmd(
            f"exec -it {pod_name} -- /bin/bash"
            f' -c "find {data_path} -type f | "shuf" -n 1"',
            timeout=300,
        )

        ini_md5sum_pod_data = pod.cal_md5sum(pod_obj=pod_obj, file_name=random_file)
        start_time = time.time()
        pod_obj.delete(wait=False)
        assert wait_for_pods_to_be_running(timeout=600, sleep=15)
        end_time = time.time()
        total_time = end_time - start_time
        log.info(f"Time taken by pod to restart is  {total_time}")

        pod_objs = pod.get_all_pods(
            namespace=self.namespace, selector=["nfs-test-pod-2"], selector_label="name"
        )
        pod_obj = pod_objs[0]
        fin_md5sum_pod_data = pod.cal_md5sum(pod_obj=pod_obj, file_name=random_file)
        assert ini_md5sum_pod_data == fin_md5sum_pod_data
        return total_time

    def cleanup(self):
        # Delete deployment config
        cmd_delete_deployment_config = "delete dc nfs-test-pod"
        self.storage_cluster_obj.exec_oc_cmd(cmd_delete_deployment_config)
        # Delete pvc
        self.nfs_pvc_obj.delete()
        log.info("Teardown complete")

    @ignore_leftovers
    def test_selinux_relabel(
        self, snapshot_factory, snapshot_restore_factory, pod_factory
    ):
        """
        Steps:
            1. Create multiple cephfs pvcs(4) and 100K files each across multiple nested  directories
            2. Have some snapshots created.
            3. Run the IOs for few vols with specific files and take md5sum for them
            4. Apply the fix/solution as mentioned in the “Existing PVs” section
            5. Restart the pods which are hosting cephfs files in large numbers
            6. Check for relabeling - this should not be happening.
            7. Do restore snapshot - verify the data integrity and “context” applied as part of the solution
            8. Verify the data integrity on the files from step “Run the IOs for few vols with specific files

        """
        self.create_files()
        self.create_pvc_pod()
        snap_obj = snapshot_factory(pvc_obj=self.nfs_pvc_obj, wait=False)
        log.info(f"snapshot created {snap_obj}")

        # Todo
        # Apply the fix/solution in the “Existing PVs” section
        #
        #

        self.data_integrity_check()
        log.info(f"Creating a PVC from snapshot [restore] {snap_obj.name}")
        restore_snap_obj = snapshot_restore_factory(snapshot_obj=snap_obj)
        log.info(f"snapshot restore created {restore_snap_obj}")
        pod_restore_obj = pod_factory(
            pvc=restore_snap_obj, status=constants.STATUS_RUNNING
        )
        log.info(f"pod restore created {pod_restore_obj}")
        self.calculate_md5sum(pod_obj=pod_restore_obj)
