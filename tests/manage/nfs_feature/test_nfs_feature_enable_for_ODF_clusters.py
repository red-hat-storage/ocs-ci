import pytest
import logging


from ocs_ci.utility import utils, nfs_utils
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)

from ocs_ci.ocs.resources import pod, ocs

from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@tier1
@skipif_ocs_version("<4.11")
@skipif_ocp_version("<4.11")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestDefaultNfsDisabled(ManageTest):
    """
    Test nfs feature enable for ODF 4.11

    """

    def test_nfs_not_enabled_by_default(self):
        """
        This test is to validate nfs feature is not enabled by default for  ODF(4.11) clusters

        Steps:
        1:- Check cephnfs resources not available by default

        """
        storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace="openshift-storage"
        )
        # Checks cephnfs resources not available by default
        cephnfs_resource = storage_cluster_obj.exec_oc_cmd("get cephnfs")
        if cephnfs_resource is None:
            log.info(f"No resources found in openshift-storage namespace.")
        else:
            log.error("nfs feature is enabled by default")


@tier1
@skipif_ocs_version("<4.11")
@skipif_ocp_version("<4.11")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestNfsEnable(ManageTest):
    """
    Test nfs feature enable for ODF 4.11

    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown(self, request):
        """
        Setup-Teardown for the class

        Steps:
        ---Setup---
        1:- Create objects for storage_cluster, configmap, pod, pv, pvc, service and storageclass
        2:- Fetch number of cephfsplugin and cephfsplugin_provisioner pods running
        3:- Enable nfs feature
        4:- Create loadbalancer service for nfs
        ---Teardown---
        5:- Disable nfs feature
        6:- Delete ocs nfs Service

        """
        self = request.node.cls
        log.info("-----Setup-----")
        self.namespace = "openshift-storage"
        self.storage_cluster_obj = ocp.OCP(
            kind="Storagecluster", namespace=self.namespace
        )
        self.config_map_obj = ocp.OCP(kind="Configmap", namespace=self.namespace)
        self.pod_obj = ocp.OCP(kind="Pod", namespace=self.namespace)
        self.service_obj = ocp.OCP(kind="Service", namespace=self.namespace)
        self.pvc_obj = ocp.OCP(kind=constants.PVC, namespace=self.namespace)
        self.pv_obj = ocp.OCP(kind=constants.PV, namespace=self.namespace)
        self.nfs_sc = "ocs-storagecluster-ceph-nfs"
        self.sc = ocs.OCS(kind=constants.STORAGECLASS, metadata={"name": self.nfs_sc})
        self.test_folder = "test_nfs"
        utils.exec_cmd(cmd="mkdir -p " + self.test_folder)

        # Enable nfs feature
        log.info("----Enable nfs----")
        nfs_ganesha_pod_name = nfs_utils.nfs_enable(
            self.storage_cluster_obj,
            self.config_map_obj,
            self.pod_obj,
            self.namespace,
        )

        # Create loadbalancer service for nfs
        self.hostname_add = nfs_utils.create_nfs_load_balancer_service(
            self.storage_cluster_obj,
        )

        yield

        log.info("-----Teardown-----")
        # Disable nfs feature
        nfs_utils.nfs_disable(
            self.storage_cluster_obj,
            self.config_map_obj,
            self.pod_obj,
            self.sc,
            nfs_ganesha_pod_name,
        )
        # Delete ocs nfs Service
        nfs_utils.delete_nfs_load_balancer_service(
            self.storage_cluster_obj,
        )

        utils.exec_cmd(cmd="rm -rf " + self.test_folder)

    def test_nfs_feature_enable(
        self,
    ):
        """
        This test is to validate nfs feature enable after deployment of  ODF(4.11) cluster

        Steps:
        1:- Check cephnfs resource running

        """
        # Check cephnfs resource running
        cephnfs_resource_status = self.storage_cluster_obj.exec_oc_cmd(
            "get CephNFS ocs-storagecluster-cephnfs --output jsonpath='{.status.phase}'"
        )
        assert cephnfs_resource_status == "Ready"

    def test_incluster_nfs_export(
        self,
        pod_factory,
    ):
        """
        This test is to validate NFS export using a PVC mounted on an app pod (in-cluster)

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create pods with nfs pvcs mounted
        3:- Run IO
        4:- Wait for IO completion
        5:- Verify presence of the file
        6:- Deletion of Pods and PVCs

        """
        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode="Filesystem",
        )

        # Create nginx pod with nfs pvcs mounted
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        file_name = pod_obj.name
        # Run IO
        pod_obj.run_io(
            storage_type="fs",
            size="4G",
            fio_filename=file_name,
            runtime=60,
        )
        log.info("IO started on all pods")

        # Wait for IO completion
        fio_result = pod_obj.get_fio_results()
        log.info("IO completed on all pods")
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )
        # Verify presence of the file
        file_path = pod.get_file_path(pod_obj, file_name)
        log.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        log.info(f"File {file_name} exists in {pod_obj.name}")

        # Deletion of Pods and PVCs
        log.info(f"Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

    def test_outcluster_nfs_export(
        self,
        pod_factory,
    ):
        """
        This test is to validate export where the export is consumed from outside the Openshift cluster
        - Create a LoadBalancer Service pointing to the CephNFS server
        - Direct external NFS clients to the Service endpoint from the step above

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create nginx pod with nfs pvcs mounted
        3:- Fetch sharing details for the nfs pvc
        4:- Run IO
        5:- Wait for IO completion
        6:- Verify presence of the file
        7:- Create /var/lib/www/html/index.html file
        8:- Connect the external client using the share path and ingress address
        9:- Verify able to read exported volume
        10:- Verify able to write to the exported volume from external client
        11:- Able to read updated /var/lib/www/html/index.html file from inside the pod
        12:- unmount
        13:- Deletion of Pods and PVCs

        """

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode="Filesystem",
        )

        # Create nginx pod with nfs pvcs mounted
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        # Fetch sharing details for the nfs pvc
        fetch_vol_name_cmd = (
            "get pvc " + nfs_pvc_obj.name + " --output jsonpath='{.spec.volumeName}'"
        )
        vol_name = self.pvc_obj.exec_oc_cmd(fetch_vol_name_cmd)
        log.info(f"For pvc {nfs_pvc_obj.name} volume name is, {vol_name}")
        fetch_pv_share_cmd = (
            "get pv "
            + vol_name
            + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
        )
        share_details = self.pv_obj.exec_oc_cmd(fetch_pv_share_cmd)
        log.info(f"Share details is, {share_details}")

        file_name = pod_obj.name
        # Run IO
        pod_obj.run_io(
            storage_type="fs",
            size="4G",
            fio_filename=file_name,
            runtime=60,
        )
        log.info("IO started on all pods")

        # Wait for IO completion
        fio_result = pod_obj.get_fio_results()
        log.info("IO completed on all pods")
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )
        # Verify presence of the file
        file_path = pod.get_file_path(pod_obj, file_name)
        log.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        log.info(f"File {file_name} exists in {pod_obj.name}")

        # Create /var/lib/www/html/index.html file inside the pod
        command = (
            "bash -c "
            + '"echo '
            + "'hello world'"
            + '  > /var/lib/www/html/index.html"'
        )
        pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )

        # Connect the external client using the share path and ingress address
        export_nfs_external_cmd = (
            "sudo mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + share_details
            + " "
            + self.test_folder
        )

        result = retry(
            (CommandFailed),
            tries=200,
            delay=10,
        )(utils.exec_cmd(cmd=export_nfs_external_cmd))
        assert result.returncode == 0

        # Verify able to read exported volume
        command = f"cat {self.test_folder}/index.html"
        result = utils.exec_cmd(cmd=command)
        stdout = result.stdout.decode().rstrip()
        log.info(stdout)
        assert stdout == "hello world"

        command = f"sudo chmod 666 {self.test_folder}/index.html"
        result = utils.exec_cmd(cmd=command)
        assert result.returncode == 0

        # Verify able to write to the exported volume
        command = (
            "bash -c "
            + '"echo '
            + "'test_writing'"
            + f'  >> {self.test_folder}/index.html"'
        )
        result = utils.exec_cmd(cmd=command)
        assert result.returncode == 0

        command = f"cat {self.test_folder}/index.html"
        result = utils.exec_cmd(cmd=command)
        stdout = result.stdout.decode().rstrip()
        log.info(stdout)
        assert stdout == "hello world" + """\n""" + "test_writing"

        # Able to read updated /var/lib/www/html/index.html file from inside the pod
        command = "bash -c " + '"cat ' + ' /var/lib/www/html/index.html"'
        result = pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        assert result.rstrip() == "hello world" + """\n""" + "test_writing"

        # unmount
        result = retry(
            (CommandFailed),
            tries=300,
            delay=10,
        )(utils.exec_cmd(cmd="sudo umount -l " + self.test_folder))
        assert result.returncode == 0

        # Deletion of Pods and PVCs
        log.info(f"Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

    def test_multiple_nfs_based_PVs(
        self,
        pod_factory,
    ):
        """
        This test is to validate creation of multiple NFS based PVs and verify the creation of
        NFS exports in NFS ganesha server

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create pods with nfs pvcs mounted
        3:- Fetch sharing details for the nfs pvc
        4:- Run IO
        5:- Wait for IO completion
        6:- Verify presence of the file
        7:- Create /var/lib/www/html/index.html file inside the pod
        8:- Connect the external client using the share path and ingress address
        9:- Verify able to access exported volume
        10:- unmount
        11:- Deletion of Pods and PVCs

        """

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_objs, yaml_creation_dir = helpers.create_multiple_pvcs(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            number_of_pvc=2,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
        )

        for pvc_obj in nfs_pvc_objs:
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
            pvc_obj.reload()

            #  Create nginx pod with nfs pvcs mounted (incluster export)
            pod_obj = pod_factory(
                interface=constants.CEPHFILESYSTEM,
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
            )
            # Fetch sharing details for the nfs pvc
            fetch_vol_name_cmd = (
                "get pvc " + pvc_obj.name + " --output jsonpath='{.spec.volumeName}'"
            )
            vol_name = self.pvc_obj.exec_oc_cmd(fetch_vol_name_cmd)
            log.info(f"For pvc {pvc_obj.name} volume name is, {vol_name}")
            fetch_pv_share_cmd = (
                "get pv "
                + vol_name
                + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
            )
            share_details = self.pv_obj.exec_oc_cmd(fetch_pv_share_cmd)
            log.info(f"Share details is, {share_details}")

            file_name = pod_obj.name
            # Run IO
            pod_obj.run_io(
                storage_type="fs",
                size="4G",
                fio_filename=file_name,
                runtime=60,
            )
            log.info("IO started on all pods")

            # Wait for IO completion
            fio_result = pod_obj.get_fio_results()
            log.info("IO completed on all pods")
            err_count = fio_result.get("jobs")[0].get("error")
            assert err_count == 0, (
                f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
            )
            # Verify presence of the file
            file_path = pod.get_file_path(pod_obj, file_name)
            log.info(f"Actual file path on the pod {file_path}")
            assert pod.check_file_existence(
                pod_obj, file_path
            ), f"File {file_name} doesn't exist"
            log.info(f"File {file_name} exists in {pod_obj.name}")

            # Create /var/lib/www/html/index.html file inside the pod
            command = (
                "bash -c "
                + '"echo '
                + "'hello world'"
                + '  > /var/lib/www/html/index.html"'
            )
            pod_obj.exec_cmd_on_pod(
                command=command,
                out_yaml_format=False,
            )

            # Connect the external client using the share path and ingress address
            export_nfs_external_cmd = (
                "sudo mount -t nfs4 -o proto=tcp "
                + self.hostname_add
                + ":"
                + share_details
                + " "
                + self.test_folder
            )

            result = retry(
                (CommandFailed),
                tries=200,
                delay=10,
            )(utils.exec_cmd(cmd=export_nfs_external_cmd))
            assert result.returncode == 0

            # Verify able to access exported volume
            command = f"cat {self.test_folder}/index.html"
            result = utils.exec_cmd(cmd=command)
            stdout = result.stdout.decode().rstrip()
            log.info(stdout)
            assert stdout == "hello world"

            # unmount
            result = utils.exec_cmd(cmd="sudo umount -l " + self.test_folder)
            assert result.returncode == 0

            # Deletion of Pods and PVCs
            log.info(f"Deleting pods")
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 180
            ), f"Pod {pod_obj.name} is not deleted"

        log.info("Deleting PVCs")
        for pvc_obj in nfs_pvc_objs:
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(
                resource_name=pvc_obj.name
            ), f"PVC {pvc_obj.name} is not deleted"
            log.info(f"Verified: PVC {pvc_obj.name} is deleted.")

    def test_multiple_mounts_of_same_nfs_volume(
        self,
        pod_factory,
    ):
        """
        This test is to validate multiple mounts of the same NFS volume/export

        Steps:
        1:- Create nfs pvc with storageclass ocs-storagecluster-ceph-nfs
        2:- Fetch sharing details for the nfs pvc
        3:- Create multiple pods with same nfs pvc mounted
        4:- Run IO
        5:- Wait for IO completion
        6:- Verify presence of the file
        7:- Create /var/lib/www/html/index.html file inside the pod
        8:- Connect the external client using the share path and ingress address
        9:- Verify able to access exported volume
        10:- unmount
        11:- Deletion of Pods and PVCs

        """
        # Create nfs pvc with storageclass ocs-storagecluster-ceph-nfs
        pvc_objs = []
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode="Filesystem",
        )
        pvc_objs.append(nfs_pvc_obj)

        # Fetch sharing details for the nfs pvc
        fetch_vol_name_cmd = (
            "get pvc " + nfs_pvc_obj.name + " --output jsonpath='{.spec.volumeName}'"
        )
        vol_name = self.pvc_obj.exec_oc_cmd(fetch_vol_name_cmd)
        log.info(f"For pvc {nfs_pvc_obj.name} volume name is, {vol_name}")
        fetch_pv_share_cmd = (
            "get pv "
            + vol_name
            + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
        )
        share_details = self.pv_obj.exec_oc_cmd(fetch_pv_share_cmd)
        log.info(f"Share details is, {share_details}")

        #  Create multiple pods with same nfs pvc mounted
        pod_objs = helpers.create_pods(
            pvc_objs,
            pod_factory,
            constants.CEPHFILESYSTEM,
            2,
            status=constants.STATUS_RUNNING,
        )

        for pod_obj in pod_objs:
            pod_names = []
            # Create /var/lib/www/html/shared_file.html file inside the pod
            command = (
                "bash -c "
                + '"echo '
                + f"'I am pod, {pod_obj.name}'"
                + '  >> /var/lib/www/html/shared_file.html"'
            )
            result = pod_obj.exec_cmd_on_pod(
                command=command,
                out_yaml_format=False,
            )
            log.info(result)
            pod_names.append(pod_obj.name)

        for pod_obj in pod_objs:
            command = f"cat /var/lib/www/html/shared_file.html"
            result = pod_obj.exec_cmd_on_pod(
                command=command,
                out_yaml_format=False,
            )
            log.info(result)
            for pod_name in pod_names:
                assert_str = f"I am pod, {pod_name}"
                assert assert_str in result

        # Connect the external client using the share path and ingress address
        export_nfs_external_cmd = (
            "sudo mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + share_details
            + " "
            + self.test_folder
        )

        result = retry(
            (CommandFailed),
            tries=200,
            delay=10,
        )(utils.exec_cmd(cmd=export_nfs_external_cmd))
        assert result.returncode == 0

        # Verify able to access exported volume
        command = f"cat {self.test_folder}/shared_file.html"
        result = utils.exec_cmd(cmd=command)
        stdout = result.stdout.decode().rstrip()
        for pod_name in pod_names:
            assert_str = f"I am pod, {pod_name}"
            assert assert_str in stdout

        # unmount
        result = utils.exec_cmd(cmd="sudo umount -l " + self.test_folder)
        assert result.returncode == 0

        # Deletion of Pods and PVCs
        log.info(f"Deleting pods")
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 180
            ), f"Pod {pod_obj.name} is not deleted"

        log.info("Deleting PVCs")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

    def test_external_nfs_client_can_write_read_new_file(
        self,
        pod_factory,
    ):
        """
        This test is to validate external client can write and read back a new file,
        and the pods can read the external client's written content.

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create nginx pod with nfs pvcs mounted
        3:- Fetch sharing details for the nfs pvc
        4:- Run IO
        5:- Wait for IO completion
        6:- Verify presence of the file
        7:- Connect the external client using the share path and ingress address
        8:- Verify able to write new file in exported volume by external client
        9:- Able to read the external client's written content from inside the pod
        10:- unmount
        11:- Deletion of Pods and PVCs

        """

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode="Filesystem",
        )

        # Create nginx pod with nfs pvcs mounted
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        # Fetch sharing details for the nfs pvc
        fetch_vol_name_cmd = (
            "get pvc " + nfs_pvc_obj.name + " --output jsonpath='{.spec.volumeName}'"
        )
        vol_name = self.pvc_obj.exec_oc_cmd(fetch_vol_name_cmd)
        log.info(f"For pvc {nfs_pvc_obj.name} volume name is, {vol_name}")
        fetch_pv_share_cmd = (
            "get pv "
            + vol_name
            + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
        )
        share_details = self.pv_obj.exec_oc_cmd(fetch_pv_share_cmd)
        log.info(f"Share details is, {share_details}")

        file_name = pod_obj.name
        # Run IO
        pod_obj.run_io(
            storage_type="fs",
            size="4G",
            fio_filename=file_name,
            runtime=60,
        )
        log.info("IO started on all pods")

        # Wait for IO completion
        fio_result = pod_obj.get_fio_results()
        log.info("IO completed on all pods")
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )
        # Verify presence of the file
        file_path = pod.get_file_path(pod_obj, file_name)
        log.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        log.info(f"File {file_name} exists in {pod_obj.name}")

        # Connect the external client using the share path and ingress address
        export_nfs_external_cmd = (
            "sudo mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + share_details
            + " "
            + self.test_folder
        )

        result = retry(
            (CommandFailed),
            tries=200,
            delay=10,
        )(utils.exec_cmd(cmd=export_nfs_external_cmd))
        assert result.returncode == 0

        # Verify able to write new file in exported volume by external client
        command = (
            "bash -c "
            + '"echo '
            + "'written from external client'"
            + f'  > {self.test_folder}/test.html"'
        )
        result = utils.exec_cmd(cmd=command)
        assert result.returncode == 0

        command = f"sudo chmod 666 {self.test_folder}/test.html"
        result = utils.exec_cmd(cmd=command)
        assert result.returncode == 0

        # Able to read the external client's written content from inside the pod
        command = "bash -c " + '"cat ' + ' /var/lib/www/html/test.html"'
        result = pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        assert result.rstrip() == "written from external client"

        # unmount
        result = retry(
            (CommandFailed),
            tries=300,
            delay=10,
        )(utils.exec_cmd(cmd="sudo umount -l " + self.test_folder))
        assert result.returncode == 0

        # Deletion of Pods and PVCs
        log.info(f"Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")
