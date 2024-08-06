import pytest
import logging
import time
import os
import socket


from ocs_ci.utility import nfs_utils
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.framework import config
from ocs_ci.utility.connection import Connection
from ocs_ci.ocs import constants, ocp
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier4c,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_hci_provider_and_client,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    polarion_id,
    aws_platform_required,
    skipif_external_mode,
)

from ocs_ci.ocs.resources import pod, ocs
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ConfigurationError


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@brown_squad
@tier1
@skipif_ocs_version("<4.11")
@skipif_ocp_version("<4.11")
@skipif_managed_service
@skipif_hci_provider_and_client
@skipif_disconnected_cluster
@skipif_proxy_cluster
@polarion_id("OCS-4270")
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
            log.info("No resources found in openshift-storage namespace.")
        else:
            log.error("nfs feature is enabled by default")


@brown_squad
@skipif_external_mode
@skipif_ocs_version("<4.11")
@skipif_ocp_version("<4.11")
@skipif_managed_service
@skipif_hci_provider_and_client
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

        Pre-Req:
        By default in our jenkins jobs we are creating one of our custom config file,
        so we can make sure
        ENV_DATA:
            nfs_client_ip: "10.xx.xxx.xx"
            nfs_client_user: "root"
            nfs_client_private_key: constants.SSH_PRIV_KEY
        these values are provided in all our automation runs in Jenkins.

        But if someone will run locally, they will need to create custom config file and provide that via
        --ocsci-conf in order to run the external nfs consume tests. Example:
        ENV_DATA:
            nfs_client_ip: "10.xx.xxx.xx"
            nfs_client_user: "root"
            nfs_client_private_key: "<path to ssh private key>"

        If this VM IP is not available in config, then the external nfs consume tests will be skipped.

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
        platform = config.ENV_DATA.get("platform", "").lower()
        self.run_id = config.RUN.get("run_id")
        self.test_folder = f"mnt/test_nfs_{self.run_id}"
        log.info(f"nfs mount point out of cluster is----- {self.test_folder}")
        self.nfs_client_ip = config.ENV_DATA.get("nfs_client_ip")
        log.info(f"nfs_client_ip is: {self.nfs_client_ip}")

        self.nfs_client_user = config.ENV_DATA.get("nfs_client_user")
        log.info(f"nfs_client_user is: {self.nfs_client_user}")

        self.nfs_client_private_key = os.path.expanduser(
            config.ENV_DATA.get("nfs_client_private_key")
            or config.DEPLOYMENT["ssh_key_private"]
        )

        # Enable nfs feature
        log.info("----Enable nfs----")
        nfs_ganesha_pod_name = nfs_utils.nfs_enable(
            self.storage_cluster_obj,
            self.config_map_obj,
            self.pod_obj,
            self.namespace,
        )

        if platform == constants.AWS_PLATFORM:
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
        if platform == constants.AWS_PLATFORM:
            # Delete ocs nfs Service
            nfs_utils.delete_nfs_load_balancer_service(
                self.storage_cluster_obj,
            )

    def teardown(self):
        """
        Check if any nfs idle mount is available out of cluster
        and remove those.
        """
        if self.con:
            retcode, stdout, _ = self.con.exec_cmd(
                "findmnt -t nfs4 " + self.test_folder
            )
            if stdout:
                log.info("unmounting existing nfs mount")
                nfs_utils.unmount(self.con, self.test_folder)
            log.info("Delete mount point")
            _, _, _ = self.con.exec_cmd("rm -rf " + self.test_folder)

    # the NFS Client VM might not be healthy, so rebooting it and re-trying
    @property
    @retry((TimeoutError, socket.gaierror), tries=3, delay=60, backoff=1)
    def con(self):
        """
        Create connection to NFS Client VM, if not accessible, try to restart it.
        """
        if (
            not hasattr(self, "__nfs_client_connection")
            or not self.__nfs_client_connection
        ):
            try:
                self.__nfs_client_connection = self.get_nfs_client_connection(
                    re_try=False
                )
            except (TimeoutError, socket.gaierror):
                nfs_client_vm_cloud = config.ENV_DATA.get("nfs_client_vm_cloud")
                nfs_client_vm_name = config.ENV_DATA.get("nfs_client_vm_name")
                if not nfs_client_vm_cloud or not nfs_client_vm_name:
                    raise ConfigurationError(
                        "NFS Client VM is not accessible and ENV_DATA nfs_client_vm_cloud and/or nfs_client_vm_name "
                        "parameters are not configured to be able to automatically reboot the NFS Client VM."
                    )
                cmd = f"openstack --os-cloud {nfs_client_vm_cloud} server reboot --hard --wait {nfs_client_vm_name}"
                exec_cmd(cmd)

                time.sleep(60)
                self.__nfs_client_connection = self.get_nfs_client_connection()
        return self.__nfs_client_connection

    def get_nfs_client_connection(self, re_try=True):
        """
        Create connection to NFS Client VM.
        """
        log.info("Connecting to nfs client test VM")
        tries = 3 if re_try else 1

        @retry((TimeoutError, socket.gaierror), tries=tries, delay=60, backoff=1)
        def __make_connection():
            return Connection(
                self.nfs_client_ip,
                self.nfs_client_user,
                private_key=self.nfs_client_private_key,
            )

        return __make_connection()

    @tier1
    @polarion_id("OCS-4269")
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

    @tier1
    @polarion_id("OCS-4272")
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
        log.info("Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        pv_obj = nfs_pvc_obj.backed_pv_obj
        log.info(f"pv object-----{pv_obj}")

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

        log.info("Check nfs pv is deleted")
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    @tier1
    @aws_platform_required
    @polarion_id("OCS-4273")
    def test_outcluster_nfs_export(
        self,
        pod_factory,
    ):
        """
        This test is to validate export where the export is consumed from outside the Openshift cluster
        - Create a LoadBalancer Service pointing to the CephNFS server
        - Direct external NFS clients to the Service endpoint from the step above

        Prerequisites:
            On the client host machine openshift-dev pub key in authorized_keys should be available
            and nfs-utils package should be installed.

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
        nfs_utils.skip_test_if_nfs_client_unavailable(self.nfs_client_ip)

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
            size="4Gi",
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

        retcode, _, _ = self.con.exec_cmd("mkdir -p " + self.test_folder)
        assert retcode == 0
        export_nfs_external_cmd = (
            "mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + share_details
            + " "
            + self.test_folder
        )

        retry(
            (CommandFailed),
            tries=200,
            delay=10,
        )(self.con.exec_cmd(export_nfs_external_cmd))

        # Verify able to read exported volume
        command = f"cat {self.test_folder}/index.html"
        retcode, stdout, _ = self.con.exec_cmd(command)
        stdout = stdout.rstrip()
        log.info(stdout)
        assert stdout == "hello world"

        command = f"chmod 666 {self.test_folder}/index.html"
        retcode, _, _ = self.con.exec_cmd(command)
        assert retcode == 0

        # Verify able to write to the exported volume
        command = (
            "bash -c "
            + '"echo '
            + "'test_writing'"
            + f'  >> {self.test_folder}/index.html"'
        )
        retcode, _, stderr = self.con.exec_cmd(command)
        assert retcode == 0, f"failed with error---{stderr}"

        command = f"cat {self.test_folder}/index.html"
        retcode, stdout, _ = self.con.exec_cmd(command)
        assert retcode == 0
        stdout = stdout.rstrip()
        assert stdout == "hello world" + """\n""" + "test_writing"

        # Able to read updated /var/lib/www/html/index.html file from inside the pod
        command = "bash -c " + '"cat ' + ' /var/lib/www/html/index.html"'
        result = pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        assert result.rstrip() == "hello world" + """\n""" + "test_writing"

        # Unmount
        nfs_utils.unmount(self.con, self.test_folder)

        # Deletion of Pods and PVCs
        log.info("Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        pv_obj = nfs_pvc_obj.backed_pv_obj
        log.info(f"pv object-----{pv_obj}")

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

        log.info("Check nfs pv is deleted")
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    @tier1
    @aws_platform_required
    @polarion_id("OCS-4274")
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
        nfs_utils.skip_test_if_nfs_client_unavailable(self.nfs_client_ip)

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
            retcode, _, _ = self.con.exec_cmd("mkdir -p " + self.test_folder)
            assert retcode == 0
            export_nfs_external_cmd = (
                "mount -t nfs4 -o proto=tcp "
                + self.hostname_add
                + ":"
                + share_details
                + " "
                + self.test_folder
            )
            retry(
                (CommandFailed),
                tries=200,
                delay=10,
            )(self.con.exec_cmd(export_nfs_external_cmd))

            # Verify able to access exported volume
            command = f"cat {self.test_folder}/index.html"
            retcode, stdout, _ = self.con.exec_cmd(command)
            stdout = stdout.rstrip()
            log.info(stdout)
            assert stdout == "hello world"

            # Unmount
            nfs_utils.unmount(self.con, self.test_folder)

            # Deletion of Pods and PVCs
            log.info("Deleting pods")
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 180
            ), f"Pod {pod_obj.name} is not deleted"

            pv_obj = pvc_obj.backed_pv_obj
            log.info(f"pv object-----{pv_obj}")
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(
                resource_name=pvc_obj.name
            ), f"PVC {pvc_obj.name} is not deleted"
            log.info(f"Verified: PVC {pvc_obj.name} is deleted.")

            log.info("Check nfs pv is deleted")
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    @tier1
    @aws_platform_required
    @polarion_id("OCS-4293")
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
        nfs_utils.skip_test_if_nfs_client_unavailable(self.nfs_client_ip)

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
            command = "cat /var/lib/www/html/shared_file.html"
            result = pod_obj.exec_cmd_on_pod(
                command=command,
                out_yaml_format=False,
            )
            log.info(result)
            for pod_name in pod_names:
                assert_str = f"I am pod, {pod_name}"
                assert assert_str in result

        # Connect the external client using the share path and ingress address
        retcode, _, _ = self.con.exec_cmd("mkdir -p " + self.test_folder)
        assert retcode == 0
        export_nfs_external_cmd = (
            "mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + share_details
            + " "
            + self.test_folder
        )
        retry(
            (CommandFailed),
            tries=200,
            delay=10,
        )(self.con.exec_cmd(export_nfs_external_cmd))

        # Verify able to access exported volume
        command = f"cat {self.test_folder}/shared_file.html"
        retcode, stdout, _ = self.con.exec_cmd(command)
        stdout = stdout.rstrip()
        log.info(stdout)
        for pod_name in pod_names:
            assert_str = f"I am pod, {pod_name}"
            assert assert_str in stdout

        # Unmount
        nfs_utils.unmount(self.con, self.test_folder)

        # Deletion of Pods and PVCs
        log.info("Deleting pods")
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 180
            ), f"Pod {pod_obj.name} is not deleted"

        pv_obj = nfs_pvc_obj.backed_pv_obj
        log.info(f"pv object-----{pv_obj}")

        log.info("Deleting PVCs")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

        log.info("Check nfs pv is deleted")
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    @tier1
    @aws_platform_required
    @polarion_id("OCS-4312")
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
        nfs_utils.skip_test_if_nfs_client_unavailable(self.nfs_client_ip)

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
        retcode, _, _ = self.con.exec_cmd("mkdir -p " + self.test_folder)
        assert retcode == 0
        export_nfs_external_cmd = (
            "mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + share_details
            + " "
            + self.test_folder
        )
        retry(
            (CommandFailed),
            tries=200,
            delay=10,
        )(self.con.exec_cmd(export_nfs_external_cmd))

        # Verify able to write new file in exported volume by external client
        command = (
            "bash -c "
            + '"echo '
            + "'written from external client'"
            + f'  > {self.test_folder}/test.html"'
        )
        retcode, _, _ = self.con.exec_cmd(command)
        assert retcode == 0

        command = f"sudo chmod 666 {self.test_folder}/test.html"
        retcode, _, _ = self.con.exec_cmd(command)
        assert retcode == 0

        # Able to read the external client's written content from inside the pod
        command = "bash -c " + '"cat ' + ' /var/lib/www/html/test.html"'
        result = pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        assert result.rstrip() == "written from external client"

        # Unmount
        nfs_utils.unmount(self.con, self.test_folder)

        # Deletion of Pods and PVCs
        log.info("Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        pv_obj = nfs_pvc_obj.backed_pv_obj
        log.info(f"pv object-----{pv_obj}")

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

        log.info("Check nfs pv is deleted")
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    @tier1
    @polarion_id("OCS-4275")
    def test_nfs_volume_with_different_accesss_mode(
        self,
        pod_factory,
    ):
        """
        This test is to validate NFS volumes with different access modes

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs with different
        access modes
        2:- Create pods with the nfs pvcs mounted
        3:- Run IO
        4:- Wait for IO completion
        5:- Verify presence of the file
        6:- Deletion of Pods and PVCs

        """
        access_modes = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]

        for access_mode in access_modes:
            # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
            nfs_pvc_obj = helpers.create_pvc(
                sc_name=self.nfs_sc,
                namespace=self.namespace,
                size="5Gi",
                do_reload=True,
                access_mode=access_mode,
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
            log.info("Deleting pod")
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 180
            ), f"Pod {pod_obj.name} is not deleted"

            pv_obj = nfs_pvc_obj.backed_pv_obj
            log.info(f"pv object-----{pv_obj}")

            log.info("Deleting PVC")
            nfs_pvc_obj.delete()
            nfs_pvc_obj.ocp.wait_for_delete(
                resource_name=nfs_pvc_obj.name
            ), f"PVC {nfs_pvc_obj.name} is not deleted"
            log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

            log.info("Check nfs pv is deleted")
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    @tier4c
    @polarion_id("OCS-4284")
    def test_respin_of_nfs_plugin_pods_for_incluster_consumer(
        self,
        pod_factory,
    ):
        """
        This test is to check respin of NFS plugin pods when active I/O is running on in-cluster
        consumer on the same node

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create pods with nfs pvcs mounted
        3:- Run IO
        4:- Respin nfsplugin pods while active I/O on in-cluster consumer
        5:- Wait for IO completion
        6:- Verify presence of the file
        7:- Deletion of Pods and PVCs

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

        # Respin nfsplugin pods while active I/O on in-cluster consumer
        nfsplugin_pod_objs = pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"], selector=["csi-nfsplugin"]
        )
        log.info(f"nfs plugin pods-----{nfsplugin_pod_objs}")
        pod.delete_pods(pod_objs=nfsplugin_pod_objs)

        # Wait untill nfsplugin pods recovery
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin",
            resource_count=len(nfsplugin_pod_objs),
            timeout=3600,
            sleep=5,
        )
        log.info("All nfsplugin pods are up and running")

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
        log.info("Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        pv_obj = nfs_pvc_obj.backed_pv_obj
        log.info(f"pv object-----{pv_obj}")

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

        log.info("Check nfs pv is deleted")
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    @tier4c
    @polarion_id("OCS-4296")
    def test_respin_app_pod_exported_nfs_volume_incluster(
        self,
    ):
        """
        This test is to test respin of the app pod using the NFS volume/export in in-cluster consumer scenario

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create deployment for app pod
        3:- Run IO
        4:- Wait for IO completion
        5:- Verify presence of the file
        6:- Create /mnt/test file inside the pod
        7:- Respin the app pod
        8:- Able to read the /mnt/test file's content from inside the respined pod
        9:- Edit /mnt/test file
        10:- Able to read updated /mnt/test file
        11:- Delete deployment
        12:- Deletion of nfs PVC

        """
        try:
            # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
            nfs_pvc_obj = helpers.create_pvc(
                sc_name=self.nfs_sc,
                namespace=self.namespace,
                pvc_name="nfs-pvc",
                size="5Gi",
                do_reload=True,
                access_mode=constants.ACCESS_MODE_RWO,
                volume_mode="Filesystem",
            )

            # Create deployment for app pod
            log.info("----creating deployment ---")
            deployment_data = templating.load_yaml(constants.NFS_APP_POD_YAML)
            helpers.create_resource(**deployment_data)
            time.sleep(60)

            assert self.pod_obj.wait_for_resource(
                resource_count=1,
                condition=constants.STATUS_RUNNING,
                selector="name=nfs-test-pod",
                dont_allow_other_resources=True,
                timeout=60,
            )
            pod_objs = pod.get_all_pods(
                namespace=self.namespace,
                selector=["nfs-test-pod"],
                selector_label="name",
            )

            pod_obj = pod_objs[0]
            log.info(f"pod obj name----{pod_obj.name}")

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

            # Create /mnt/test file inside the pod
            command = "bash -c " + '"echo ' + "'Before respin'" + '  > /mnt/test"'
            pod_obj.exec_cmd_on_pod(
                command=command,
                out_yaml_format=False,
            )

            # Respin the app pod
            log.info(f"Respin pod {pod_obj.name}")
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 60
            ), f"Pod {pod_obj.name} is not deleted"

            assert self.pod_obj.wait_for_resource(
                resource_count=1,
                condition=constants.STATUS_RUNNING,
                selector="name=nfs-test-pod",
                dont_allow_other_resources=True,
                timeout=60,
            )

            respinned_pod_objs = pod.get_all_pods(
                namespace=self.namespace,
                selector=["nfs-test-pod"],
                selector_label="name",
            )

            respinned_pod_obj = respinned_pod_objs[0]
            log.info(f"pod obj name----{respinned_pod_obj.name}")

            # Able to read the /mnt/test file's content from inside the respined pod
            command = "bash -c " + '"cat ' + ' /mnt/test"'
            result = respinned_pod_obj.exec_cmd_on_pod(
                command=command,
                out_yaml_format=False,
            )
            assert result.rstrip() == "Before respin"

            # Edit /mnt/test file
            command = "bash -c " + '"echo ' + "'After respin'" + '  >> /mnt/test"'

            respinned_pod_obj.exec_cmd_on_pod(
                command=command,
                out_yaml_format=False,
            )
            # Able to read updated /mnt/test file
            command = "bash -c " + '"cat ' + ' /mnt/test"'
            result = respinned_pod_obj.exec_cmd_on_pod(
                command=command,
                out_yaml_format=False,
            )
            assert result.rstrip() == "Before respin" + """\n""" + "After respin"

        finally:
            # Delete deployment
            cmd_delete_deployment = "delete dc nfs-test-pod"
            self.storage_cluster_obj.exec_oc_cmd(cmd_delete_deployment)

            pv_obj = nfs_pvc_obj.backed_pv_obj
            log.info(f"pv object-----{pv_obj}")

            # Deletion of nfs PVC
            log.info("Deleting PVC")
            nfs_pvc_obj.delete()
            nfs_pvc_obj.ocp.wait_for_delete(
                resource_name=nfs_pvc_obj.name
            ), f"PVC {nfs_pvc_obj.name} is not deleted"
            log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

            log.info("Check nfs pv is deleted")
            pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

    @tier4c
    @polarion_id("OCS-4294")
    def test_respin_of_cephfs_plugin_provisioner_pods_for_incluster_consumer(
        self,
        pod_factory,
    ):
        """
        This test is to check respin of cephfs provisioner pod during active I/O is running on in-cluster
        consumer

        Steps:
        1:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        2:- Create pods with nfs pvcs mounted
        3:- Run IO
        4:- Respin cephfsplugin provisioner pods while active I/O on in-cluster consumer
        5:- Wait for IO completion
        6:- Verify presence of the file
        7:- Deletion of Pods and PVCs
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

        # Respin cephfsplugin provisioner pods while active I/O on in-cluster consumer
        cephfsplugin_provisioner_pod_objs = pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=["csi-cephfsplugin-provisioner"],
        )
        log.info(
            f"cephfs plugin provisioner pods-----{cephfsplugin_provisioner_pod_objs}"
        )
        pod.delete_pods(pod_objs=cephfsplugin_provisioner_pod_objs)

        # Wait untill cephfsplugin provisioner pods recovery
        self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=csi-cephfsplugin-provisioner",
            resource_count=len(cephfsplugin_provisioner_pod_objs),
            timeout=3600,
            sleep=5,
        )
        log.info("All cephfsplugin rovisioner pods are up and running")

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
        log.info("Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        pv_obj = nfs_pvc_obj.backed_pv_obj
        log.info(f"pv object-----{pv_obj}")

        log.info("Deleting PVC")
        nfs_pvc_obj.delete()
        nfs_pvc_obj.ocp.wait_for_delete(
            resource_name=nfs_pvc_obj.name
        ), f"PVC {nfs_pvc_obj.name} is not deleted"
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

        log.info("Check nfs pv is deleted")
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)
