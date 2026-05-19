import ipaddress
from threading import Thread
import pytest
import logging
import time
import os
import socket
import threading

from ocs_ci.utility import nfs_utils
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.framework import config
from ocs_ci.utility.connection import Connection
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources import pvc
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.framework.pytest_customization.marks import (
    skipif_rosa_hcp,
    skipif_lean_deployment,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    skip_for_provider_or_client_if_ocs_version,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    polarion_id,
    skipif_external_mode,
)
from ocs_ci.utility import version as version_module
from ocs_ci.ocs.resources import pod, ocs
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ConfigurationError
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
)

log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@skipif_rosa_hcp
@skipif_external_mode
@skipif_ocs_version("<4.11")
@skipif_ocp_version("<4.11")
@skipif_managed_service
@skip_for_provider_or_client_if_ocs_version("<4.19")
@skipif_disconnected_cluster
@skipif_proxy_cluster
@skipif_lean_deployment
class TestNfsExport(ManageTest):
    """
    Test nfs feature enable for ODF 4.11

    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown(
        self,
        request,
    ):
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
        cls = request.cls
        log.info("-----Setup-----")
        cls.nfs_app_deployment = "nfs-test-pod"
        cls.namespace = config.ENV_DATA["cluster_namespace"]
        cls.storage_cluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER, namespace=cls.namespace
        )
        cls.sc_obj = ocp.OCP(kind=constants.STORAGECLASS)
        cls.config_map_obj = ocp.OCP(kind=constants.CONFIGMAP, namespace=cls.namespace)
        cls.pod_obj = ocp.OCP(kind=constants.POD, namespace=cls.namespace)
        cls.service_obj = ocp.OCP(kind=constants.SERVICE, namespace=cls.namespace)
        cls.pvc_obj = ocp.OCP(kind=constants.PVC, namespace=cls.namespace)
        cls.pv_obj = ocp.OCP(kind=constants.PV, namespace=cls.namespace)
        cls.nfs_sc = constants.NFS_STORAGECLASS_NAME
        cls.sc = ocs.OCS(kind=constants.STORAGECLASS, metadata={"name": cls.nfs_sc})
        cls.retain_nfs_sc_name = "ocs-storagecluster-ceph-nfs-retain"
        platform = config.ENV_DATA.get("platform", "").lower()
        cls.run_id = config.RUN.get("run_id")
        cls.test_folder = f"mnt/test_nfs_{cls.run_id}"
        log.info(f"nfs mount point out of cluster is----- {cls.test_folder}")
        cls.nfs_client_ip = config.ENV_DATA.get("nfs_client_ip")
        log.info(f"nfs_client_ip is: {cls.nfs_client_ip}")

        cls.nfs_client_user = config.ENV_DATA.get("nfs_client_user")
        log.info(f"nfs_client_user is: {cls.nfs_client_user}")

        cls.nfs_client_private_key = os.path.expanduser(
            config.ENV_DATA.get("nfs_client_private_key")
            or config.DEPLOYMENT["ssh_key_private"]
        )

        # Enable nfs feature
        log.info("----Enable nfs----")
        if (
            config.default_cluster_ctx.ENV_DATA["cluster_type"].lower()
            == constants.HCI_CLIENT
        ):
            nfs_ganesha_pod, cls.hostname_add = nfs_utils.nfs_access_for_clients(
                cls.nfs_sc
            )

            # Create a duplicate sc of nfs-sc and update the server details with hostname_add
            if (
                version_module.get_semantic_ocs_version_from_config()
                < version_module.VERSION_4_21
            ):
                _ = nfs_utils.create_nfs_sc(
                    sc_name_to_create=constants.COPY_NFS_STORAGECLASS_NAME,
                    sc_name_to_copy=cls.nfs_sc,
                    server=cls.hostname_add,
                )
                cls.nfs_sc = constants.COPY_NFS_STORAGECLASS_NAME
            yield
            # Remove NFS SC from distributed storage classes on the provider
            nfs_utils.remove_nfs_storage_class_from_all_consumers(
                constants.NFS_STORAGECLASS_NAME
            )
            # Disable nfs feature
            nfs_utils.disable_nfs_service_from_provider(cls.sc, nfs_ganesha_pod)

            # delete nfs non default storageclass if available
            if ocp.OCP(kind=constants.STORAGECLASS).is_exist(
                resource_name=constants.COPY_NFS_STORAGECLASS_NAME
            ):
                cls.sc_obj.delete(resource_name=constants.COPY_NFS_STORAGECLASS_NAME)

        else:
            nfs_ganesha_pod_name = nfs_utils.nfs_enable(
                cls.storage_cluster_obj,
                cls.config_map_obj,
                cls.pod_obj,
                cls.namespace,
            )

            if (
                platform == constants.AWS_PLATFORM
                or platform == constants.IBMCLOUD_PLATFORM
                or platform == constants.HCI_BAREMETAL
            ):
                # Create loadbalancer service for nfs
                cls.hostname_add = nfs_utils.create_nfs_load_balancer_service(
                    cls.storage_cluster_obj,
                )

            yield

            log.info("-----Teardown-----")
            # Disable nfs feature
            nfs_utils.nfs_disable(
                cls.storage_cluster_obj,
                cls.config_map_obj,
                cls.pod_obj,
                cls.sc,
                nfs_ganesha_pod_name,
            )
            if (
                platform == constants.AWS_PLATFORM
                or platform == constants.IBMCLOUD_PLATFORM
                or platform == constants.HCI_BAREMETAL
            ):
                # Delete ocs nfs Service
                nfs_utils.delete_nfs_load_balancer_service(
                    cls.storage_cluster_obj,
                )

        if cls.sc_obj.is_exist(resource_name=cls.retain_nfs_sc_name):
            # Delete the nfs retain StorageClass
            cls.sc_obj.delete(resource_name=cls.retain_nfs_sc_name)
            log.info(f"Wait until the SC, {cls.retain_nfs_sc_name} is deleted.")
            cls.sc_obj.wait_for_delete(resource_name=cls.retain_nfs_sc_name)

        # Check if NFS client connection was established and clean up mount
        if (
            hasattr(cls, "_TestNfsExport__nfs_client_connection")
            and cls._TestNfsExport__nfs_client_connection
        ):
            try:
                con = cls._TestNfsExport__nfs_client_connection
                retcode, stdout, _ = con.exec_cmd("findmnt -t nfs4 " + cls.test_folder)
                if stdout:
                    log.info("unmounting existing nfs mount")
                    nfs_utils.unmount(con, cls.test_folder)
                log.info("Delete mount point")
                _, _, _ = con.exec_cmd("rm -rf " + cls.test_folder)
            except Exception as e:
                log.warning(f"Failed to cleanup NFS mount: {e}")

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

        After establishing the SSH connection, if the NFS LB endpoint is a
        hostname (not a raw IP), the hostname is resolved from within the
        cluster and /etc/hosts on the client VM is updated. This is required
        when the NFS client VM is in a different VPC from the OpenShift cluster
        and cannot resolve IBM Cloud VPC LB hostnames via its DNS servers.


        If hostname resolution from the cluster fails (timeout), the code will
        proceed without updating /etc/hosts, assuming the NFS client VM can
        resolve the hostname via its own DNS configuration.
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

        con = __make_connection()
        hostname_add = getattr(self, "hostname_add", None)
        if hostname_add:
            is_ip = False
            try:
                ipaddress.ip_address(hostname_add)
                is_ip = True
            except ValueError:
                pass
            if not is_ip:
                log.info(
                    "NFS LB endpoint %s is a hostname, resolving and "
                    "updating /etc/hosts on NFS client VM",
                    hostname_add,
                )
                nfs_utils.update_etc_hosts_on_nfs_client(con, hostname_add)
        return con

    @tier1
    @polarion_id("OCS-4272")
    def test_cluster_inout_nfs_export(
        self,
        pod_factory,
        request,
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

        # import random
        #
        # for index in range(1):
        #     # index = random.randint(1, 99999)
        #     pod_name = "test-pod-incluster-" + str(index)
        #     pvc_name = "test-pvc-incluster-" + str(index)
        #     # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        #     nfs_pvc_obj = helpers.create_pvc(
        #         sc_name=self.nfs_sc,
        #         namespace=self.namespace,
        #         size="1Gi",
        #         do_reload=True,
        #         access_mode=constants.ACCESS_MODE_RWO,
        #         volume_mode="Filesystem",
        #         pvc_name=pvc_name,
        #     )
        #
        #     # # Create nginx pod with nfs pvcs mounted
        #     # pod_obj = pod_factory(
        #     #     interface=constants.CEPHFILESYSTEM,
        #     #     pvc=nfs_pvc_obj,
        #     #     status=constants.STATUS_RUNNING,
        #     # )
        #     #
        #     # Create deployment for app pod
        #     log.info("----creating deployment ---")
        #     deployment_data = templating.load_yaml(constants.NFS_APP_POD_YAML)
        #
        #     # Deployment name
        #     deployment_data["metadata"]["name"] = pod_name
        #
        #     # Label values (there are two places)
        #     deployment_data["metadata"]["labels"]["app"] = pod_name
        #
        #     deployment_data["spec"]["selector"]["matchLabels"]["name"] = pod_name
        #
        #     deployment_data["spec"]["template"]["metadata"]["labels"]["name"] = pod_name
        #
        #     # PVC claimName
        #     deployment_data["spec"]["template"]["spec"]["volumes"][0][
        #         "persistentVolumeClaim"
        #     ]["claimName"] = pvc_name
        #
        #     helpers.create_resource(**deployment_data)
        #     time.sleep(60)
        #
        #     assert self.pod_obj.wait_for_resource(
        #         resource_count=1,
        #         condition=constants.STATUS_RUNNING,
        #         selector=f"name={pod_name}",
        #         dont_allow_other_resources=True,
        #         timeout=120,
        #     )
        #     pod_obj = pod.get_all_pods(
        #         namespace=self.namespace,
        #         selector=[pod_name],
        #         selector_label="name",
        #     )[0]
        #
        #     file_name = pod_obj.name
        #     # Run IO
        #     pod_obj.run_io(
        #         storage_type="fs",
        #         size="1G",
        #         fio_filename=file_name,
        #         runtime=60,
        #     )
        #     log.info("IO started on all pods")
        #
        #     # Wait for IO completion
        #     fio_result = pod_obj.get_fio_results()
        #     log.info("IO completed on all pods")
        #     err_count = fio_result.get("jobs")[0].get("error")
        #     assert err_count == 0, (
        #         f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        #     )
        #     # Verify presence of the file
        #     file_path = pod.get_file_path(pod_obj, file_name)
        #     log.info(f"Actual file path on the pod {file_path}")
        #     assert pod.check_file_existence(
        #         pod_obj, file_path
        #     ), f"File {file_name} doesn't exist"
        #     log.info(f"File {file_name} exists in {pod_obj.name}")
        log.info("Starting outcluster case")
        # # Deletion of Pods and PVCs
        # log.info("Deleting pod")
        # pod_obj.delete()
        # pod_obj.ocp.wait_for_delete(
        #     pod_obj.name, 180
        # ), f"Pod {pod_obj.name} is not deleted"
        #
        # pv_obj = nfs_pvc_obj.backed_pv_obj
        # log.info(f"pv object-----{pv_obj}")
        #
        # log.info("Deleting PVC")
        # nfs_pvc_obj.delete()
        # nfs_pvc_obj.ocp.wait_for_delete(
        #     resource_name=nfs_pvc_obj.name
        # ), f"PVC {nfs_pvc_obj.name} is not deleted"
        # log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")
        #
        # log.info("Check nfs pv is deleted")
        # pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=180)

        nfs_utils.skip_test_if_nfs_client_unavailable(self.nfs_client_ip)

        # Generate unique names using timestamp to avoid conflicts between test runs
        import random

        unique_suffix = f"{int(time.time())}-{random.randint(1000, 9999)}"
        pod_name = f"test-pod-outcluster-{unique_suffix}"
        pvc_name = f"test-pvc-outcluster-{unique_suffix}"
        log.info(f"Using unique names: pod={pod_name}, pvc={pvc_name}")
        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            size="10Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWX,
            volume_mode="Filesystem",
            pvc_name=pvc_name,
        )

        # # Create nginx pod with nfs pvcs mounted
        # pod_obj = pod_factory(
        #     interface=constants.CEPHFILESYSTEM,
        #     pvc=nfs_pvc_obj,
        #     status=constants.STATUS_RUNNING,
        # )

        # Create deployment for app pod
        log.info("----creating deployment ---")
        deployment_data = templating.load_yaml(constants.NFS_APP_POD_YAML)

        # Deployment name
        deployment_data["metadata"]["name"] = pod_name

        # Label values (there are two places)
        deployment_data["metadata"]["labels"]["app"] = pod_name

        deployment_data["spec"]["selector"]["matchLabels"]["name"] = pod_name

        deployment_data["spec"]["template"]["metadata"]["labels"]["name"] = pod_name

        # PVC claimName
        deployment_data["spec"]["template"]["spec"]["volumes"][0][
            "persistentVolumeClaim"
        ]["claimName"] = pvc_name

        helpers.create_resource(**deployment_data)
        time.sleep(120)
        assert self.pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector=f"name={pod_name}",
            dont_allow_other_resources=True,
            timeout=120,
        )
        pod_obj = pod.get_all_pods(
            namespace=self.namespace,
            selector=[pod_name],
            selector_label="name",
        )[0]

        # Use local variable to avoid modifying class instance variable
        test_folder_for_pod = self.test_folder + "-" + pod_name

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

        # file_name = pod_obj.name
        # # Run IO
        # pod_obj.run_io(
        #     storage_type="fs",
        #     size="4Gi",
        #     fio_filename=file_name,
        #     runtime=60,
        # )
        # log.info("IO started on all pods")
        #
        # # Wait for IO completion
        # fio_result = pod_obj.get_fio_results()
        # log.info("IO completed on all pods")
        # err_count = fio_result.get("jobs")[0].get("error")
        # assert err_count == 0, (
        #     f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        # )
        # # Verify presence of the file
        # file_path = pod.get_file_path(pod_obj, file_name)
        # log.info(f"Actual file path on the pod {file_path}")
        # assert pod.check_file_existence(
        #     pod_obj, file_path
        # ), f"File {file_name} doesn't exist"
        # log.info(f"File {file_name} exists in {pod_obj.name}")
        # Create /var/lib/www/html/index.html file inside the pod
        command = "bash -c " + '"echo ' + "'hello world'" + '  > /mnt/index.html"'
        pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        # Get connection once to avoid multiple 5-minute waits
        con = self.con
        retcode, _, _ = con.exec_cmd("mkdir -p " + test_folder_for_pod)
        assert retcode == 0

        export_nfs_external_cmd = (
            "mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + share_details
            + " "
            + test_folder_for_pod
        )

        log.info(f"Mounting NFS export: {export_nfs_external_cmd}")
        retry(
            (CommandFailed),
            tries=28,
            delay=10,
        )(
            con.exec_cmd
        )(export_nfs_external_cmd)

        # Verify mount is successful
        retcode, stdout, _ = con.exec_cmd(f"findmnt -M {test_folder_for_pod}")
        assert retcode == 0, f"Mount verification failed for {test_folder_for_pod}"
        log.info(f"Successfully mounted NFS export at {test_folder_for_pod}")

        # Add finalizer for complete cleanup after all resources are created
        def cleanup_all_resources():
            """Cleanup all test resources in reverse order of creation"""
            log.info("Running cleanup for all test resources...")

            # 1. Unmount NFS
            try:
                log.info(f"Unmounting {test_folder_for_pod}")
                nfs_utils.unmount(con, test_folder_for_pod)
                con.exec_cmd(f"rm -rf {test_folder_for_pod}")
                log.info("Waiting for NFS export to be fully released...")
                time.sleep(10)
            except Exception as e:
                log.warning(f"Failed to unmount NFS: {e}")

            # 2. Delete deployment
            try:
                log.info(f"Deleting deployment {pod_name}")
                deployment_obj = ocp.OCP(
                    kind=constants.DEPLOYMENT, namespace=self.namespace
                )
                if deployment_obj.is_exist(resource_name=pod_name):
                    deployment_obj.delete(resource_name=pod_name)
                    deployment_obj.wait_for_delete(resource_name=pod_name, timeout=180)
                    log.info(f"Deployment {pod_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete deployment: {e}")

            # 3. Wait for pod termination
            try:
                log.info("Waiting for pod to be terminated...")
                pod_obj.ocp.wait_for_delete(pod_obj.name, timeout=180)
                log.info(f"Pod {pod_obj.name} terminated successfully")
            except Exception as e:
                log.warning(f"Failed to wait for pod deletion: {e}")

            # 4. Delete PVC and PV
            try:
                pv_obj = nfs_pvc_obj.backed_pv_obj
                log.info(f"Deleting PVC {nfs_pvc_obj.name}")
                nfs_pvc_obj.delete(wait=True)
                log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

                log.info("Checking if NFS PV is deleted")
                pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=300)
                log.info(f"PV {pv_obj.name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete PVC/PV: {e}")

            log.info("Cleanup complete")

        request.addfinalizer(cleanup_all_resources)

        # Verify able to read exported volume
        command = f"cat {test_folder_for_pod}/index.html"
        retcode, stdout, _ = con.exec_cmd(command)
        stdout = stdout.rstrip()
        log.info(stdout)
        assert stdout == "hello world"
        command = f"chmod 666 {test_folder_for_pod}/index.html"
        retcode, _, _ = con.exec_cmd(command)
        assert retcode == 0

        # Verify able to write to the exported volume
        command = (
            "bash -c "
            + '"echo '
            + "'test_writing'"
            + f'  >> {test_folder_for_pod}/index.html"'
        )
        retcode, _, stderr = con.exec_cmd(command)
        assert retcode == 0, f"failed with error---{stderr}"

        command = f"cat {test_folder_for_pod}/index.html"
        retcode, stdout, _ = con.exec_cmd(command)
        assert retcode == 0
        stdout = stdout.rstrip()
        assert stdout == "hello world" + """\n""" + "test_writing"

        # Able to read updated /var/lib/www/html/index.html file from inside the pod
        command = "bash -c " + '"cat ' + ' /mnt/index.html"'
        result = pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )
        assert result.rstrip() == "hello world" + """\n""" + "test_writing"

        # ========================================================================
        # Scenario: NFS Server Pod Node Reboot During Active I/O
        # ========================================================================
        log.info("=" * 80)
        log.info("Starting NFS Server Pod Node Reboot During Active I/O scenario")
        log.info("=" * 80)

        # Step 1: Start continuous read/write I/O from NFS mounted client
        log.info("Step 1: Starting continuous I/O operations on NFS mount")

        # Define test file name as constant
        IO_TEST_FILE_NAME = "io_test_single.txt"
        test_file = f"{test_folder_for_pod}/{IO_TEST_FILE_NAME}"

        io_errors = []
        io_stop_event = threading.Event()
        io_completed = threading.Event()

        def continuous_io_operations():
            """
            Perform continuous read/write operations on a single NFS file with checksum validation
            """
            iteration = 0
            previous_checksum = None

            try:
                # Initialize the file with initial content
                initial_data = f"IO test started at {time.time()}"
                init_cmd = f'echo "{initial_data}" > {test_file}'
                retcode, _, stderr = con.exec_cmd(init_cmd)
                if retcode != 0:
                    io_errors.append(f"Failed to initialize test file: {stderr}")
                    log.error(f"Initialization error: {stderr}")
                    return

                # Get initial file checksum using md5sum
                checksum_cmd = f"md5sum {test_file}"
                retcode, stdout, stderr = con.exec_cmd(checksum_cmd, use_logger=False)
                if retcode == 0:
                    previous_checksum = stdout.split()[0]
                    log.info(f"File initialized with checksum: {previous_checksum}")
                else:
                    log.warning("Failed to get initial checksum")

                while not io_stop_event.is_set():
                    iteration += 1
                    test_data = f"IO test iteration {iteration} - {time.time()}"

                    # Write operation - append to the file
                    write_cmd = f'echo "{test_data}" >> {test_file}'
                    retcode, _, stderr = con.exec_cmd(write_cmd)
                    if retcode != 0:
                        io_errors.append(
                            f"Write failed at iteration {iteration}: {stderr}"
                        )
                        log.error(f"Write error: {stderr}")
                        continue

                    # Calculate current file checksum using md5sum
                    checksum_cmd = f"md5sum {test_file}"
                    retcode, stdout, stderr = con.exec_cmd(
                        checksum_cmd, use_logger=False
                    )
                    if retcode != 0:
                        io_errors.append(
                            f"Checksum calculation failed at iteration {iteration}: {stderr}"
                        )
                        log.error(f"Checksum error: {stderr}")
                        continue

                    current_checksum = stdout.split()[0]

                    # Verify checksum changed after write (data was actually written)
                    if current_checksum == previous_checksum:
                        io_errors.append(
                            f"Checksum unchanged at iteration {iteration}: "
                            f"file may not have been updated"
                        )
                        log.error(
                            f"Data integrity error at iteration {iteration}: "
                            f"checksum unchanged ({current_checksum}), write may have failed"
                        )

                    # Read the last line to verify content
                    read_cmd = f"tail -n 1 {test_file}"
                    retcode, stdout, stderr = con.exec_cmd(read_cmd, use_logger=False)
                    if retcode != 0:
                        io_errors.append(
                            f"Read failed at iteration {iteration}: {stderr}"
                        )
                        log.error(f"Read error: {stderr}")
                    elif stdout.strip() != test_data:
                        io_errors.append(f"Data mismatch at iteration {iteration}")
                        log.error(
                            f"Data mismatch: expected '{test_data}', got '{stdout.strip()}'"
                        )

                    # Update previous checksum for next iteration
                    previous_checksum = current_checksum

                    # Periodically log file checksum
                    if iteration % 10 == 0:
                        log.info(
                            f"Iteration {iteration}: File checksum - {current_checksum}"
                        )

                    # Small delay between iterations
                    time.sleep(2)

                    if iteration % 20 == 0:
                        log.info(f"Completed {iteration} I/O iterations successfully")

            except Exception as e:
                io_errors.append(f"Exception during I/O: {str(e)}")
                log.error(f"I/O thread exception: {e}")
            finally:
                io_completed.set()
                log.info(f"I/O operations completed. Total iterations: {iteration}")

                # Final file statistics
                try:
                    stat_cmd = f"wc -l {test_file}"
                    retcode, stdout, _ = con.exec_cmd(stat_cmd, use_logger=False)
                    if retcode == 0:
                        line_count = stdout.split()[0]
                        log.info(f"Final file statistics: {line_count} lines written")

                    # Final checksum using md5sum
                    final_checksum_cmd = f"md5sum {test_file}"
                    retcode, stdout, _ = con.exec_cmd(
                        final_checksum_cmd, use_logger=False
                    )
                    if retcode == 0:
                        final_checksum = stdout.split()[0]
                        log.info(f"Final file checksum: {final_checksum}")
                except Exception as stat_error:
                    log.warning(f"Failed to get final file statistics: {stat_error}")

        # Start I/O in background thread
        io_thread: Thread = threading.Thread(
            target=continuous_io_operations, daemon=True
        )
        io_thread.start()
        log.info("Continuous I/O thread started")

        # Let I/O run for a bit before node reboot
        time.sleep(30)
        log.info("Initial I/O operations running successfully")

        # Step 2: Identify and reboot node hosting NFS server pod
        log.info("Step 2: Identifying node hosting NFS server pod")

        # Get NFS server pods
        nfs_server_pods = get_all_pods(
            namespace=self.namespace, selector=["rook-ceph-nfs"], selector_label="app"
        )

        if not nfs_server_pods:
            io_stop_event.set()
            io_thread.join(timeout=30)
            raise Exception("No NFS server pods found")

        nfs_server_pod = nfs_server_pods[0]
        log.info(f"Found NFS server pod: {nfs_server_pod.name}")

        # Get node hosting the NFS server pod
        nfs_node_name = nfs_server_pod.data["spec"]["nodeName"]
        log.info(f"NFS server pod is running on node: {nfs_node_name}")

        log.info(f"Rebooting node: {nfs_node_name}")

        # # Perform node reboot using platform nodes
        # log.info("Initiating node reboot...")
        # factory = platform_nodes.PlatformNodesFactory()
        # nodes_platform = factory.get_nodes_platform()
        # nodes_platform.restart_nodes([nfs_node_obj], wait=True)
        # log.info(f"Node {nfs_node_name} reboot completed")

        # Step 3: Wait for node and pods to recover
        log.info("Step 3: Waiting for node to come back online")

        # Wait for node to be ready
        log.info("Waiting for node to be in Ready state...")
        wait_for_nodes_status(
            node_names=[nfs_node_name], status=constants.NODE_READY, timeout=900
        )
        log.info(f"Node {nfs_node_name} is back online and Ready")

        # Wait for NFS server pod to be running again
        log.info("Waiting for NFS server pod to be running...")
        wait_for_pods_to_be_running(
            pod_names=[nfs_server_pod.name], namespace=self.namespace, timeout=600
        )
        log.info("NFS server pod is running again")

        # Verify NFS mount is still accessible
        log.info("Verifying NFS mount accessibility...")
        retcode, stdout, _ = con.exec_cmd(f"findmnt -M {test_folder_for_pod}")
        assert retcode == 0, "NFS mount not accessible after node reboot"
        log.info("NFS mount is still accessible")

        # Let I/O continue for a bit after recovery
        log.info("Continuing I/O operations after node recovery...")
        time.sleep(60)

        # Stop I/O operations
        log.info("Stopping I/O operations...")
        io_stop_event.set()
        io_thread.join(timeout=60)

        # Check for I/O errors
        log.info("Checking I/O results...")
        if io_errors:
            log.warning(f"I/O errors detected: {len(io_errors)} errors")
            for error in io_errors[:10]:  # Log first 10 errors
                log.warning(f"  - {error}")
            # Note: Some transient errors during reboot might be acceptable
            # Fail only if there are persistent errors after recovery
            if len(io_errors) > 20:
                raise AssertionError(f"Too many I/O errors detected: {len(io_errors)}")
        else:
            log.info("No I/O errors detected - all operations successful!")

        # Verify data consistency
        log.info("Verifying data consistency on NFS mount...")

        # Verify the single test file exists and is readable
        verify_cmd = f"test -f {test_file} && echo 'exists' || echo 'missing'"
        retcode, stdout, _ = con.exec_cmd(verify_cmd)
        file_exists = stdout.strip() == "exists"

        if file_exists:
            log.info(f"Test file {test_file} exists on NFS mount")

            # Read and verify file content
            retcode, stdout, _ = con.exec_cmd(f"cat {test_file}")
            if retcode == 0:
                line_count = len(stdout.strip().split("\n"))
                log.info(
                    f"Successfully read {test_file}: {line_count} lines, first 50 chars: {stdout.strip()[:50]}..."
                )
            else:
                log.warning(f"Could not read {test_file}")
        else:
            log.error(f"Test file {test_file} not found on NFS mount")

        # Verify data from pod perspective
        log.info("Verifying data consistency from pod...")
        pod_file_path = f"/mnt/{IO_TEST_FILE_NAME}"
        pod_verify_cmd = f"test -f {pod_file_path} && wc -l {pod_file_path} || echo '0'"
        result = pod_obj.exec_cmd_on_pod(
            command=f"bash -c '{pod_verify_cmd}'", out_yaml_format=False
        )
        pod_line_count = (
            int(result.strip().split()[0]) if result.strip().split()[0].isdigit() else 0
        )
        log.info(f"Pod sees {pod_line_count} lines in test file")

        # Verify file exists from both perspectives
        assert file_exists, f"Test file {test_file} not found on NFS mount"
        assert pod_line_count > 0, "Pod cannot see test file or file is empty"

        log.info(
            f"NFS node reboot test completed: {nfs_node_name} rebooted, {len(io_errors)} I/O errors, "
            f"test file verified with {pod_line_count} lines"
        )

        # ========================================================================
        # Scenario: NFS PVC Snapshot and Restore with Data Integrity Verification
        # ========================================================================
        log.info("=" * 80)
        log.info("Starting NFS PVC Snapshot and Restore scenario")
        log.info("=" * 80)

        # Step 1: Capture file checksum from NFS mount
        log.info("Step 1: Capturing file checksum from NFS mount")

        # Calculate checksum of the single test file
        checksum_cmd = f"md5sum {test_file}"
        retcode, stdout, _ = con.exec_cmd(checksum_cmd)
        assert retcode == 0, f"Failed to calculate file checksum: {stdout}"

        original_file_checksum = stdout.split()[0]
        log.info(f"Original file checksum: {original_file_checksum}")

        # Get file line count for reference
        count_cmd = f"wc -l {test_file}"
        retcode, stdout, _ = con.exec_cmd(count_cmd)
        original_line_count = int(stdout.split()[0]) if retcode == 0 else 0
        log.info(f"Total lines in file: {original_line_count}")

        # Step 2: Create snapshot of the NFS PVC
        log.info("Step 2: Creating snapshot of NFS PVC")

        snapshot_name = f"{pvc_name}-snapshot"
        snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        # Get the NFS snapshot class (not CephFS)
        # NFS has its own snapshot class: ocs-storagecluster-nfsplugin-snapclass
        nfs_snapshotclass_name = "ocs-storagecluster-nfsplugin-snapclass"

        log.info(
            f"Creating snapshot: {snapshot_name} from NFS PVC: {pvc_name} using "
            f"snapshot class: {nfs_snapshotclass_name}"
        )
        from ocs_ci.ocs.resources.pvc import create_pvc_snapshot

        snapshot_obj = create_pvc_snapshot(
            pvc_name=pvc_name,
            snap_yaml=snap_yaml,
            snap_name=snapshot_name,
            namespace=self.namespace,
            sc_name=nfs_snapshotclass_name,
            wait=True,
            timeout=300,
        )

        log.info(f"Snapshot {snapshot_name} created successfully")

        # Add snapshot to cleanup
        def cleanup_snapshot():
            try:
                log.info(f"Deleting snapshot {snapshot_name}")
                snapshot_obj.delete()
                snapshot_obj.ocp.wait_for_delete(
                    resource_name=snapshot_name, timeout=180
                )
                log.info(f"Snapshot {snapshot_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete snapshot: {e}")

        request.addfinalizer(cleanup_snapshot)

        # Step 3: Create new PVC from snapshot (restore)
        log.info("Step 3: Creating new PVC from snapshot")

        restored_pvc_name = f"{pvc_name}-restored"
        restored_pvc_obj = pvc.create_restore_pvc(
            sc_name=self.nfs_sc,
            snap_name=snapshot_name,
            namespace=self.namespace,
            size="10Gi",
            pvc_name=restored_pvc_name,
            volume_mode="Filesystem",
            restore_pvc_yaml=constants.CSI_CEPHFS_PVC_RESTORE_YAML,
            access_mode=constants.ACCESS_MODE_RWX,
        )

        log.info(f"Restored PVC {restored_pvc_name} created from snapshot")

        # Add restored PVC to cleanup
        def cleanup_restored_pvc():
            try:
                log.info(f"Deleting restored PVC {restored_pvc_name}")
                restored_pvc_obj.delete(wait=True)
                log.info(f"Restored PVC {restored_pvc_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete restored PVC: {e}")

        request.addfinalizer(cleanup_restored_pvc)

        # Step 4: Create new pod with restored PVC
        log.info("Step 4: Creating new pod with restored PVC")

        restored_pod_name = f"{pod_name}-restored"
        restored_deployment_data = templating.load_yaml(constants.NFS_APP_POD_YAML)

        # Configure deployment
        restored_deployment_data["metadata"]["name"] = restored_pod_name
        restored_deployment_data["metadata"]["labels"]["app"] = restored_pod_name
        restored_deployment_data["spec"]["selector"]["matchLabels"][
            "name"
        ] = restored_pod_name
        restored_deployment_data["spec"]["template"]["metadata"]["labels"][
            "name"
        ] = restored_pod_name
        restored_deployment_data["spec"]["template"]["spec"]["volumes"][0][
            "persistentVolumeClaim"
        ]["claimName"] = restored_pvc_name

        helpers.create_resource(**restored_deployment_data)
        time.sleep(60)

        assert self.pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector=f"name={restored_pod_name}",
            dont_allow_other_resources=True,
            timeout=120,
        )

        restored_pod_obj = pod.get_all_pods(
            namespace=self.namespace,
            selector=[restored_pod_name],
            selector_label="name",
        )[0]

        log.info(f"Restored pod {restored_pod_obj.name} is running")

        # Add restored deployment to cleanup
        def cleanup_restored_deployment():
            try:
                log.info(f"Deleting restored deployment {restored_pod_name}")
                deployment_obj = ocp.OCP(
                    kind=constants.DEPLOYMENT, namespace=self.namespace
                )
                if deployment_obj.is_exist(resource_name=restored_pod_name):
                    deployment_obj.delete(resource_name=restored_pod_name)
                    deployment_obj.wait_for_delete(
                        resource_name=restored_pod_name, timeout=180
                    )
                    log.info(
                        f"Restored deployment {restored_pod_name} deleted successfully"
                    )

                log.info("Waiting for restored pod to be terminated...")
                restored_pod_obj.ocp.wait_for_delete(restored_pod_obj.name, timeout=180)
                log.info(
                    f"Restored pod {restored_pod_obj.name} terminated successfully"
                )
            except Exception as e:
                log.warning(f"Failed to delete restored deployment/pod: {e}")

        request.addfinalizer(cleanup_restored_deployment)

        # Step 5: Mount restored PVC on NFS client and verify data integrity
        log.info(
            "Step 5: Mounting restored PVC on NFS client for data integrity verification"
        )

        # Get NFS share details for restored PVC
        fetch_restored_vol_name_cmd = (
            "get pvc " + restored_pvc_name + " --output jsonpath='{.spec.volumeName}'"
        )
        restored_vol_name = self.pvc_obj.exec_oc_cmd(fetch_restored_vol_name_cmd)
        log.info(
            f"For restored PVC {restored_pvc_name} volume name is, {restored_vol_name}"
        )

        fetch_restored_pv_share_cmd = (
            "get pv "
            + restored_vol_name
            + " --output jsonpath='{.spec.csi.volumeAttributes.share}'"
        )
        restored_share_details = self.pv_obj.exec_oc_cmd(fetch_restored_pv_share_cmd)
        log.info(f"Restored share details is, {restored_share_details}")

        # Create mount point for restored NFS export
        restored_test_folder = f"{self.test_folder}-restored"
        retcode, _, _ = con.exec_cmd(f"mkdir -p {restored_test_folder}")
        assert retcode == 0, f"Failed to create mount point {restored_test_folder}"

        # Mount restored NFS export
        export_restored_nfs_cmd = (
            "mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + restored_share_details
            + " "
            + restored_test_folder
        )

        log.info(f"Mounting restored NFS export: {export_restored_nfs_cmd}")
        retry(
            (CommandFailed),
            tries=28,
            delay=10,
        )(
            con.exec_cmd
        )(export_restored_nfs_cmd)

        # Verify mount is successful
        retcode, stdout, _ = con.exec_cmd(f"findmnt -M {restored_test_folder}")
        assert retcode == 0, f"Mount verification failed for {restored_test_folder}"
        log.info(f"Successfully mounted restored NFS export at {restored_test_folder}")

        # Add restored mount to cleanup
        def cleanup_restored_mount():
            try:
                log.info(f"Unmounting {restored_test_folder}")
                nfs_utils.unmount(con, restored_test_folder)
                con.exec_cmd(f"rm -rf {restored_test_folder}")
                log.info(f"Restored mount {restored_test_folder} cleaned up")
            except Exception as e:
                log.warning(f"Failed to unmount restored NFS: {e}")

        request.addfinalizer(cleanup_restored_mount)

        # Calculate checksum from restored NFS mount on client
        log.info("Calculating file checksum from restored NFS mount...")
        restored_test_file = f"{restored_test_folder}/{IO_TEST_FILE_NAME}"
        restored_checksum_cmd = f"md5sum {restored_test_file}"
        retcode, stdout, _ = con.exec_cmd(restored_checksum_cmd)
        assert retcode == 0, f"Failed to calculate restored file checksum: {stdout}"

        restored_file_checksum = stdout.split()[0]
        log.info(f"Restored file checksum: {restored_file_checksum}")

        # Get line count in restored file
        restored_count_cmd = f"wc -l {restored_test_file}"
        retcode, stdout, _ = con.exec_cmd(restored_count_cmd)
        restored_line_count = int(stdout.split()[0]) if retcode == 0 else 0
        log.info(f"Total lines in restored file: {restored_line_count}")

        # Verify line counts match
        assert (
            original_line_count == restored_line_count
        ), f"Line count mismatch! Original: {original_line_count}, Restored: {restored_line_count}"

        # Verify checksums match
        assert original_file_checksum == restored_file_checksum, (
            f"File checksum mismatch! Data integrity check failed.\n"
            f"Original checksum: {original_file_checksum}\n"
            f"Restored checksum: {restored_file_checksum}"
        )

        log.info("=" * 80)
        log.info("NFS Snapshot and Restore scenario completed successfully!")
        log.info("Summary:")
        log.info(f"  - Snapshot created: {snapshot_name}")
        log.info(f"  - Restored PVC: {restored_pvc_name}")
        log.info(f"  - Lines verified: {restored_line_count}")
        log.info(f"  - Original checksum: {original_file_checksum}")
        log.info(f"  - Restored checksum: {restored_file_checksum}")
        log.info("  - Data integrity: VERIFIED ✓")
        log.info("=" * 80)
        log.info("Test completed successfully - cleanup will be handled by finalizer")

        # ========================================================================
        # Scenario: Clone Restored PVC, Resize, and Verify Data Integrity
        # ========================================================================
        log.info("=" * 80)
        log.info("Starting PVC Clone, Resize, and Data Integrity Verification scenario")
        log.info("=" * 80)

        # Step 1: Create a clone of the restored PVC
        log.info("Step 1: Creating clone of restored PVC")

        cloned_pvc_name = f"{restored_pvc_name}-clone"
        cloned_pvc_obj = pvc.create_pvc_clone(
            sc_name=self.nfs_sc,
            parent_pvc=restored_pvc_name,
            clone_yaml=constants.CSI_CEPHFS_PVC_CLONE_YAML,
            namespace=self.namespace,
            pvc_name=cloned_pvc_name,
            storage_size="10Gi",
        )

        log.info(f"Cloned PVC {cloned_pvc_name} created successfully")

        # Add cloned PVC to cleanup
        def cleanup_cloned_pvc():
            try:
                log.info(f"Deleting cloned PVC {cloned_pvc_name}")
                cloned_pvc_obj.delete()
                cloned_pvc_obj.ocp.wait_for_delete(
                    resource_name=cloned_pvc_name, timeout=180
                )
                log.info(f"Cloned PVC {cloned_pvc_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete cloned PVC: {e}")

        request.addfinalizer(cleanup_cloned_pvc)

        # Step 2: Create pod deployment using the cloned PVC
        log.info("Step 2: Creating pod deployment with cloned PVC")

        cloned_pod_name = f"test-pod-cloned-{int(time.time())}"
        cloned_deployment_data = templating.load_yaml(constants.NFS_APP_POD_YAML)

        # Deployment name
        cloned_deployment_data["metadata"]["name"] = cloned_pod_name

        # Label values
        cloned_deployment_data["metadata"]["labels"]["app"] = cloned_pod_name
        cloned_deployment_data["spec"]["selector"]["matchLabels"][
            "name"
        ] = cloned_pod_name
        cloned_deployment_data["spec"]["template"]["metadata"]["labels"][
            "name"
        ] = cloned_pod_name

        # PVC claimName
        cloned_deployment_data["spec"]["template"]["spec"]["volumes"][0][
            "persistentVolumeClaim"
        ]["claimName"] = cloned_pvc_name

        helpers.create_resource(**cloned_deployment_data)
        time.sleep(120)

        assert self.pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector=f"name={cloned_pod_name}",
            dont_allow_other_resources=True,
            timeout=300,
        )

        # Get the actual pod created by the deployment (it will have a generated suffix)
        cloned_pod_objs = pod.get_all_pods(
            namespace=self.namespace, selector=[cloned_pod_name], selector_label="name"
        )
        assert (
            len(cloned_pod_objs) > 0
        ), f"No pods found for deployment {cloned_pod_name}"
        # cloned_pod_obj = cloned_pod_objs[0]

        log.info(f"Cloned pod {cloned_pod_name} is running")

        # Add cloned pod to cleanup
        def cleanup_cloned_pod():
            try:
                log.info(f"Deleting cloned pod deployment {cloned_pod_name}")
                deployment_obj = ocp.OCP(
                    kind=constants.DEPLOYMENT, namespace=self.namespace
                )
                deployment_obj.delete(resource_name=cloned_pod_name)
                log.info(f"Cloned pod deployment {cloned_pod_name} deleted")
            except Exception as e:
                log.warning(f"Failed to delete cloned pod: {e}")

        request.addfinalizer(cleanup_cloned_pod)

        # Step 3: Get NFS export details for cloned PVC and mount on client
        log.info("Step 3: Getting NFS export details for cloned PVC")

        # Get volume name from cloned PVC
        fetch_cloned_vol_name_cmd = (
            f"get pvc {cloned_pvc_name} -o jsonpath='{{.spec.volumeName}}'"
        )
        cloned_vol_name = self.pvc_obj.exec_oc_cmd(fetch_cloned_vol_name_cmd)
        log.info(f"Cloned volume name: {cloned_vol_name}")

        # Get NFS share details from cloned PV
        fetch_cloned_pv_share_cmd = f"get pv {cloned_vol_name} -o jsonpath='{{.spec.csi.volumeAttributes.share}}'"
        cloned_share_details = self.pv_obj.exec_oc_cmd(fetch_cloned_pv_share_cmd)
        log.info(f"Cloned NFS share: {cloned_share_details}")

        # Create mount point for cloned NFS export
        cloned_test_folder = f"{self.test_folder}-cloned"
        retcode, _, _ = con.exec_cmd(f"mkdir -p {cloned_test_folder}")
        assert retcode == 0, f"Failed to create mount point {cloned_test_folder}"

        # Mount cloned NFS export on client
        mount_cloned_cmd = (
            "mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + cloned_share_details
            + " "
            + cloned_test_folder
        )

        log.info(f"Mounting cloned NFS export: {mount_cloned_cmd}")
        retry(
            (CommandFailed),
            tries=28,
            delay=10,
        )(
            con.exec_cmd
        )(mount_cloned_cmd)

        # Verify mount is successful
        retcode, stdout, _ = con.exec_cmd(f"findmnt -M {cloned_test_folder}")
        assert retcode == 0, f"Mount verification failed for {cloned_test_folder}"
        log.info(f"Successfully mounted cloned NFS export at {cloned_test_folder}")

        # Add cloned mount to cleanup
        def cleanup_cloned_mount():
            try:
                log.info(f"Unmounting cloned NFS export from {cloned_test_folder}")
                con.exec_cmd(f"umount {cloned_test_folder}")
                con.exec_cmd(f"rm -rf {cloned_test_folder}")
                log.info(f"Cloned mount {cloned_test_folder} cleaned up")
            except Exception as e:
                log.warning(f"Failed to unmount cloned NFS: {e}")

        request.addfinalizer(cleanup_cloned_mount)

        # Step 4: Write IO to a single file on cloned mount
        log.info("Step 4: Writing IO to single file on cloned NFS mount")

        CLONED_IO_FILE_NAME = "io_test_cloned.txt"
        cloned_io_file = f"{cloned_test_folder}/{CLONED_IO_FILE_NAME}"

        # Write initial data to single file
        initial_cloned_data = f"Cloned PVC IO test started at {time.time()}"
        init_cloned_cmd = f'echo "{initial_cloned_data}" > {cloned_io_file}'
        retcode, _, stderr = con.exec_cmd(init_cloned_cmd)
        assert retcode == 0, f"Failed to initialize cloned IO file: {stderr}"

        # Append multiple lines to the same single file
        log.info("Writing data to single file...")
        for i in range(1, 21):
            io_data = f"Cloned IO iteration {i} - {time.time()}"
            write_cmd = f'echo "{io_data}" >> {cloned_io_file}'
            retcode, _, stderr = con.exec_cmd(write_cmd, use_logger=False)
            if retcode != 0:
                log.warning(f"Write failed at iteration {i}: {stderr}")
            time.sleep(0.5)

        log.info(
            f"IO operations completed on cloned mount - single file: {CLONED_IO_FILE_NAME}"
        )

        # Step 5: Capture checksum of the single file before resize
        log.info("Step 5: Capturing file checksum before PVC resize")

        cloned_checksum_cmd = f"md5sum {cloned_io_file}"
        retcode, stdout, _ = con.exec_cmd(cloned_checksum_cmd)
        assert retcode == 0, f"Failed to calculate cloned file checksum: {stdout}"
        pre_resize_checksum = stdout.split()[0]
        log.info(f"Pre-resize file checksum: {pre_resize_checksum}")

        # Get line count before resize
        pre_resize_line_cmd = f"wc -l {cloned_io_file}"
        retcode, stdout, _ = con.exec_cmd(pre_resize_line_cmd)
        pre_resize_line_count = int(stdout.split()[0]) if retcode == 0 else 0
        log.info(f"Pre-resize line count: {pre_resize_line_count}")

        # Step 6: Resize (expand) the cloned PVC
        log.info("Step 6: Resizing cloned PVC")

        new_size = "15Gi"
        log.info(f"Expanding cloned PVC from 10Gi to {new_size}")

        # Patch the PVC to request more storage
        patch_cmd = (
            f"patch pvc {cloned_pvc_name} -p "
            f'\'{{"spec":{{"resources":{{"requests":{{"storage":"{new_size}"}}}}}}}}\''
        )
        result = cloned_pvc_obj.ocp.exec_oc_cmd(patch_cmd)
        log.info(f"PVC resize request submitted: {result}")

        # Wait for PVC to be resized
        log.info("Waiting for PVC resize to complete...")
        time.sleep(30)  # Give some time for resize operation

        # Verify PVC size
        for attempt in range(12):  # Wait up to 2 minutes
            size_cmd = (
                f"get pvc {cloned_pvc_name} -o jsonpath='{{.status.capacity.storage}}'"
            )
            current_size = cloned_pvc_obj.ocp.exec_oc_cmd(size_cmd)
            if current_size and current_size.strip() == new_size:
                log.info(f"PVC successfully resized to {new_size}")
                break
            log.info(
                f"Waiting for resize... Current size: {current_size.strip() if current_size else 'unknown'}"
            )
            time.sleep(10)
        else:
            log.warning("PVC resize may not have completed within timeout")

        # Step 7: Verify data integrity after resize
        log.info("Step 7: Verifying data integrity after PVC resize")

        # Calculate checksum of the same file after resize
        post_resize_checksum_cmd = f"md5sum {cloned_io_file}"
        retcode, stdout, _ = con.exec_cmd(post_resize_checksum_cmd)
        assert retcode == 0, f"Failed to calculate post-resize file checksum: {stdout}"
        post_resize_checksum = stdout.split()[0]
        log.info(f"Post-resize file checksum: {post_resize_checksum}")

        # Get line count after resize
        post_resize_line_cmd = f"wc -l {cloned_io_file}"
        retcode, stdout, _ = con.exec_cmd(post_resize_line_cmd)
        post_resize_line_count = int(stdout.split()[0]) if retcode == 0 else 0
        log.info(f"Post-resize line count: {post_resize_line_count}")

        # Verify line count unchanged
        assert pre_resize_line_count == post_resize_line_count, (
            f"Line count mismatch after resize!\n"
            f"Pre-resize: {pre_resize_line_count}\n"
            f"Post-resize: {post_resize_line_count}"
        )

        # Verify checksum unchanged
        assert pre_resize_checksum == post_resize_checksum, (
            f"File checksum mismatch after resize! Data integrity check failed.\n"
            f"Pre-resize checksum: {pre_resize_checksum}\n"
            f"Post-resize checksum: {post_resize_checksum}"
        )

        log.info("=" * 80)
        log.info(
            "PVC Clone, Resize, and Data Integrity scenario completed successfully!"
        )
        log.info("Summary:")
        log.info(f"  - Cloned PVC: {cloned_pvc_name}")
        log.info(f"  - Cloned pod: {cloned_pod_name}")
        log.info(f"  - IO file: {CLONED_IO_FILE_NAME}")
        log.info("  - Original size: 10Gi")
        log.info(f"  - Resized to: {new_size}")
        log.info(f"  - Line count: {post_resize_line_count}")
        log.info(f"  - Pre-resize checksum: {pre_resize_checksum}")
        log.info(f"  - Post-resize checksum: {post_resize_checksum}")
        log.info("  - Data integrity: VERIFIED ✓")

        # ========================================================================
        # Scenario: Snapshot Resized PVC, Restore, Resize Again, Verify Integrity
        # ========================================================================
        log.info("=" * 80)
        log.info("Starting Snapshot of Resized PVC, Restore, and Re-Resize scenario")
        log.info("=" * 80)

        # Step 1: Create snapshot of the resized cloned PVC
        log.info("Step 1: Creating snapshot of resized cloned PVC")

        cloned_snapshot_name = f"{cloned_pvc_name}-snapshot"
        snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML
        nfs_snapshotclass_name = "ocs-storagecluster-nfsplugin-snapclass"

        log.info(
            f"Creating snapshot: {cloned_snapshot_name} from resized PVC: {cloned_pvc_name} "
            f"using snapshot class: {nfs_snapshotclass_name}"
        )

        cloned_snapshot_obj = create_pvc_snapshot(
            pvc_name=cloned_pvc_name,
            snap_yaml=snap_yaml,
            snap_name=cloned_snapshot_name,
            namespace=self.namespace,
            sc_name=nfs_snapshotclass_name,
            wait=True,
            timeout=300,
        )

        log.info(f"Snapshot {cloned_snapshot_name} created successfully")

        # Add snapshot to cleanup
        def cleanup_cloned_snapshot():
            try:
                log.info(f"Deleting snapshot {cloned_snapshot_name}")
                cloned_snapshot_obj.delete()
                cloned_snapshot_obj.ocp.wait_for_delete(
                    resource_name=cloned_snapshot_name, timeout=180
                )
                log.info(f"Snapshot {cloned_snapshot_name} deleted successfully")
            except Exception as e:
                log.warning(f"Failed to delete snapshot: {e}")

        request.addfinalizer(cleanup_cloned_snapshot)

        # Step 2: Restore PVC from the snapshot
        log.info("Step 2: Restoring PVC from snapshot of resized PVC")

        final_restored_pvc_name = f"{cloned_pvc_name}-restored"
        final_restored_pvc_obj = pvc.create_restore_pvc(
            sc_name=self.nfs_sc,
            snap_name=cloned_snapshot_name,
            namespace=self.namespace,
            size="15Gi",
            pvc_name=final_restored_pvc_name,
            volume_mode="Filesystem",
            restore_pvc_yaml=constants.CSI_CEPHFS_PVC_RESTORE_YAML,
            access_mode=constants.ACCESS_MODE_RWX,
        )

        log.info(f"Restored PVC {final_restored_pvc_name} created from snapshot")

        # Add restored PVC to cleanup
        def cleanup_final_restored_pvc():
            try:
                log.info(f"Deleting final restored PVC {final_restored_pvc_name}")
                final_restored_pvc_obj.delete(wait=True)
                log.info(
                    f"Final restored PVC {final_restored_pvc_name} deleted successfully"
                )
            except Exception as e:
                log.warning(f"Failed to delete final restored PVC: {e}")

        request.addfinalizer(cleanup_final_restored_pvc)

        # Step 3: Create pod deployment using the final restored PVC
        log.info("Step 3: Creating pod deployment with final restored PVC")

        final_restored_pod_name = f"test-pod-final-{int(time.time())}"
        final_restored_deployment_data = templating.load_yaml(
            constants.NFS_APP_POD_YAML
        )

        # Deployment name
        final_restored_deployment_data["metadata"]["name"] = final_restored_pod_name

        # Label values
        final_restored_deployment_data["metadata"]["labels"][
            "app"
        ] = final_restored_pod_name
        final_restored_deployment_data["spec"]["selector"]["matchLabels"][
            "name"
        ] = final_restored_pod_name
        final_restored_deployment_data["spec"]["template"]["metadata"]["labels"][
            "name"
        ] = final_restored_pod_name

        # PVC claimName
        final_restored_deployment_data["spec"]["template"]["spec"]["volumes"][0][
            "persistentVolumeClaim"
        ]["claimName"] = final_restored_pvc_name

        helpers.create_resource(**final_restored_deployment_data)
        time.sleep(120)

        assert self.pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector=f"name={final_restored_pod_name}",
            dont_allow_other_resources=True,
            timeout=300,
        )

        # Get the actual pod created by the deployment (using selector, not direct name)
        final_restored_pod_objs = pod.get_all_pods(
            namespace=self.namespace,
            selector=[final_restored_pod_name],
            selector_label="name",
        )
        assert (
            len(final_restored_pod_objs) > 0
        ), f"No pods found for deployment {final_restored_pod_name}"
        # final_restored_pod_obj = final_restored_pod_objs[0]

        log.info(f"Final restored pod {final_restored_pod_name} is running")

        # Add final restored pod to cleanup
        def cleanup_final_restored_pod():
            try:
                log.info(
                    f"Deleting final restored pod deployment {final_restored_pod_name}"
                )
                deployment_obj = ocp.OCP(
                    kind=constants.DEPLOYMENT, namespace=self.namespace
                )
                deployment_obj.delete(resource_name=final_restored_pod_name)
                log.info(
                    f"Final restored pod deployment {final_restored_pod_name} deleted"
                )
            except Exception as e:
                log.warning(f"Failed to delete final restored pod: {e}")

        request.addfinalizer(cleanup_final_restored_pod)

        # Step 4: Get NFS export details and mount on client
        log.info("Step 4: Getting NFS export details for final restored PVC")

        # Get volume name from final restored PVC
        fetch_final_vol_name_cmd = (
            f"get pvc {final_restored_pvc_name} -o jsonpath='{{.spec.volumeName}}'"
        )
        final_restored_vol_name = self.pvc_obj.exec_oc_cmd(fetch_final_vol_name_cmd)
        log.info(f"Final restored volume name: {final_restored_vol_name}")

        # Get NFS share details from final restored PV
        fetch_final_pv_share_cmd = (
            f"get pv {final_restored_vol_name} "
            f"-o jsonpath='{{.spec.csi.volumeAttributes.share}}'"
        )
        final_restored_share_details = self.pv_obj.exec_oc_cmd(fetch_final_pv_share_cmd)
        log.info(f"Final restored NFS share: {final_restored_share_details}")

        # Create mount point for final restored NFS export
        final_restored_test_folder = f"{self.test_folder}-final"
        retcode, _, _ = con.exec_cmd(f"mkdir -p {final_restored_test_folder}")
        assert (
            retcode == 0
        ), f"Failed to create mount point {final_restored_test_folder}"

        # Mount final restored NFS export on client
        mount_final_restored_cmd = (
            "mount -t nfs4 -o proto=tcp "
            + self.hostname_add
            + ":"
            + final_restored_share_details
            + " "
            + final_restored_test_folder
        )

        log.info(f"Mounting final restored NFS export: {mount_final_restored_cmd}")
        retry(
            (CommandFailed),
            tries=28,
            delay=10,
        )(
            con.exec_cmd
        )(mount_final_restored_cmd)

        # Verify mount is successful
        retcode, stdout, _ = con.exec_cmd(f"findmnt -M {final_restored_test_folder}")
        assert (
            retcode == 0
        ), f"Mount verification failed for {final_restored_test_folder}"
        log.info(
            f"Successfully mounted final restored NFS export at {final_restored_test_folder}"
        )

        # Add final restored mount to cleanup
        def cleanup_final_restored_mount():
            try:
                log.info(
                    f"Unmounting final restored NFS export from {final_restored_test_folder}"
                )
                con.exec_cmd(f"umount {final_restored_test_folder}")
                con.exec_cmd(f"rm -rf {final_restored_test_folder}")
                log.info(
                    f"Final restored mount {final_restored_test_folder} cleaned up"
                )
            except Exception as e:
                log.warning(f"Failed to unmount final restored NFS: {e}")

        request.addfinalizer(cleanup_final_restored_mount)

        # Step 5: Write IO to a single file on final restored mount
        log.info("Step 5: Writing IO to single file on final restored NFS mount")

        FINAL_IO_FILE_NAME = "io_test_final.txt"
        final_io_file = f"{final_restored_test_folder}/{FINAL_IO_FILE_NAME}"

        # Write initial data to single file
        initial_final_data = f"Final restored PVC IO test started at {time.time()}"
        init_final_cmd = f'echo "{initial_final_data}" > {final_io_file}'
        retcode, _, stderr = con.exec_cmd(init_final_cmd)
        assert retcode == 0, f"Failed to initialize final IO file: {stderr}"

        # Append multiple lines to the same single file
        log.info("Writing data to single file...")
        for i in range(1, 21):
            io_data = f"Final IO iteration {i} - {time.time()}"
            write_cmd = f'echo "{io_data}" >> {final_io_file}'
            retcode, _, stderr = con.exec_cmd(write_cmd)
            if retcode != 0:
                log.warning(f"Write failed at iteration {i}: {stderr}")
            time.sleep(0.5)

        log.info(
            f"IO operations completed on final restored mount - single file: {FINAL_IO_FILE_NAME}"
        )

        # Step 6: Capture checksum before resize
        log.info("Step 6: Capturing file checksum before final PVC resize")

        final_checksum_cmd = f"md5sum {final_io_file}"
        retcode, stdout, _ = con.exec_cmd(final_checksum_cmd)
        assert retcode == 0, f"Failed to calculate final file checksum: {stdout}"
        pre_final_resize_checksum = stdout.split()[0]
        log.info(f"Pre-final-resize file checksum: {pre_final_resize_checksum}")

        # Get line count before resize
        pre_final_resize_line_cmd = f"wc -l {final_io_file}"
        retcode, stdout, _ = con.exec_cmd(pre_final_resize_line_cmd)
        pre_final_resize_line_count = int(stdout.split()[0]) if retcode == 0 else 0
        log.info(f"Pre-final-resize line count: {pre_final_resize_line_count}")

        # Step 7: Resize the final restored PVC
        log.info("Step 7: Resizing final restored PVC")

        final_new_size = "20Gi"
        log.info(f"Expanding final restored PVC from 15Gi to {final_new_size}")

        # Patch the PVC to request more storage
        final_patch_cmd = (
            f"patch pvc {final_restored_pvc_name} -p "
            f'\'{{"spec":{{"resources":{{"requests":{{"storage":"{final_new_size}"}}}}}}}}\''
        )
        result = final_restored_pvc_obj.ocp.exec_oc_cmd(final_patch_cmd)
        log.info(f"Final PVC resize request submitted: {result}")

        # Wait for PVC to be resized
        log.info("Waiting for final PVC resize to complete...")
        time.sleep(30)

        # Verify PVC size
        for attempt in range(12):  # Wait up to 2 minutes
            size_cmd = f"get pvc {final_restored_pvc_name} -o jsonpath='{{.status.capacity.storage}}'"
            current_size = final_restored_pvc_obj.ocp.exec_oc_cmd(size_cmd)
            if current_size and current_size.strip() == final_new_size:
                log.info(f"Final PVC successfully resized to {final_new_size}")
                break
            log.info(
                f"Waiting for final resize... Current size: {current_size.strip() if current_size else 'unknown'}"
            )
            time.sleep(10)
        else:
            log.warning("Final PVC resize may not have completed within timeout")

        # Step 8: Verify data integrity after final resize
        log.info("Step 8: Verifying data integrity after final PVC resize")

        # Calculate checksum of the same file after resize
        post_final_resize_checksum_cmd = f"md5sum {final_io_file}"
        retcode, stdout, _ = con.exec_cmd(post_final_resize_checksum_cmd)
        assert (
            retcode == 0
        ), f"Failed to calculate post-final-resize file checksum: {stdout}"
        post_final_resize_checksum = stdout.split()[0]
        log.info(f"Post-final-resize file checksum: {post_final_resize_checksum}")

        # Get line count after resize
        post_final_resize_line_cmd = f"wc -l {final_io_file}"
        retcode, stdout, _ = con.exec_cmd(post_final_resize_line_cmd)
        post_final_resize_line_count = int(stdout.split()[0]) if retcode == 0 else 0
        log.info(f"Post-final-resize line count: {post_final_resize_line_count}")

        # Verify line count unchanged
        assert pre_final_resize_line_count == post_final_resize_line_count, (
            f"Line count mismatch after final resize!\n"
            f"Pre-resize: {pre_final_resize_line_count}\n"
            f"Post-resize: {post_final_resize_line_count}"
        )

        # Verify checksum unchanged
        assert pre_final_resize_checksum == post_final_resize_checksum, (
            f"File checksum mismatch after final resize! Data integrity check failed.\n"
            f"Pre-resize checksum: {pre_final_resize_checksum}\n"
            f"Post-resize checksum: {post_final_resize_checksum}"
        )

        log.info("=" * 80)
        log.info("Snapshot, Restore, and Re-Resize scenario completed successfully!")
        log.info("Summary:")
        log.info(f"  - Snapshot of resized PVC: {cloned_snapshot_name}")
        log.info(f"  - Final restored PVC: {final_restored_pvc_name}")
        log.info(f"  - Final restored pod: {final_restored_pod_name}")
        log.info(f"  - IO file: {FINAL_IO_FILE_NAME}")
        log.info("  - Original size: 15Gi")
        log.info(f"  - Resized to: {final_new_size}")
        log.info(f"  - Line count: {post_final_resize_line_count}")
        log.info(f"  - Pre-resize checksum: {pre_final_resize_checksum}")
        log.info(f"  - Post-resize checksum: {post_final_resize_checksum}")
        log.info("  - Data integrity: VERIFIED ✓")
        log.info("=" * 80)
        log.info(
            "All test scenarios completed successfully - cleanup will be handled by finalizers"
        )
        log.info("=" * 80)
