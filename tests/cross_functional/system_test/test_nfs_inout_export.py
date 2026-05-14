import ipaddress
import pytest
import logging
import time
import os
import socket

from subprocess import CompletedProcess
from ocs_ci.utility import nfs_utils
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.framework import config
from ocs_ci.utility.connection import Connection
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import (
    brown_squad,
    skipif_rosa_hcp,
    skipif_lean_deployment,
)
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier2,
    tier4c,
    skipif_ocp_version,
    skipif_managed_service,
    skip_for_provider_or_client_if_ocs_version,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    polarion_id,
    skipif_external_mode,
    skipif_hci_client,
    hci_client_required,
)
from ocs_ci.utility import version as version_module
from ocs_ci.ocs.resources import pod, ocs
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed, ConfigurationError
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
)
from ocs_ci.utility.nfs_utils import provisioner_selectors

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

        pod_name = "test-pod-outcluster-0"
        pvc_name = "test-pvc-outcluster-0"
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

        import pdb

        pdb.set_trace()

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

        # Cleanup
        log.info("Cleaning up resources")

        # Unmount
        log.info(f"Unmounting {test_folder_for_pod}")
        nfs_utils.unmount(con, test_folder_for_pod)

        # Remove mount point directory
        con.exec_cmd(f"rm -rf {test_folder_for_pod}")

        # Wait a bit after unmounting to ensure NFS server releases the export
        log.info("Waiting for NFS export to be fully released...")
        time.sleep(10)

        # Deletion of Pods and PVCs
        log.info("Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        pv_obj = nfs_pvc_obj.backed_pv_obj
        log.info(f"pv object-----{pv_obj}")

        import pdb

        pdb.set_trace()

        log.info("Deleting PVC")
        nfs_pvc_obj.delete(wait=True)
        log.info(f"Verified: PVC {nfs_pvc_obj.name} is deleted.")

        log.info("Check nfs pv is deleted")
        pv_obj.ocp.wait_for_delete(resource_name=pv_obj.name, timeout=300)

        log.info("Cleanup complete")
