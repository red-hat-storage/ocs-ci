import pytest
import logging
import yaml

from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from functools import partial
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
)

# from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.ocs.resources import pod


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


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

    @pytest.fixture(autouse=True)
    def setup(
        self,
    ):
        """ """
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

        self.nfs_spec_enable = '{"spec": {"nfs":{"enable": true}}}'
        self.rook_csi_config_enable = '{"data":{"ROOK_CSI_ENABLE_NFS": "true"}}'
        self.nfs_spec_disable = '{"spec": {"nfs":{"enable": false}}}'
        self.rook_csi_config_disable = '{"data":{"ROOK_CSI_ENABLE_NFS": "false"}}'
        self.test_folder = "test_nfs"

        pod_functions = {
            "cephfsplugin": partial(
                pod.get_plugin_pods, interface=constants.CEPHBLOCKPOOL
            ),
            "cephfsplugin_provisioner": partial(pod.get_cephfsplugin_provisioner_pods),
        }

        # Get number of pods of type 'cephfs plugin' and 'cephfs plugin provisioner' pods
        self.num_of_cephfsplugin_pods = len(pod_functions["cephfsplugin"]())
        log.info(f"number of pods, {self.num_of_cephfsplugin_pods}")
        self.num_of_cephfsplugin_provisioner_pods = len(
            pod_functions["cephfsplugin_provisioner"]()
        )
        log.info(f"number of pods, {self.num_of_cephfsplugin_provisioner_pods}")

    def nfs_enable(self):
        """
        Enable nfs feature and ROOK_CSI_ENABLE_NFS
        Steps:
        1:- Enable nfs feature for storage-cluster
        2:- Enable ROOK_CSI_ENABLE_NFS via patch request
        3:- Check nfs-ganesha server is up and running
        4:- Check csi-nfsplugin pods are up and running

        """
        # Enable nfs feature for storage-cluster using patch command
        assert self.storage_cluster_obj.patch(
            resource_name="ocs-storagecluster",
            params=self.nfs_spec_enable,
            format_type="merge",
        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"

        # Enable ROOK_CSI_ENABLE_NFS via patch request
        assert self.config_map_obj.patch(
            resource_name="rook-ceph-operator-config",
            params=self.rook_csi_config_enable,
            format_type="merge",
        ), "configmap/rook-ceph-operator-config not patched"

        # Check nfs-ganesha server is up and running
        assert self.pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-nfs",
            dont_allow_other_resources=True,
            timeout=60,
        )

        # Check csi-nfsplugin and csi-nfsplugin-provisioner pods are up and running
        assert self.pod_obj.wait_for_resource(
            resource_count=self.num_of_cephfsplugin_pods,
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin",
            dont_allow_other_resources=True,
            timeout=60,
        )

        assert self.pod_obj.wait_for_resource(
            resource_count=self.num_of_cephfsplugin_provisioner_pods,
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin-provisioner",
            dont_allow_other_resources=True,
            timeout=60,
        )

        exec_cmd(cmd="mkdir " + self.test_folder)

    def nfs_disable(self):
        """
        Steps:
        1. oc patch -n openshift-storage storageclusters.ocs.openshift.io ocs-storagecluster
        --patch '{"spec": {"nfs":{"enable": false}}}' --type merge
        2. oc patch cm rook-ceph-operator-config -n openshift-storage -p $'data:\n "ROOK_CSI_ENABLE_NFS":  "false"'
        3. manually delete CephNFS, ocs nfs Service, nfs-ganesha pod and the nfs StorageClass
        """

        exec_cmd(cmd="rm -rf " + self.test_folder)
        assert self.storage_cluster_obj.patch(
            resource_name="ocs-storagecluster",
            params=self.nfs_spec_disable,
            format_type="merge",
        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"

        # Enable ROOK_CSI_ENABLE_NFS via patch request
        assert self.config_map_obj.patch(
            resource_name="rook-ceph-operator-config",
            params=self.rook_csi_config_disable,
            format_type="merge",
        ), "configmap/rook-ceph-operator-config not patched"

        # Delete the nfs StorageClass
        cmd_delete_nfs_sc = "delete sc " + self.nfs_sc
        self.storage_cluster_obj.exec_oc_cmd(cmd_delete_nfs_sc)

        # Delete CephNFS
        cmd_delete_cephnfs = "delete CephNFS ocs-storagecluster-cephnfs"
        self.storage_cluster_obj.exec_oc_cmd(cmd_delete_cephnfs)

        # Delete nfs-ganesha pod
        pod_objs = pod.get_all_pods(
            namespace=self.namespace, selector=["rook-ceph-nfs"]
        )
        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 180
            ), f"Pod {pod_obj.name} is not deleted"

    # def teardown(self):
    #     """
    #     Delete cephnfs CR,ocs nfs service and nfs storageclass
    #     Remove test_nfs folder
    #     """
    #     exec_cmd(cmd="rm -rf "+ self.test_folder)
    #     self.nfs_disable()

    def test_nfs_not_enabled_by_default(
        self,
    ):
        """
        This test is to validate nfs feature is not enabled by default for  ODF(4.11) clusters

        Steps:
        1:- Check cephnfs resources not available by default

        """
        # Checks cephnfs resources not available by default
        cephnfs_resource = self.storage_cluster_obj.exec_oc_cmd("get cephnfs")
        if cephnfs_resource is None:
            log.info(f"No resources found in openshift-storage namespace.")

    def test_nfs_feature_enable(
        self,
    ):
        """
        This test is to validate nfs feature enable after deployment of  ODF(4.11) cluster

        Steps:
        1:- Check cephnfs resources not available by default
        2:- Enable nfs feature for storage-cluster
        3:- Check cephnfs resource running

        """
        # Checks cephnfs resources not available by default
        cephnfs_resource = self.storage_cluster_obj.exec_oc_cmd("get cephnfs")
        if cephnfs_resource is None:
            log.info(f"No resources found in openshift-storage namespace.")

        # Enable nfs feature for storage-cluster
        self.nfs_enable()

        # Check cephnfs resource running
        cephnfs_resource_status = self.storage_cluster_obj.exec_oc_cmd(
            "get CephNFS ocs-storagecluster-cephnfs --output jsonpath='{.status.phase}'"
        )
        assert cephnfs_resource_status == "Ready"

        self.nfs_disable()

    def test_incluster_nfs_export(
        self,
        pod_factory,
    ):
        """
        This test is to validate NFS export using a PVC mounted on an app pod (in-cluster)

        Steps:
        1:- Enable nfs feature for storage-cluster
        2:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        3:- Check the pvc are in BOUND status
        4:- Create pods with nfs pvcs mounted
        5:- Run IO
        6:- Wait for IO completion
        7:- Verify presence of the file
        8:- Deletion of Pods and PVCs

        """
        # Enable nfs feature for storage-cluster
        self.nfs_enable()

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

        self.nfs_disable()

    def test_outcluster_nfs_export(
        self,
        pod_factory,
    ):
        """
        This test is to validate export where the export is consumed from outside the Openshift cluster
        - Create a LoadBalancer Service pointing to the CephNFS server
        - Direct external NFS clients to the Service endpoint from the step above

        Steps:
        1:- Enable nfs feature for storage-cluster
        2:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        3:- Check the pvc are in BOUND status
        4:- Fetch sharing details for the nfs pvc
        5:- Create nginx pod with nfs pvcs mounted
        6:- Run IO
        7:- Wait for IO completion
        8:- Verify presence of the file
        9:- Create /var/lib/www/html/index.html file
        10:- Create loadbalancer service for nfs
        11:- Fetch ingress address details for the nfs loadbalancer service
        12:- Connect the external client using the share path and ingress address
        13:- Deletion of Pods and PVCs

        """
        # Enable nfs feature for storage-cluster
        self.nfs_enable()

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_obj = helpers.create_pvc(
            sc_name=self.nfs_sc,
            pvc_name="nfs-pvc",
            namespace=self.namespace,
            size="5Gi",
            do_reload=True,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode="Filesystem",
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

        # Create nginx pod with nfs pvcs mounted
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=nfs_pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=constants.NGINX_POD_YAML,
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

        # Create /var/lib/www/html/index.html file inside the pod
        command = (
            f"bash -c "
            + '"echo '
            + "'hello world'"
            + '  > /var/lib/www/html/index.html"'
        )
        pod_obj.exec_cmd_on_pod(
            command=command,
            out_yaml_format=False,
        )

        # Create loadbalancer service for nfs
        log.info("----create loadbalancer service----")
        service = """
            apiVersion: v1
            kind: Service
            metadata:
              name: rook-ceph-nfs-my-nfs-load-balancer
              namespace: openshift-storage
            spec:
              ports:
              - name: nfs
                port: 2049
              type: LoadBalancer
              externalTrafficPolicy: Local
              selector:
                app: rook-ceph-nfs
                ceph_nfs: ocs-storagecluster-cephnfs
            """

        nfs_service_data = yaml.safe_load(service)
        log.info(nfs_service_data)
        with open("nfs_service.yaml", "w") as file:
            yaml.dump(nfs_service_data, file)
        print(open("nfs_service.yaml").read())
        create_nfs_service_cmd = "create -f nfs_service.yaml"
        res = run_oc_command(cmd=create_nfs_service_cmd, namespace=self.namespace)
        if ERRMSG in res[0]:
            err_msg = f"Failed to create service : {res}"
            log.error(err_msg)
            raise Exception(err_msg)

        # Fetch ingress address details for the nfs loadbalancer service
        ingress_add = self.service_obj.exec_oc_cmd(
            "get service rook-ceph-nfs-my-nfs-load-balancer --output jsonpath='{.status.loadBalancer.ingress}'"
        )
        hostname = ingress_add[0]
        hostname_add = hostname["hostname"]
        log.info(f"ingress address, {ingress_add}")
        log.info(f"ingress hostname, {hostname_add}")

        # Connect the external client using the share path and ingress address
        export_nfs_external_cmd = (
            "sudo mount -t nfs4 -o proto=tcp "
            + hostname_add
            + ":"
            + share_details
            + " "
            + self.test_folder
        )
        exec_cmd(cmd=export_nfs_external_cmd)
        assert exec_cmd(cmd="echo $?") == 0

        # Delete ocs nfs Service
        cmd_delete_nfs_service = "delete service rook-ceph-nfs-my-nfs-load-balancer"
        self.storage_cluster_obj.exec_oc_cmd(cmd_delete_nfs_service)

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

        self.nfs_disable()
