import pytest
import logging
import yaml


from ocs_ci.ocs import api_client, constants, ocp
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

from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import (
    create_pods,
)

log = logging.getLogger(__name__)


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
        self.rook_ceph_cm_obj = ocp.OCP(kind="Configmap", namespace=self.namespace)
        self.nfs_plugin_pod_obj = ocp.OCP(kind="Pod", namespace=self.namespace)
        self.nfs_plugin_provisioner_pod_obj = ocp.OCP(
            kind="Pod", namespace=self.namespace
        )
        self.nfs_ganesha_pod_obj = ocp.OCP(kind=constants.POD, namespace=self.namespace)

        self.nfs_sc = "ocs-storagecluster-ceph-nfs"

        self.nfs_spec_enable = '{"spec": {"nfs":{"enable": true}}}'
        self.rook_csi_config_enable = '{"data":{"ROOK_CSI_ENABLE_NFS": "true"}}'
        self.nfs_spec_disable = '{"spec": {"nfs":{"enable": false}}}'
        self.rook_csi_config_disable = '{"data":{"ROOK_CSI_ENABLE_NFS": "false"}}'

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

        """
        # Enable nfs feature for storage-cluster using patch command
        assert self.storage_cluster_obj.patch(
            resource_name="ocs-storagecluster",
            params=self.nfs_spec_enable,
            format_type="merge",
        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"

        # Enable ROOK_CSI_ENABLE_NFS via patch request
        assert self.rook_ceph_cm_obj.patch(
            resource_name="rook-ceph-operator-config",
            params=self.rook_csi_config_enable,
            format_type="merge",
        ), "configmap/rook-ceph-operator-config not patched"

    def nfs_disable(self):
        """
        Steps:
        1. oc patch -n openshift-storage storageclusters.ocs.openshift.io ocs-storagecluster
        --patch '{"spec": {"nfs":{"enable": false}}}' --type merge
        2. oc patch cm rook-ceph-operator-config -n openshift-storage -p $'data:\n "ROOK_CSI_ENABLE_NFS":  "false"'
        3. manually delete CephNFS, ocs nfs Service, nfs-ganesha pod and the nfs StorageClass
        """
        assert self.storage_cluster_obj.patch(
            resource_name="ocs-storagecluster",
            params=self.nfs_spec_disable,
            format_type="merge",
        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"

        # Enable ROOK_CSI_ENABLE_NFS via patch request
        assert self.rook_ceph_cm_obj.patch(
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

        # Delete ocs nfs Service
        cmd_delete_nfs_service = "delete service rook-ceph-nfs-my-nfs-load-balancer"
        self.storage_cluster_obj.exec_oc_cmd(cmd_delete_nfs_service)

        # Delete nfs-ganesha pod
        self.nfs_ganesha_pod_obj.delete()
        self.nfs_ganesha_pod_obj.ocp.wait_for_delete(
            self.nfs_ganesha_pod_obj.name, 180
        ), f"Pod {self.nfs_ganesha_pod_obj.name} is not deleted"

    def teardown(self):
        """
        Delete cephnfs CR,ocs nfs service and nfs storageclass
        """
        self.nfs_disable()

    def test_nfs_feature_enable(
        self,
        pod_factory,
    ):
        """
        This test is to validate nfs feature enable after deployment of  ODF(4.11) cluster

        Steps:
        1:- Check cephnfs resources not available by default
        2:- Enable nfs feature for storage-cluster
        3:- Check nfs-ganesha server is up and running
        4:- Check csi-nfsplugin pods are up and running

        """
        # Checks cephnfs resources not available by default
        cephnfs_resource = self.storage_cluster_obj.exec_oc_cmd("get cephnfs")
        if cephnfs_resource is None:
            log.info(f"No resources found in openshift-storage namespace.")

        # Enable nfs feature for storage-cluster
        self.nfs_enable()

        # Check nfs-ganesha server is up and running
        assert self.nfs_ganesha_pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-nfs",
            dont_allow_other_resources=True,
            timeout=60,
        )

        # Check csi-nfsplugin and csi-nfsplugin-provisioner pods are up and running
        assert self.nfs_plugin_pod_obj.wait_for_resource(
            resource_count=self.num_of_cephfsplugin_pods,
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin",
            dont_allow_other_resources=True,
            timeout=60,
        )

        assert self.nfs_plugin_provisioner_pod_obj.wait_for_resource(
            resource_count=self.num_of_cephfsplugin_provisioner_pods,
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin-provisioner",
            dont_allow_other_resources=True,
            timeout=60,
        )

    def test_incluster_nfs_export(
        self,
        pod_factory,
    ):
        """
        This test is to validate nfs feature enable after deployment of  ODF(4.11) cluster

        Steps:
        1:- Check cephnfs resources not available by default
        2:- Enable nfs feature for storage-cluster
        3:- Check nfs-ganesha server is up and running
        4:- Check csi-nfsplugin pods are up and running
        5:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        6:- Check the pvc are in BOUND status
        7:- Create pods with nfs pvcs mounted
        8:- Run IO
        9:- Wait for IO completion
        10:- Verify presence of the file
        11:- Deletion of Pods and PVCs

        """
        # Enable nfs feature for storage-cluster
        self.nfs_enable()

        # Check nfs-ganesha server is up and running
        assert self.nfs_ganesha_pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-nfs",
            dont_allow_other_resources=True,
            timeout=60,
        )

        # Check csi-nfsplugin and csi-nfsplugin-provisioner pods are up and running
        assert self.nfs_plugin_pod_obj.wait_for_resource(
            resource_count=self.num_of_cephfsplugin_pods,
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin",
            dont_allow_other_resources=True,
            timeout=60,
        )

        assert self.nfs_plugin_provisioner_pod_obj.wait_for_resource(
            resource_count=self.num_of_cephfsplugin_provisioner_pods,
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin-provisioner",
            dont_allow_other_resources=True,
            timeout=60,
        )

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_objs, yaml_creation_dir = helpers.create_multiple_pvcs(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            number_of_pvc=1,
            size="5Gi",
            burst=False,
            do_reload=True,
        )

        # Check the pvc are in BOUND status
        for pvc_obj in nfs_pvc_objs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=60
            )
            pvc_obj.reload()

        # Create pods with nfs pvcs mounted
        pod_objs_with_nfs_pvc = create_pods(
            nfs_pvc_objs,
            pod_factory,
            constants.CEPHFILESYSTEM,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )

        for pod_obj in pod_objs_with_nfs_pvc:
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
        for pod_obj in pod_objs_with_nfs_pvc:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(
                pod_obj.name, 180
            ), f"Pod {pod_obj.name} is not deleted"

        log.info("Deleting PVC")
        for pvc_obj in nfs_pvc_objs:
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(
                resource_name=pvc_obj.name
            ), f"PVC {pvc_obj.name} is not deleted"
            log.info(f"Verified: PVC {pvc_obj.name} is deleted.")

    def test_outcluster_nfs_export(
        self,
    ):
        """
        This test is to validate nfs feature enable after deployment of  ODF(4.11) cluster

        Steps:
        1:- Check cephnfs resources not available by default
        2:- Enable nfs feature for storage-cluster
        3:- Check nfs-ganesha server is up and running
        4:- Check csi-nfsplugin pods are up and running
        5:- Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        6:- Check the pvc are in BOUND status
        7:- Create nginx pod with nfs pvcs mounted
        8:- Run IO
        9:- Wait for IO completion
        10:- Verify presence of the file
        11:- Create /var/lib/www/html/index.html file
        12:- Create loadbalancer service for nfs
        13:- Fetch sharing details for the nfs pvc
        14:- Fetch ingress address details for the nfs loadbalancer service
        15:- Connect the external client using the share path and ingress address
        16:- Deletion of Pods and PVCs

        """
        # Enable nfs feature for storage-cluster
        self.nfs_enable()

        # Check nfs-ganesha server is up and running
        assert self.nfs_ganesha_pod_obj.wait_for_resource(
            resource_count=1,
            condition=constants.STATUS_RUNNING,
            selector="app=rook-ceph-nfs",
            dont_allow_other_resources=True,
            timeout=60,
        )

        # Check csi-nfsplugin and csi-nfsplugin-provisioner pods are up and running
        assert self.nfs_plugin_pod_obj.wait_for_resource(
            resource_count=self.num_of_cephfsplugin_pods,
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin",
            dont_allow_other_resources=True,
            timeout=60,
        )

        assert self.nfs_plugin_provisioner_pod_obj.wait_for_resource(
            resource_count=self.num_of_cephfsplugin_provisioner_pods,
            condition=constants.STATUS_RUNNING,
            selector="app=csi-nfsplugin-provisioner",
            dont_allow_other_resources=True,
            timeout=60,
        )

        # Create nfs pvcs with storageclass ocs-storagecluster-ceph-nfs
        nfs_pvc_objs, yaml_creation_dir = helpers.create_multiple_pvcs(
            sc_name=self.nfs_sc,
            namespace=self.namespace,
            number_of_pvc=1,
            size="5Gi",
            burst=False,
            do_reload=True,
        )

        # Check the pvc are in BOUND status
        for pvc_obj in nfs_pvc_objs:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=60
            )
            pvc_obj.reload()

            # Create nginx pod with nfs pvcs mounted
            pod_obj = helpers.create_pod(
                interface_type=constants.CEPHFILESYSTEM,
                pvc_name=pvc_obj.name,
                namespace=pvc_obj.namespace,
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

            # Create loadbalancer service for nfs
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

        service_data = yaml.safe_load(service)
        res = api_client.client.create_service(
            body=service_data, namespace=self.namespace
        )
        log.info(res)
        log.info(f"Created service: {res['metadata']['name']}")

        # Deletion of Pods and PVCs
        log.info(f"Deleting pod")
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(
            pod_obj.name, 180
        ), f"Pod {pod_obj.name} is not deleted"

        log.info("Deleting PVC")
        for pvc_obj in nfs_pvc_objs:
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(
                resource_name=pvc_obj.name
            ), f"PVC {pvc_obj.name} is not deleted"
            log.info(f"Verified: PVC {pvc_obj.name} is deleted.")
