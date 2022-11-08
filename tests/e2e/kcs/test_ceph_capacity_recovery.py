import logging
import pytest
import itertools

from ocs_ci.ocs.perftests import PASTest

from ocs_ci.ocs import node, constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import PodNotCreated
from ocs_ci.ocs.resources import pvc, ocs
from ocs_ci.ocs.cluster import (
    get_percent_used_capacity,
    get_osd_utilization,
    get_ceph_df_detail,
)


log = logging.getLogger(__name__)

class TestCephCapacityRecovery(PASTest):

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        # Run the test in its own project (namespace)
        self.create_test_project()

        self.interface = "CephFileSystem"

        super(TestCephCapacityRecovery, self).setup()

        # Getting the total Storage capacity
        try:
            self.ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        except Exception as err:
            err_msg = f"Failed to get Storage capacity : {err}"
            log.error(err_msg)
            raise Exception(err_msg)

        log.info(f"Working on cluster {self.ceph_cluster.cluster_name} with capacity {self.ceph_capacity}")
        self.num_of_pvcs = 200
        self.pvc_size = self.ceph_capacity/self.num_of_pvcs
        self.pvc_size_str = str(self.pvc_size) + "Gi"
        log.info(f"Creating pvs of {self.pvc_size_str} size")

    def teardown(self):
        """
        Cleanup the test environment
        """
        log.info("Starting the test cleanup")

        # Deleting the namespace used by the test
        self.delete_test_project()

        super(TestCephCapacityRecovery, self).teardown()

    def test_capacity_recovery(
        self,
    ):
        log.info(f"Ceph Recovery test start")

        helpers.pull_images(constants.PERF_IMAGE)

        worker_nodes_list = node.get_worker_nodes()
        assert len(worker_nodes_list) > 1
        node_one = worker_nodes_list[0]

        self.sc_obj = ocs.OCS(
            kind="StorageCluster",
            metadata={
                "namespace": self.namespace,
                "name": constants.CEPHFILESYSTEM_SC,
            },
        )

        pvc_list = []
        pod_list = []
        for i in range(5):
            index = i+1

            log.info(f"Start creating PVC")
            pvc_obj = helpers.create_pvc(
                sc_name=self.sc_obj.name, size=self.pvc_size_str, namespace=self.namespace, access_mode=constants.ACCESS_MODE_RWX
            )
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)

            log.info(f"PVC {pvc_obj.name} was successfully created in namespace {self.namespace}.")
            # Create a pod on one node
            log.info(f"Creating Pod with pvc {pvc_obj.name} on node {node_one}")

            pvc_obj.reload()

            try:
                pod_obj = helpers.create_pod(
                    interface_type=self.interface,
                    pvc_name=pvc_obj.name,
                    namespace=pvc_obj.namespace,
                    node_name=node_one,
                    pod_dict_path=constants.PERF_POD_YAML
                )
            except Exception as e:
                log.error(
                    f"Pod on PVC {pvc_obj.name} was not created, exception {str(e)}"
                )
                raise PodNotCreated("Pod on PVC was not created.")

            # Confirm that pod is running on the selected_nodes
            helpers.wait_for_resource_state(
                resource=pod_obj, state=constants.STATUS_RUNNING, timeout=600
            )
            pvc_list.append(pvc_obj)
            pod_list.append(pod_obj)

            file_name = f"{pod_obj.name}-ceph_capacity_recovery"
            log.info(f"Starting IO on the POD {pod_obj.name}")

            log.info(f"Obj str size is {self.pvc_size_str}")
            filesize = int(float(self.pvc_size_str[:-2]) * 0.70)
            # Change the file size to MB for the FIO function
            file_size = f"{filesize * constants.GB2MB}M"


            pod_obj.fillup_fs(size=file_size, fio_filename=file_name, performance_pod=True)

            log.info(f"Start creation of clone number for pvc number {index}.")
            #start_time = self.get_time("csi")
            cloned_pvc_obj = pvc.create_pvc_clone(
                sc_name=pvc_obj.backed_sc,
                parent_pvc=pvc_obj.name,
                pvc_name=f"clone-pas-test-{index}",
                clone_yaml=constants.CSI_CEPHFS_PVC_CLONE_YAML,
                namespace=pvc_obj.namespace,
                storage_size=self.pvc_size_str,
            )
            helpers.wait_for_resource_state(
                cloned_pvc_obj, constants.STATUS_BOUND, 1200
            )
            log.info(f"Finished successfully creation of clone for pvc number {index}.")

            snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML
            snap_sc_name = helpers.default_volumesnapshotclass(constants.CEPHFILESYSTEM).name

            snap_name = pvc_obj.name.replace("pvc-test", f"snapshot-test{index}")

            snap_obj = pvc.create_pvc_snapshot(
                pvc_name=pvc_obj.name,
                snap_yaml=snap_yaml,
                snap_name=snap_name,
                namespace=pvc_obj.namespace,
                sc_name=snap_sc_name
            )

            # Wait until the snapshot is bound and ready to use
            snap_obj.ocp.wait_for_resource(
                condition="true",
                resource_name=snap_name,
                column=constants.STATUS_READYTOUSE,
                timeout=1200,
            )

            log.info(f"Finished successfully creation of snapshot number {index} .")

        for (pod_obj, pvc_obj) in zip(pod_list, pvc_list):
            log.info(f"Deleting the test POD : {pod_obj.name}")
            try:
                pod_obj.delete()
                log.info("Wait until the pod is deleted.")
                pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)
            except Exception as ex:
                log.error(f"Cannot delete the test pod : {ex}")

            # Deleting the PVC which used in the test.
            log.info(f"Delete the PVC : {pvc_obj.name}")
            try:
                pvc_obj.delete()
                log.info("Wait until the pvc is deleted.")
                pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)
            except Exception as ex:
                log.error(f"Cannot delete the test pvc : {ex}")


    def verify_osd_used_capacity_greater_than_expected(self, expected_used_capacity):
        """
        Verify OSD percent used capacity greate than ceph_full_ratio

        Args:
            expected_used_capacity (float): expected used capacity

        Returns:
             bool: True if used_capacity greater than expected_used_capacity, False otherwise

        """
        used_capacity = get_percent_used_capacity()
        log.info(f"Used Capacity is {used_capacity}%")
        ceph_df_detail = get_ceph_df_detail()
        log.info(f"ceph df detail: {ceph_df_detail}")
        osds_utilization = get_osd_utilization()
        log.info(f"osd utilization: {osds_utilization}")
        for osd_id, osd_utilization in osds_utilization.items():
            if osd_utilization > expected_used_capacity:
                log.info(f"OSD ID:{osd_id}:{osd_utilization} greater than 85%")
                return True
        return False
