import logging

import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad, skipif_no_nvmeof
from ocs_ci.framework.testlib import ManageTest, tier1, polarion_id
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.ocs import OCS

logger = logging.getLogger(__name__)


@green_squad
@tier1
@skipif_no_nvmeof
class TestNvmeofPvc(ManageTest):
    """
    Tests for basic PVC lifecycle and data integrity using the NVMe-oF
    (NVMe over Fabrics) StorageClass.
    """

    @pytest.fixture(autouse=True)
    def nvmeof_prerequisites(self):
        """
        Verify NVMe-oF prerequisites before running the test:
            - NVMe-oF Gateway pods are deployed and healthy (Running).
            - NVMe-oF StorageClass exists.

        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # NVMe-oF StorageClass must exist
        sc_ocp_obj = OCP(kind=constants.STORAGECLASS, namespace=namespace)
        assert sc_ocp_obj.is_exist(resource_name=constants.CEPH_NVMEOF_SC), (
            f"NVMe-oF StorageClass {constants.CEPH_NVMEOF_SC} does not exist. "
            "Ensure the StorageCluster was deployed with nvmeof enabled."
        )
        logger.info("NVMe-oF StorageClass %s exists", constants.CEPH_NVMEOF_SC)

        # NVMe-oF Gateway pods must be deployed and healthy
        gateway_pods = pod.get_pods_having_label(
            label=constants.NVMEOF_APP_LABEL, namespace=namespace
        )
        assert gateway_pods, (
            "No NVMe-oF Gateway pods found with label "
            f"{constants.NVMEOF_APP_LABEL} in namespace {namespace}"
        )
        gateway_pod_names = [pod_data["metadata"]["name"] for pod_data in gateway_pods]
        logger.info("Found NVMe-oF Gateway pods: %s", gateway_pod_names)
        assert pod.wait_for_pods_to_be_running(
            namespace=namespace, pod_names=gateway_pod_names, timeout=300
        ), "NVMe-oF Gateway pods are not in Running state"
        logger.info("All NVMe-oF Gateway pods are healthy (Running)")

    @pytest.fixture()
    def nvmeof_storageclass(self):
        """
        Return the existing NVMe-oF StorageClass as an OCS object so that it can
        be consumed by the pvc_factory fixture.

        Returns:
            OCS: OCS instance of the NVMe-oF StorageClass

        """
        sc_ocp_obj = OCP(
            kind=constants.STORAGECLASS,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.CEPH_NVMEOF_SC,
        )
        return OCS(**sc_ocp_obj.get())

    @polarion_id("OCS-8237")
    def test_nvmeof_pvc_data_integrity_and_reclaim(
        self, nvmeof_storageclass, pvc_factory, pod_factory
    ):
        """
        Verify PVC lifecycle and data integrity on the NVMe-oF StorageClass.

        Steps:
            1. Create a RWO PVC using the NVMe-oF StorageClass and verify it
               reaches the Bound state.
            2. Create a pod that mounts the PVC, write data with fio and verify
               data integrity by comparing md5sum after re-reading.
            3. Delete the pod and the PVC.
            4. Verify the PV is reclaimed according to its reclaim policy.

        """
        # Step 1: Create a RWO PVC using the NVMe-oF StorageClass
        logger.info(
            "Creating a RWO PVC using StorageClass %s", constants.CEPH_NVMEOF_SC
        )
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            storageclass=nvmeof_storageclass,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        logger.info("PVC %s reached Bound state", pvc_obj.name)

        # Capture PV details and reclaim policy before deletion
        pv_obj = pvc_obj.backed_pv_obj
        pv_name = pv_obj.name
        reclaim_policy = pvc_obj.reclaim_policy
        logger.info(
            "PVC %s is backed by PV %s with reclaim policy %s",
            pvc_obj.name,
            pv_name,
            reclaim_policy,
        )

        # Step 2: Create a pod mounting the PVC, run IO and verify data integrity
        pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        logger.info("Pod %s is running and mounts PVC %s", pod_obj.name, pvc_obj.name)

        file_name = pod_obj.name
        logger.info("Running fio on pod %s to write file %s", pod_obj.name, file_name)
        pod_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            fio_filename=file_name,
            end_fsync=1,
        )
        fio_result = pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert (
            err_count == 0
        ), f"IO error on pod {pod_obj.name}. FIO result: {fio_result}"
        logger.info("fio completed successfully on pod %s", pod_obj.name)

        # Calculate md5sum of the written file and verify data integrity on re-read
        original_md5sum = pod.cal_md5sum(pod_obj, file_name)
        logger.info("Verifying data integrity on pod %s", pod_obj.name)
        assert pod.verify_data_integrity(
            pod_obj, file_name, original_md5sum
        ), f"Data integrity check failed for file {file_name} on pod {pod_obj.name}"
        logger.info("Data integrity verified on pod %s", pod_obj.name)

        # Step 3: Delete the pod, then the PVC
        logger.info("Deleting pod %s", pod_obj.name)
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        logger.info("Deleting PVC %s", pvc_obj.name)
        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        # Step 4: Verify the PV is reclaimed according to its reclaim policy
        if reclaim_policy == constants.RECLAIM_POLICY_DELETE:
            logger.info("Reclaim policy is Delete, verifying PV %s is deleted", pv_name)
            pv_obj.ocp.wait_for_delete(resource_name=pv_name, timeout=180)
            logger.info("PV %s deleted as per Delete reclaim policy", pv_name)
        elif reclaim_policy == constants.RECLAIM_POLICY_RETAIN:
            logger.info(
                "Reclaim policy is Retain, verifying PV %s is Released", pv_name
            )
            helpers.wait_for_resource_state(
                pv_obj, constants.STATUS_RELEASED, timeout=180
            )
            logger.info("PV %s is Released as per Retain reclaim policy", pv_name)
            # Cleanup the retained PV so no leftovers remain. Switch the reclaim
            # policy to Delete so the controller removes the backing volume too,
            # then wait for controller-driven deletion (pvc_factory_fixture pattern).
            patch_param = '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
            pv_obj.ocp.patch(resource_name=pv_name, params=patch_param)
            pv_obj.ocp.wait_for_delete(resource_name=pv_name, timeout=180)
        else:
            pytest.fail(f"Unexpected reclaim policy {reclaim_policy} for PV {pv_name}")
