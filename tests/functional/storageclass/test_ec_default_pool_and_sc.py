import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    skipif_ec_pools_disabled,
)
from ocs_ci.framework.testlib import ManageTest, tier1, tier2
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@pytest.fixture()
def restore_default_sc_annotation(request):
    """Ensure default SC annotation is restored after test."""
    rbd_sc_name = constants.DEFAULT_STORAGECLASS_RBD

    def finalizer():
        current_defaults = helpers.get_default_storage_class()
        if rbd_sc_name not in current_defaults:
            log.info(f"Finalizer: restoring '{rbd_sc_name}' as default SC")
            helpers.change_default_storageclass(rbd_sc_name)

    request.addfinalizer(finalizer)


@green_squad
@skipif_ec_pools_disabled
class TestECDefaultPoolAndSC(ManageTest):
    """
    Verify ODF internal component pool usage and default StorageClass
    behavior on EC-only clusters.
    """

    @tier1
    @pytest.mark.polarion_id("OCS-8032")
    def test_ec_internal_components_and_default_sc_pvc_binding(
        self, pvc_factory, pod_factory
    ):
        """
        Steps:
        1. Enumerate all PVCs in openshift-storage and log their StorageClass
        2. Check NooBaa DB PVC StorageClass
        3. Verify EC RBD SC is marked as default
        4. Create PVC without storageClassName — verify it binds to default EC SC
        5. Create pod and write ~1 GB
        6. Verify RBD image exists in the replicated metadata pool
        7. Verify data usage increased on the EC data pool
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        rbd_sc_name = constants.DEFAULT_STORAGECLASS_RBD

        # Step 1: Enumerate all PVCs in openshift-storage and verify all are Bound
        all_pvc_objs = pvc.get_all_pvc_objs(namespace=namespace)
        assert all_pvc_objs, "No PVCs found in openshift-storage namespace"
        log.info("=== PVC inventory in openshift-storage ===")
        not_bound = []
        for pvc_obj in all_pvc_objs:
            status = pvc_obj.get().get("status", {}).get("phase", "Unknown")
            log.info(
                f"  PVC: {pvc_obj.name}, StorageClass: {pvc_obj.backed_sc}, "
                f"Status: {status}"
            )
            if status != constants.STATUS_BOUND:
                not_bound.append(pvc_obj.name)
        assert not not_bound, f"PVCs not in Bound state: {not_bound}"

        # Step 2: Check NooBaa DB PVC uses the default RBD StorageClass
        noobaa_pvcs = pvc.get_pvc_objs(
            pvc_names=[constants.NOOBAA_DB_PVC_NAME],
            namespace=namespace,
        )
        assert noobaa_pvcs, (
            f"NooBaa DB PVC '{constants.NOOBAA_DB_PVC_NAME}' not found "
            f"in namespace '{namespace}'"
        )
        noobaa_sc = noobaa_pvcs[0].backed_sc
        log.info(
            f"NooBaa DB PVC '{constants.NOOBAA_DB_PVC_NAME}' "
            f"uses StorageClass: {noobaa_sc}"
        )
        assert noobaa_sc == rbd_sc_name, (
            f"NooBaa DB PVC uses StorageClass '{noobaa_sc}', "
            f"expected '{rbd_sc_name}'"
        )

        # Step 2b: Check Prometheus and Alertmanager PVCs use EC data pool
        monitoring_ns = defaults.OCS_MONITORING_NAMESPACE
        monitoring_pvc_objs = pvc.get_all_pvc_objs(namespace=monitoring_ns)
        assert monitoring_pvc_objs, f"No monitoring PVCs found in '{monitoring_ns}'"
        log.info("=== Monitoring PVC inventory ===")
        wrong_sc_pvcs = []
        for mon_pvc in monitoring_pvc_objs:
            mon_sc = mon_pvc.backed_sc
            log.info(f"  PVC: {mon_pvc.name}, StorageClass: {mon_sc}")
            if mon_sc != rbd_sc_name:
                wrong_sc_pvcs.append((mon_pvc.name, mon_sc))
        assert not wrong_sc_pvcs, (
            f"Monitoring PVCs not using EC RBD SC '{rbd_sc_name}': " f"{wrong_sc_pvcs}"
        )
        log.info(
            f"Verified: All {len(monitoring_pvc_objs)} monitoring PVCs "
            f"use EC RBD SC '{rbd_sc_name}'"
        )

        # Step 3: Verify EC RBD SC is marked as default
        default_scs = helpers.get_default_storage_class()
        log.info(f"Default StorageClasses: {default_scs}")
        assert rbd_sc_name in default_scs, (
            f"EC RBD SC '{rbd_sc_name}' is not marked as default. "
            f"Current default SCs: {default_scs}"
        )

        # Step 4: Create PVC without storageClassName
        pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
        pvc_data["metadata"]["name"] = helpers.create_unique_resource_name(
            "ec-default", "pvc"
        )
        log.info("Creating PVC without storageClassName to test default SC binding")
        del pvc_data["spec"]["storageClassName"]
        pvc_obj = pvc_factory(custom_data=pvc_data, status=constants.STATUS_BOUND)
        assert pvc_obj, "PVC creation failed"

        bound_sc = pvc_obj.get().get("spec").get("storageClassName")
        log.info(f"PVC bound to StorageClass: {bound_sc}")
        assert bound_sc == rbd_sc_name, (
            f"PVC without storageClassName bound to '{bound_sc}', "
            f"expected default EC RBD SC '{rbd_sc_name}'"
        )

        # Step 5: Capture baseline EC data pool usage
        metadata_pool = helpers.default_ceph_block_pool()
        data_pool = helpers.get_data_pool_name()
        log.info(f"Metadata pool: {metadata_pool}, Data pool (EC): {data_pool}")

        used_before_io = helpers.fetch_used_size(data_pool)
        log.info(f"Baseline EC data pool usage: {used_before_io} GB")

        # Step 6: Create pod and write ~1 GB
        pod_obj = pod_factory(pvc=pvc_obj, status=constants.STATUS_RUNNING)
        log.info(f"Pod '{pod_obj.name}' created and running")

        pod.run_io_and_verify_mount_point(pod_obj, bs="10M", count="100")
        log.info("IO completed (~1 GB written)")

        # Step 7: Verify RBD image exists in metadata pool
        rbd_images = pod.list_ceph_images(pool_name=metadata_pool)
        log.info(f"RBD images in metadata pool '{metadata_pool}': {rbd_images}")
        pvc_obj.reload()
        image_uuid = pvc_obj.image_uuid
        found = any(image_uuid in img for img in rbd_images)
        assert found, (
            f"RBD image for PVC (uuid={image_uuid}) not found in "
            f"metadata pool '{metadata_pool}'. Images: {rbd_images}"
        )
        log.info(
            f"Verified: RBD image (uuid={image_uuid}) exists in "
            f"metadata pool '{metadata_pool}'"
        )

        # Step 8: Verify EC data pool usage increased
        used_after_io = helpers.fetch_used_size(data_pool)
        log.info(f"Post-IO EC data pool usage: {used_after_io} GB")
        assert used_after_io > used_before_io, (
            f"EC data pool '{data_pool}' usage did not increase after IO. "
            f"Before: {used_before_io} GB, After: {used_after_io} GB"
        )
        log.info(
            f"Verified: EC data pool usage increased from "
            f"{used_before_io} GB to {used_after_io} GB"
        )

    @tier2
    @pytest.mark.polarion_id("OCS-8033")
    def test_ec_default_sc_reconciliation(self, restore_default_sc_annotation):
        """
        Steps:
        1. Remove default annotation from EC RBD StorageClass
        2. Verify SC is no longer default
        3. Wait for operator reconciliation
        4. Document whether operator restores the default label
        """
        rbd_sc_name = constants.DEFAULT_STORAGECLASS_RBD

        # Verify initial state
        initial_defaults = helpers.get_default_storage_class()
        assert rbd_sc_name in initial_defaults, (
            f"'{rbd_sc_name}' is not default before test. "
            f"Current defaults: {initial_defaults}"
        )

        # Step 1: Remove default annotation
        log.info(f"Removing default annotation from '{rbd_sc_name}'")
        ocp_obj = OCP(kind="StorageClass")
        patch = (
            ' \'{"metadata": {"annotations":'
            '{"storageclass.kubernetes.io/is-default-class"'
            ':"false"}}}\' '
        )
        patch_cmd = f"patch storageclass {rbd_sc_name} -p" + patch
        ocp_obj.exec_oc_cmd(command=patch_cmd)

        # Step 2: Verify SC is no longer default
        current_defaults = helpers.get_default_storage_class()
        assert (
            rbd_sc_name not in current_defaults
        ), f"'{rbd_sc_name}' still has default annotation after removal"
        log.info(f"Confirmed: '{rbd_sc_name}' is no longer default")

        # Step 3: Wait for operator reconciliation
        reconciled = False
        log.info("Waiting up to 300s for operator to reconcile default SC annotation")
        try:
            for default_scs in TimeoutSampler(
                timeout=300,
                sleep=15,
                func=helpers.get_default_storage_class,
            ):
                if rbd_sc_name in default_scs:
                    log.info(
                        f"Operator reconciled: '{rbd_sc_name}' restored as default"
                    )
                    reconciled = True
                    break
        except TimeoutExpiredError:
            log.warning(
                f"Operator did NOT restore default annotation on '{rbd_sc_name}' "
                f"within 300 seconds"
            )

        # Step 4: Document result
        if reconciled:
            log.info(
                "RESULT: ODF operator DOES reconcile the default SC annotation. "
                "Removing the is-default-class annotation is automatically restored."
            )
        else:
            log.info(
                "RESULT: ODF operator does NOT reconcile the default SC annotation. "
                "Manual removal of the annotation is persistent. "
                "Restoring via finalizer."
            )

        # Final verification: default is restored (by operator or finalizer)
        final_defaults = helpers.get_default_storage_class()
        if rbd_sc_name not in final_defaults:
            log.info(f"Restoring '{rbd_sc_name}' as default SC manually")
            helpers.change_default_storageclass(rbd_sc_name)
            final_defaults = helpers.get_default_storage_class()

        assert (
            rbd_sc_name in final_defaults
        ), f"'{rbd_sc_name}' is not default after test completion"
