import logging

import pytest

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.testlib import (
    ignore_leftovers,
    E2ETest,
    tier3,
    skipif_openshift_dedicated,
    skipif_rosa,
    skipif_ocs_version,
    skipif_external_mode,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import (
    modify_statefulset_replica_count,
    validate_pv_delete,
    wait_for_resource_state,
)
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_noobaa_pods, wait_for_storage_pods
from ocs_ci.ocs.resources.pvc import get_pvc_objs, delete_pvcs, create_restore_pvc
from ocs_ci.ocs.resources.ocs import OCS

log = logging.getLogger(__name__)


@tier3
@ignore_leftovers
@pytest.mark.polarion_id("OCS-2605")
@pytest.mark.bugzilla("1924047")
@skipif_ocs_version("<4.6")
@skipif_openshift_dedicated
@skipif_rosa
@skipif_external_mode
class TestNoobaaBackupAndRecovery(E2ETest):
    """
    Test to verify noobaa backup and recovery

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def teardown_fixture(self, request):
        def finalizer():
            # Get the statefulset replica count
            sst_obj = OCP(
                kind=constants.STATEFULSET,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            noobaa_db_sst_obj = sst_obj.get(resource_name=self.noobaa_db_sst_name)
            if noobaa_db_sst_obj["spec"]["replicas"] != 1:
                modify_statefulset_replica_count(
                    statefulset_name=self.noobaa_db_sst_name, replica_count=1
                ), f"Failed to scale up the statefulset {self.noobaa_db_sst_name}"

            try:
                self.restore_pvc_obj.delete()
            except CommandFailed as ex:
                if f'"{ self.restore_pvc_obj.name}" not found' not in str(ex):
                    raise ex

        request.addfinalizer(finalizer)

    def test_noobaa_db_backup_and_recovery(
        self,
        pvc_factory,
        pod_factory,
        snapshot_factory,
        bucket_factory,
        rgw_bucket_factory,
    ):
        """
        Test case to verify noobaa backup and recovery

        1. Take snapshot db-noobaa-db-0 PVC and retore it to PVC
        2. Scale down the statefulset noobaa-db
        3. Get the yaml of the current PVC, db-noobaa-db-0 and
           change the parameter persistentVolumeReclaimPolicy to Retain for restored PVC
        4. Delete both PVCs, the PV for the original claim db-noobaa-db-0 will be removed.
           The PV for claim db-noobaa-db-0-snapshot-restore will move to ‘Released’
        5. Edit again restore PV and remove the claimRef section.
           The volume will transition to Available.
        6. Edit the yaml db-noobaa-db-0.yaml and change the setting volumeName to restored PVC.
        7. Scale up the stateful set again and the pod should be running

        """

        # Initialise variable
        self.noobaa_db_sst_name = "noobaa-db-pg"

        # Get noobaa pods before execution
        noobaa_pods = get_noobaa_pods()

        # Get noobaa PVC before execution
        noobaa_pvc_obj = get_pvc_objs(pvc_names=["db-noobaa-db-pg-0"])
        noobaa_pv_name = noobaa_pvc_obj[0].get("spec").get("spec").get("volumeName")

        # Take snapshot db-noobaa-db-0 PVC
        log.info(f"Creating snapshot of the {noobaa_pvc_obj[0].name} PVC")
        snap_obj = snapshot_factory(
            pvc_obj=noobaa_pvc_obj[0],
            wait=True,
            snapshot_name=f"{noobaa_pvc_obj[0].name}-snapshot",
        )
        log.info(f"Successfully created snapshot {snap_obj.name} and in Ready state")

        # Restore it to PVC
        log.info(f"Restoring snapshot {snap_obj.name} to create new PVC")
        sc_name = noobaa_pvc_obj[0].get().get("spec").get("storageClassName")
        pvc_size = (
            noobaa_pvc_obj[0]
            .get()
            .get("spec")
            .get("resources")
            .get("requests")
            .get("storage")
        )
        self.restore_pvc_obj = create_restore_pvc(
            sc_name=sc_name,
            snap_name=snap_obj.name,
            namespace=snap_obj.namespace,
            size=pvc_size,
            pvc_name=f"{snap_obj.name}-restore",
            volume_mode=snap_obj.parent_volume_mode,
            access_mode=snap_obj.parent_access_mode,
        )
        wait_for_resource_state(self.restore_pvc_obj, constants.STATUS_BOUND)
        self.restore_pvc_obj.reload()
        log.info(
            f"Succeesfuly created PVC {self.restore_pvc_obj.name} "
            f"from snapshot {snap_obj.name}"
        )

        # Scale down the statefulset noobaa-db
        modify_statefulset_replica_count(
            statefulset_name=self.noobaa_db_sst_name, replica_count=0
        ), f"Failed to scale down the statefulset {self.noobaa_db_sst_name}"

        # Get the noobaa-db PVC
        pvc_obj = OCP(
            kind=constants.PVC, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        noobaa_pvc_yaml = pvc_obj.get(resource_name=noobaa_pvc_obj[0].name)

        # Get the restored noobaa PVC and
        # change the parameter persistentVolumeReclaimPolicy to Retain
        restored_noobaa_pvc_obj = get_pvc_objs(pvc_names=[f"{snap_obj.name}-restore"])
        restored_noobaa_pv_name = (
            restored_noobaa_pvc_obj[0].get("spec").get("spec").get("volumeName")
        )
        pv_obj = OCP(kind=constants.PV, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
        params = '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
        assert pv_obj.patch(resource_name=restored_noobaa_pv_name, params=params), (
            "Failed to change the parameter persistentVolumeReclaimPolicy"
            f" to Retain {restored_noobaa_pv_name}"
        )

        # Delete both PVCs
        delete_pvcs(pvc_objs=[noobaa_pvc_obj[0], restored_noobaa_pvc_obj[0]])

        # Validate original claim db-noobaa-db-0 removed
        assert validate_pv_delete(
            pv_name=noobaa_pv_name
        ), f"PV not deleted, still exist {noobaa_pv_name}"

        # Validate PV for claim db-noobaa-db-0-snapshot-restore is in Released state
        pv_obj.wait_for_resource(
            condition=constants.STATUS_RELEASED, resource_name=restored_noobaa_pv_name
        )

        # Edit again restore PV and remove the claimRef section
        log.info(f"Remove the claimRef section from PVC {restored_noobaa_pv_name}")
        params = '[{"op": "remove", "path": "/spec/claimRef"}]'
        pv_obj.patch(
            resource_name=restored_noobaa_pv_name, params=params, format_type="json"
        )
        log.info(
            f"Successfully removed claimRef section from PVC {restored_noobaa_pv_name}"
        )

        # Validate PV is in Available state
        pv_obj.wait_for_resource(
            condition=constants.STATUS_AVAILABLE, resource_name=restored_noobaa_pv_name
        )

        # Edit the yaml db-noobaa-db-0.yaml and change the
        # setting volumeName to restored PVC
        noobaa_pvc_yaml["spec"]["volumeName"] = restored_noobaa_pv_name
        noobaa_pvc_yaml = OCS(**noobaa_pvc_yaml)
        noobaa_pvc_yaml.create()

        # Validate noobaa PVC is in bound state
        pvc_obj.wait_for_resource(
            condition=constants.STATUS_BOUND,
            resource_name=noobaa_pvc_obj[0].name,
            timeout=120,
        )

        # Scale up the statefulset again
        assert modify_statefulset_replica_count(
            statefulset_name=self.noobaa_db_sst_name, replica_count=1
        ), f"Failed to scale up the statefulset {self.noobaa_db_sst_name}"

        # Validate noobaa pod is up and running
        pod_obj = OCP(kind=constants.POD, namespace=defaults.ROOK_CLUSTER_NAMESPACE)
        pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_count=len(noobaa_pods),
            selector=constants.NOOBAA_APP_LABEL,
        )

        # Change the parameter persistentVolumeReclaimPolicy to Delete again
        params = '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
        assert pv_obj.patch(resource_name=restored_noobaa_pv_name, params=params), (
            "Failed to change the parameter persistentVolumeReclaimPolicy"
            f" to Delete {restored_noobaa_pv_name}"
        )
        log.info("Changed the parameter persistentVolumeReclaimPolicy to Delete again")

        # Verify all storage pods are running
        wait_for_storage_pods()

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        # Deleting Resources
        self.sanity_helpers.delete_resources()

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=120)
