import logging

import pytest

from ocs_ci.ocs import ocp, constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
)
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.helpers import helpers
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@green_squad
@tier1
@pytest.mark.polarion_id("OCS-6067")
class TestVolumeGroupSnapshot(ManageTest):
    """
    Tests to verify VolumeGroupSnapshot feature
    """

    def test_volume_group_snapshot(
        self, interface_iterate, multi_pvc_factory, pod_factory, teardown_factory
    ):
        """
        1. Create the PVCs and label them to include in the volume group snapshot
        2. Create pods, and run I/O on a pod file.
        3. Calculate md5sum of the file.
        4. Create a VolumeGroupSnapshot using the same label
        5. Verify creation of VolumeSnapshots
        6. Create PVC from all the VolumeSnapshot that is part of a VolumeGroupSnapshot
        7. Attach a new pod to it.
        8. Verify that the file is present on the new pod also.
        9. Verify that the md5sum of the file on the new pod matches
           with the md5sum of the file on the original pod.

        """
        interface = interface_iterate

        # Create PVC
        log.info("Create PVCs")
        pvc_objs = multi_pvc_factory(
            interface=interface,
            size=3,
            status=constants.STATUS_BOUND,
            num_of_pvc=5,
        )
        label_key = "group"
        label_value = "myGroup"
        namespace = pvc_objs[0].namespace
        for pvc_obj in pvc_objs:
            pvc_obj.add_label(label=f"{label_key}={label_value}")

        pod_objs = helpers.create_pods(
            pvc_objs,
            pod_factory,
            interface,
            status=constants.STATUS_RUNNING,
        )

        filename_to_md5sum_map = {}
        for pod_obj in pod_objs:
            log.info(f"Running IO on pod {pod_obj.name}")
            file_name = pod_obj.pvc.name
            log.info(f"File created during IO {file_name}")
            pod_obj.run_io(storage_type="fs", size="1G", fio_filename=file_name)

            # Wait for fio to finish
            pod_obj.get_fio_results()

            # Calculate md5sum
            md5sum = pod.cal_md5sum(pod_obj, file_name)
            filename_to_md5sum_map[file_name] = md5sum

        for pod_obj in pod_objs:
            pod_obj.delete()
            pod_obj.ocp.wait_for_delete(pod_obj.name)

        # Create VolumeGroupSnapshot
        vgs_yaml = constants.CSI_RBD_VOLUMEGROUPSNAPSHOT_YAML
        if interface == constants.CEPHFILESYSTEM:
            vgs_yaml = constants.CSI_CEPHFS_VOLUMEGROUPSNAPSHOT_YAML

        vgs_name = helpers.create_unique_resource_name("test", "volumegroupsnapshot")
        vgs_obj = pvc.create_volume_group_snapshot(
            label_key=label_key,
            label_value=label_value,
            vgs_yaml=vgs_yaml,
            vgs_name=vgs_name,
            namespace=namespace,
            wait=True,
        )
        teardown_factory(vgs_obj)

        vs_obj = ocp.OCP(kind=constants.VOLUMESNAPSHOT, namespace=namespace)
        vs_list = vs_obj.get()["items"]
        assert len(vs_list) == len(pvc_objs)
        pvc_to_vs_map = {}
        for vs in vs_list:
            pvc_name = vs["spec"]["source"]["persistentVolumeClaimName"]
            vs_name = vs["metadata"]["name"]
            vs_obj.wait_for_resource(
                condition="true",
                resource_name=vs_name,
                column=constants.STATUS_READYTOUSE,
            )
            pvc_to_vs_map[pvc_name] = vs_name

        # Restoring a VolumeGroupSnapshot
        # Create PVC from all the VolumeSnapshot that is part of a VolumeGroupSnapshot
        restore_pvc_objs = []
        for pvc_obj in pvc_objs:
            file_name = pvc_obj.name
            restore_pvc_name = helpers.create_unique_resource_name(
                "test", "restore-pvc"
            )
            if interface == constants.CEPHFILESYSTEM:
                restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML
            else:
                restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML

            restore_pvc_obj = pvc.create_restore_pvc(
                sc_name=pvc_obj.backed_sc,
                snap_name=pvc_to_vs_map.get(pvc_obj.name),
                namespace=namespace,
                size=str(pvc_obj.size) + "Gi",
                pvc_name=restore_pvc_name,
                restore_pvc_yaml=restore_pvc_yaml,
                access_mode=constants.ACCESS_MODE_ROX,
            )
            helpers.wait_for_resource_state(
                restore_pvc_obj, constants.STATUS_BOUND, timeout=180
            )
            restore_pvc_obj.reload()
            teardown_factory(restore_pvc_obj)
            restore_pvc_objs.append(restore_pvc_obj)

            # Create and attach pod to the pvc
            restore_pod_obj = helpers.create_pod(
                interface_type=interface,
                pvc_name=restore_pvc_obj.name,
                namespace=namespace,
                pod_dict_path=constants.NGINX_POD_YAML,
                pvc_read_only_mode=True,
            )

            # Confirm that the pod is running
            helpers.wait_for_resource_state(
                resource=restore_pod_obj, state=constants.STATUS_RUNNING
            )
            restore_pod_obj.reload()
            teardown_factory(restore_pod_obj)

            # Verify presence of the file on restored pvc
            log.info(
                f"Checking the existence of file {file_name}"
                f"on restore pod {restore_pod_obj.name}"
            )
            file_path = pod.get_file_path(restore_pod_obj, file_name)
            assert pod.check_file_existence(
                restore_pod_obj, file_path
            ), f"File {file_name} doesn't exist"
            log.info(f"File {file_name} exists in {restore_pod_obj.name}")

            # Verify that the md5sum matches
            log.info(
                f"Verifying that md5sum of {file_name} "
                f"on previous pod matches with md5sum "
                f"of the same file on restore pod {restore_pod_obj.name}"
            )
            assert pod.verify_data_integrity(
                restore_pod_obj, file_name, filename_to_md5sum_map[file_name]
            ), "Data integrity check failed"
            log.info("Data integrity check passed, md5sum are same")

        vgs_obj.delete()
        vgs_obj.ocp.wait_for_delete(vgs_obj.name)

        log.info(f"Waiting for all {constants.VOLUMESNAPSHOT} to be deleted")
        for vs_list in TimeoutSampler(120, 5, vs_obj.get):
            if len(vs_list["items"]) == 0:
                log.info(f"Verified all {constants.VOLUMESNAPSHOT} resources deleted")
                break
            log.info(
                f"{len(vs_list['items'])} {constants.VOLUMESNAPSHOT} resources not deleted"
            )
