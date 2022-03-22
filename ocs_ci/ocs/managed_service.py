import logging

from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs import constants, node
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def rwo_dynamic_pvc(
    pvc_factory,
    pod_factory,
    storageclass_factory,
):
    pvc_size = 1
    mode_list = [
        [
            constants.CEPHBLOCKPOOL,
            constants.RECLAIM_POLICY_RETAIN,
            constants.ACCESS_MODE_RWO,
        ],
        [
            constants.CEPHBLOCKPOOL,
            constants.RECLAIM_POLICY_DELETE,
            constants.ACCESS_MODE_RWX,
        ],
        [
            constants.CEPHFILESYSTEM,
            constants.RECLAIM_POLICY_RETAIN,
            constants.ACCESS_MODE_RWX,
        ],
        [
            constants.CEPHFILESYSTEM,
            constants.RECLAIM_POLICY_DELETE,
            constants.ACCESS_MODE_RWO,
        ],
    ]
    for mode in mode_list:
        interface_type = mode[0]
        reclaim_policy = mode[1]
        access_mode = mode[2]
        # Create storage class if reclaim policy is not "Delete"
        sc_obj = (
            default_storage_class(interface_type)
            if reclaim_policy == constants.RECLAIM_POLICY_DELETE
            else storageclass_factory(
                interface=interface_type, reclaim_policy=reclaim_policy
            )
        )
        worker_nodes_list = node.get_worker_nodes()
        expected_failure_str = "Multi-Attach error for volume"
        storage_type = "fs"

        logger.info(f"Creating PVC with {access_mode} access mode")
        pvc_obj = pvc_factory(
            interface=interface_type,
            storageclass=sc_obj,
            size=pvc_size,
            access_mode=access_mode,
            status=constants.STATUS_BOUND,
        )

        logger.info(
            f"Creating first pod on node: {worker_nodes_list[0]} "
            f"with pvc {pvc_obj.name}"
        )
        pod_obj1 = pod_factory(
            interface=interface_type,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            node_name=worker_nodes_list[0],
            pod_dict_path=constants.NGINX_POD_YAML,
        )

        logger.info(
            f"Creating second pod on node: {worker_nodes_list[1]} "
            f"with pvc {pvc_obj.name}"
        )
        pod_obj2 = pod_factory(
            interface=interface_type,
            pvc=pvc_obj,
            status=constants.STATUS_CONTAINER_CREATING,
            node_name=worker_nodes_list[1],
            pod_dict_path=constants.NGINX_POD_YAML,
        )

        node_pod1 = pod_obj1.get().get("spec").get("nodeName")
        node_pod2 = pod_obj2.get().get("spec").get("nodeName")
        assert node_pod1 != node_pod2, "Both pods are on the same node"

        logger.info(f"Running IO on first pod {pod_obj1.name}")
        file_name = pod_obj1.name
        pod_obj1.run_io(storage_type=storage_type, size="1G", fio_filename=file_name)
        pod.get_fio_rw_iops(pod_obj1)
        md5sum_pod1_data = pod.cal_md5sum(pod_obj=pod_obj1, file_name=file_name)

        # Verify that second pod is still in ContainerCreating state and not
        # able to attain Running state due to expected failure
        logger.info(
            f"Verify that second pod {pod_obj2.name} is still in ContainerCreating state"
        )
        helpers.wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_CONTAINER_CREATING
        )

        sample = TimeoutSampler(
            timeout=100,
            sleep=10,
            func=verify_expected_failure_event,
            ocs_obj=pod_obj2,
            failure_str=expected_failure_str,
        )
        if not sample.wait_for_func_status(result=True):
            raise UnexpectedBehaviour(
                f"Failure string {expected_failure_str} is not found in oc describe"
                f" command"
            )

        # verify_expected_failure_event(ocs_obj=pod_obj2, failure_str=expected_failure_str)

        logger.info(
            f"Deleting first pod so that second pod can attach PVC {pvc_obj.name}"
        )
        pod_obj1.delete()
        pod_obj1.ocp.wait_for_delete(resource_name=pod_obj1.name)

        # Wait for second pod to be in Running state
        helpers.wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_RUNNING, timeout=240
        )

        logger.info(f"Verify data from second pod {pod_obj2.name}")
        pod.verify_data_integrity(
            pod_obj=pod_obj2, file_name=file_name, original_md5sum=md5sum_pod1_data
        )

        pod_obj2.run_io(
            storage_type=storage_type, size="1G", fio_filename=pod_obj2.name
        )
        pod.get_fio_rw_iops(pod_obj2)

        # Again verify data integrity
        logger.info(f"Again verify data from second pod {pod_obj2.name}")
        pod.verify_data_integrity(
            pod_obj=pod_obj2, file_name=file_name, original_md5sum=md5sum_pod1_data
        )


def verify_expected_failure_event(ocs_obj, failure_str):
    """
    Checks for the expected failure event message in oc describe command

    """
    logger.info("Check expected failure event message in oc describe command")
    if failure_str in ocs_obj.describe():
        logger.info(
            f"Failure string {failure_str} is present in oc describe" f" command"
        )
        return True
    else:
        logger.info(
            f"Failure string {failure_str} is not found in oc describe" f" command"
        )
        return False


def pvc_to_pvc_clone(pvc_factory, pod_factory, teardown_factory, index):
    config.switch_ctx(index)
    logger.info(
        f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
    )
    for interface_type in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        pvc_obj = pvc_factory(
            interface=interface_type, size=1, status=constants.STATUS_BOUND
        )
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        pod_obj = pod_factory(
            interface=interface_type, pvc=pvc_obj, status=constants.STATUS_RUNNING
        )
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        logger.info(f"Running IO on pod {pod_obj.name}")
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        file_name = pod_obj.name
        logger.info(f"File created during IO {file_name}")
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        pod_obj.run_io(storage_type="fs", size="500M", fio_filename=file_name)
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )

        # Wait for fio to finish
        pod_obj.get_fio_results()
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        logger.info(f"Io completed on pod {pod_obj.name}.")
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        # Verify presence of the file
        file_path = pod.get_file_path(pod_obj, file_name)
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        logger.info(f"Actual file path on the pod {file_path}")
        logger.info(
            f"***************************{config.ENV_DATA.get('cluster_name')}***********************"
        )
        assert pod.check_file_existence(
            pod_obj, file_path
        ), f"File {file_name} does not exist"
        logger.info(f"File {file_name} exists in {pod_obj.name}")

        # Calculate md5sum of the file.
        orig_md5_sum = pod.cal_md5sum(pod_obj, file_name)

        # Create a clone of the existing pvc.
        sc_name = pvc_obj.backed_sc
        parent_pvc = pvc_obj.name
        clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
        namespace = pvc_obj.namespace
        if interface_type == constants.CEPHFILESYSTEM:
            clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
        cloned_pvc_obj = pvc.create_pvc_clone(
            sc_name, parent_pvc, clone_yaml, namespace
        )
        teardown_factory(cloned_pvc_obj)
        helpers.wait_for_resource_state(cloned_pvc_obj, constants.STATUS_BOUND)
        cloned_pvc_obj.reload()

        # Create and attach pod to the pvc
        clone_pod_obj = helpers.create_pod(
            interface_type=interface_type,
            pvc_name=cloned_pvc_obj.name,
            namespace=cloned_pvc_obj.namespace,
            pod_dict_path=constants.NGINX_POD_YAML,
        )
        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=clone_pod_obj, state=constants.STATUS_RUNNING
        )
        clone_pod_obj.reload()
        teardown_factory(clone_pod_obj)

        # Verify file's presence on the new pod
        logger.info(
            f"Checking the existence of {file_name} on cloned pod "
            f"{clone_pod_obj.name}"
        )
        assert pod.check_file_existence(
            clone_pod_obj, file_path
        ), f"File {file_path} does not exist"
        logger.info(f"File {file_name} exists in {clone_pod_obj.name}")

        # Verify Contents of a file in the cloned pvc
        # by validating if md5sum matches.
        logger.info(
            f"Verifying that md5sum of {file_name} "
            f"on pod {pod_obj.name} matches with md5sum "
            f"of the same file on restore pod {clone_pod_obj.name}"
        )
        assert pod.verify_data_integrity(
            clone_pod_obj, file_name, orig_md5_sum
        ), "Data integrity check failed"
        logger.info("Data integrity check passed, md5sum are same")

        logger.info("Run IO on new pod")
        clone_pod_obj.run_io(storage_type="fs", size="100M", runtime=10)

        # Wait for IO to finish on the new pod
        clone_pod_obj.get_fio_results()
        logger.info(f"IO completed on pod {clone_pod_obj.name}")


def pvc_snapshot(pvc_factory, pod_factory, teardown_factory):
    for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
        pvc_obj = pvc_factory(
            interface=interface, size=5, status=constants.STATUS_BOUND
        )
        pod_obj = pod_factory(
            interface=interface, pvc=pvc_obj, status=constants.STATUS_RUNNING
        )
        logger.info(f"Running IO on pod {pod_obj.name}")
        file_name = pod_obj.name
        logger.info(f"File created during IO {file_name}")
        pod_obj.run_io(storage_type="fs", size="1G", fio_filename=file_name)

        # Wait for fio to finish
        fio_result = pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )
        logger.info(f"Verified IO on pod {pod_obj.name}.")

        # Verify presence of the file
        file_path = pod.get_file_path(pod_obj, file_name)
        logger.info(f"Actual file path on the pod {file_path}")
        assert pod.check_file_existence(
            pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        logger.info(f"File {file_name} exists in {pod_obj.name}")

        # Calculate md5sum
        orig_md5_sum = pod.cal_md5sum(pod_obj, file_name)
        # Take a snapshot
        snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
        if interface == constants.CEPHFILESYSTEM:
            snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        snap_name = helpers.create_unique_resource_name("test", "snapshot")
        snap_obj = pvc.create_pvc_snapshot(
            pvc_obj.name,
            snap_yaml,
            snap_name,
            pvc_obj.namespace,
            helpers.default_volumesnapshotclass(interface).name,
        )
        snap_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=snap_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=60,
        )
        teardown_factory(snap_obj)

        # Same Storage class of the original PVC
        sc_name = pvc_obj.backed_sc

        # Size should be same as of the original PVC
        pvc_size = str(pvc_obj.size) + "Gi"

        # Create pvc out of the snapshot
        # Both, the snapshot and the restore PVC should be in same namespace
        restore_pvc_name = helpers.create_unique_resource_name("test", "restore-pvc")
        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        restore_pvc_obj = pvc.create_restore_pvc(
            sc_name=sc_name,
            snap_name=snap_obj.name,
            namespace=snap_obj.namespace,
            size=pvc_size,
            pvc_name=restore_pvc_name,
            restore_pvc_yaml=restore_pvc_yaml,
        )
        helpers.wait_for_resource_state(restore_pvc_obj, constants.STATUS_BOUND)
        restore_pvc_obj.reload()
        teardown_factory(restore_pvc_obj)

        # Create and attach pod to the pvc
        restore_pod_obj = helpers.create_pod(
            interface_type=interface,
            pvc_name=restore_pvc_obj.name,
            namespace=snap_obj.namespace,
            pod_dict_path=constants.NGINX_POD_YAML,
        )

        # Confirm that the pod is running
        helpers.wait_for_resource_state(
            resource=restore_pod_obj, state=constants.STATUS_RUNNING
        )
        restore_pod_obj.reload()
        teardown_factory(restore_pod_obj)

        # Verify that the file is present on the new pod
        logger.info(
            f"Checking the existence of {file_name} "
            f"on restore pod {restore_pod_obj.name}"
        )
        assert pod.check_file_existence(
            restore_pod_obj, file_path
        ), f"File {file_name} doesn't exist"
        logger.info(f"File {file_name} exists in {restore_pod_obj.name}")

        # Verify that the md5sum matches
        logger.info(
            f"Verifying that md5sum of {file_name} "
            f"on pod {pod_obj.name} matches with md5sum "
            f"of the same file on restore pod {restore_pod_obj.name}"
        )
        assert pod.verify_data_integrity(
            restore_pod_obj, file_name, orig_md5_sum
        ), "Data integrity check failed"
        logger.info("Data integrity check passed, md5sum are same")

        logger.info("Running IO on new pod")
        # Run IO on new pod
        restore_pod_obj.run_io(storage_type="fs", size="1G", runtime=20)

        # Wait for fio to finish
        restore_pod_obj.get_fio_results()
        logger.info("IO finished o new pod")


def flow(pvc_factory, pod_factory, storageclass_factory, teardown_factory, index):
    config.switch_ctx(index)
    rwo_dynamic_pvc(
        pvc_factory=pvc_factory,
        pod_factory=pod_factory,
        storageclass_factory=storageclass_factory,
    )
    pvc_to_pvc_clone(
        pvc_factory=pvc_factory,
        pod_factory=pod_factory,
        teardown_factory=teardown_factory,
    )
    pvc_snapshot(
        pvc_factory=pvc_factory,
        pod_factory=pod_factory,
        teardown_factory=teardown_factory,
    )
