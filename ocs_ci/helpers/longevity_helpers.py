import logging
import statistics
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants, scale_noobaa_lib, workload
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    sync_object_directory,
    wait_for_cache,
)
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.ocs.resources.pod import delete_pods
from ocs_ci.ocs.resources import pod
import ocs_ci.ocs.exceptions as ex

log = logging.getLogger(__name__)


def write_empty_files_to_bucket(
    mcg_obj, awscli_pod_session, bucket_name, test_directory_setup
):
    """
    Write empty files to bucket and verify if they are created.

    Args:
        mcg_obj (MCG) : An MCG object containing the MCG S3 connection credentials
        awscli_pod_session : Fixture to create a new AWSCLI pod for relaying commands.
        bucket_name (str) : Name of the bucket on which files are to be written.
        test_directory_setup : Fixture to setup test DIRs.

    Raises:
        UnexpectedBehaviour : Raises an exception if files are not created.

    Returns:
        Set: A set of names of all bucket objects.

    """

    full_object_path = f"s3://{bucket_name}"
    data_dir = test_directory_setup.origin_dir

    # Touch create 1000 empty files in bucket
    command = f"for file_no in $(seq 1 1000); do touch {data_dir}/test$file_no; done"
    awscli_pod_session.exec_sh_cmd_on_pod(command=command, sh="sh")
    # Write all empty objects to the bucket
    sync_object_directory(awscli_pod_session, data_dir, full_object_path, mcg_obj)

    log.info("Successfully created files.")

    obj_set = set(obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucket_name))
    test_set = set("test" + str(file_no + 1) for file_no in range(1000))

    if test_set != obj_set:
        raise ex.UnexpectedBehaviour("File name set does not match")
    log.info("File name set match")

    return obj_set


def measure_pod_to_pvc_attach_time(pod_objs):
    """
    Measures and Logs Attach Time of all PODs.

    Args:
        pod_objs (list) : List of POD objects for which we have to measure the time.

    Logs:
        Attach time of all PODs, as well as the average time.

    """
    pod_start_time_dict_list = []
    for pod_obj in pod_objs:
        pod_start_time_dict_list.append(helpers.pod_start_time(pod_obj))
    log.info(str(pod_start_time_dict_list))
    time_measures = []
    for attach_time in pod_start_time_dict_list:
        if "my-container" in attach_time:
            time_measures.append(attach_time["my-container"])
        elif "web-server" in attach_time:
            time_measures.append(attach_time["web-server"])
        else:
            time_measures.append(attach_time["performance"])
    for index, start_time in enumerate(time_measures):
        if start_time <= 30:
            log.info(f"POD {pod_objs[index].name} attach time: {start_time} seconds")
        else:
            log.error(
                f"POD {pod_objs[index].name} attach time is {start_time},"
                f"which is greater than 30 seconds"
            )
    if time_measures:
        average = statistics.mean(time_measures)
        log.info(
            f"The average attach time for the sampled {len(time_measures)} pods is {average} seconds."
        )


def measure_pod_creation_time(namespace, num_of_pods):
    """
    Measures and Logs the POD Creation Time of all the PODs.

    Args:
        namespace (str) : Namespace in which the PODs are created.
        num_of_pods (int) : Number of PODs created.

    Logs:
        POD Creation Time of all the PODs.

    """
    logs = performance_lib.run_oc_command(
        "get events --sort-by='{.lastTimestamp}'",
        namespace,
    )

    scheduled_time = None
    pod_no = num_of_pods
    accepted_creation_time = 12

    for line in logs:
        log.info(line)
        if "Scheduled" in line:
            scheduled_time = int(line.split()[0][:-1])
        elif "Created" in line:
            created_time = int(line.split()[0][:-1])
            creation_time = scheduled_time - created_time
            if creation_time <= accepted_creation_time:
                log.info(f"POD number {pod_no} was created in {creation_time} seconds.")
            else:
                log.error(
                    f"POD creation time is {creation_time} and is greater than "
                    f"{accepted_creation_time} seconds."
                )
            pod_no -= 1


def measure_pvc_creation_time(interface, pvc_objs, start_time):
    """
    Measures and Logs PVC Creation Time of all PVCs.

    Args:
        interface (str) : an interface (RBD or CephFS) to run on.
        pvc_objs (list) : List of PVC objects for which we have to measure the time.
        start_time (str) : Formatted time from which and on to search the relevant logs.

    Logs:
        PVC Creation Time of all the PVCs.

    """
    accepted_creation_time = 1
    for pvc_obj in pvc_objs:
        try:
            creation_time = performance_lib.measure_pvc_creation_time(
                interface, pvc_obj.name, start_time
            )

            if creation_time <= accepted_creation_time:
                log.info(f"PVC {pvc_obj.name} was created in {creation_time} seconds.")
            else:
                log.error(
                    f"PVC {pvc_obj.name} creation time is {creation_time} and is greater than "
                    f"{accepted_creation_time} seconds."
                )
        except Exception as err:
            log.error(
                f"Below error occured while measuring the pvc time for {pvc_obj.name} \n {err}"
            )


def measure_pvc_deletion_time(interface, pvc_objs):
    """
    Measures and Logs PVC Deletion Time of all PVCs.

    Args:
        interface (str) : an interface (RBD or CephFS) to run on.
        pvc_objs (list) : List of PVC objects for which we have to measure the time.

    Logs:
        PVC Deletion Time of all the PVCs.

    """
    accepted_deletion_time = 30
    num_of_pvcs = len(pvc_objs)
    pv_name_list = list()
    pv_to_pvc = dict()

    for pvc_no in range(num_of_pvcs):
        pv_name = pvc_objs[pvc_no].backed_pv
        pv_name_list.append(pv_name)
        pv_to_pvc[pv_name] = pvc_objs[pvc_no].name

    pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
        interface=interface, pv_name_list=pv_name_list
    )

    for pv_name, deletion_time in pvc_deletion_time.items():
        if deletion_time <= accepted_deletion_time:
            log.info(
                f"PVC {pv_to_pvc[pv_name]} was deleted in {deletion_time} seconds."
            )
        else:
            log.error(
                f"PVC {pv_to_pvc[pv_name]} deletion time is {deletion_time} and is greater than "
                f"{accepted_deletion_time} seconds."
            )


def create_restore_verify_snapshots(
    multi_snapshot_factory,
    snapshot_restore_factory,
    pod_factory,
    pvc_objs,
    namespace,
    file_name,
):
    """
    Creates snapshots from each PVC in the provided list of PVCs,
    Restores new PVCs out of the created snapshots
    and
    Verifies data integrity by checking the existence and md5sum of file in the restored PVC.

    Args:
        multi_snapshot_factory : Fixture to create a VolumeSnapshot of each PVC in the provided list of PVCs.
        snapshot_restore_factory : Fixture to create a new PVCs out of the VolumeSnapshot provided.
        pod_factory : Fixture to create new PODs.
        pvc_objs (list) : List of PVC objects for which snapshots are to be created.
        namespace (str) : Namespace in which the PVCs are created.
        file_name (str) : Name of the file on which FIO is performed.

    Returns:
        tuple: A tuple of size 2 containing a list of restored PVC objects and a list of the pods attached to the
                restored PVCs, respectively.

    """
    # Create Snapshots
    log.info("Started creation of snapshots of the PVCs.")
    snapshots = multi_snapshot_factory(pvc_obj=pvc_objs, snapshot_name_suffix=namespace)
    log.info("Created snapshots from all the PVCs and snapshots are in Ready state.")

    # Restore Snapshots
    log.info("Started restoration of the snapshots created.")
    restored_pvc_objs = list()
    for snapshot_no in range(len(snapshots)):
        restored_pvc_objs.append(
            snapshot_restore_factory(
                snapshot_obj=snapshots[snapshot_no],
                volume_mode=pvc_objs[snapshot_no].get_pvc_vol_mode,
                access_mode=pvc_objs[snapshot_no].get_pvc_access_mode,
                timeout=600,
            )
        )
    log.info("Restoration complete - Created new PVCs from all the snapshots.")

    # Attach PODs to restored PVCs
    restored_pod_objs = list()
    for restored_pvc_obj in restored_pvc_objs:
        if restored_pvc_obj.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
            restored_pod_objs.append(
                pod_factory(
                    pvc=restored_pvc_obj,
                    raw_block_pv=True,
                    status=constants.STATUS_RUNNING,
                    pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
                )
            )
        else:
            restored_pod_objs.append(
                pod_factory(pvc=restored_pvc_obj, status=constants.STATUS_RUNNING)
            )

    # Verify that the fio exists and md5sum matches
    pod.verify_data_integrity_for_multi_pvc_objs(restored_pod_objs, pvc_objs, file_name)

    return restored_pvc_objs, restored_pod_objs


def expand_verify_pvcs(pvc_objs, pod_objs, pvc_size_new, file_name, fio_size):
    """
    Expands size of each PVC in the provided list of PVCs,
    Verifies data integrity by checking the existence and md5sum of file in the expanded PVC
    and
    Runs FIO on expanded PVCs and verifies results.

    Args:
        pvc_objs (list) : List of PVC objects which are to be expanded.
        pod_objs (list) : List of POD objects attached to the PVCs.
        pvc_size_new (int) : Size of the expanded PVC in GB.
        file_name (str) : Name of the file on which FIO is performed.
        fio_size (int) : Size in MB of FIO.

    """
    # Expand original PVCs
    log.info("Started expansion of the PVCs.")
    for pvc_obj in pvc_objs:
        log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
        pvc_obj.resize_pvc(pvc_size_new, True)
    log.info("Successfully expanded the PVCs.")

    # Verify that the fio exists and md5sum matches
    for pod_no in range(len(pod_objs)):
        pod_obj = pod_objs[pod_no]
        if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
            pod.verify_data_integrity_after_expansion_for_block_pvc(
                pod_obj, pvc_objs[pod_no], fio_size
            )
        else:
            pod.verify_data_integrity(pod_obj, file_name, pvc_objs[pod_no].md5sum)

    # Run IO to utilize 50% of volume
    log.info("Run IO on all pods to utilise 50% of the expanded PVC used space")
    expanded_file_name = "fio_50"
    for pod_obj in pod_objs:
        log.info(f"Running IO on pod {pod_obj.name}")
        log.info(f"File created during IO {expanded_file_name}")
        fio_size = int(0.50 * pvc_size_new * 1000)
        storage_type = (
            "block"
            if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK
            else "fs"
        )
        pod_obj.wl_setup_done = True
        pod_obj.wl_obj = workload.WorkLoad(
            "test_workload_fio",
            pod_obj.get_storage_path(storage_type),
            "fio",
            storage_type,
            pod_obj,
            1,
        )
        pod_obj.run_io(
            storage_type=storage_type,
            size=f"{fio_size}M",
            runtime=20,
            fio_filename=expanded_file_name,
            end_fsync=1,
        )

    log.info("Started IO on all pods to utilise 50% of PVCs")

    for pod_obj in pod_objs:
        # Wait for IO to finish
        pod_obj.get_fio_results(3600)
        log.info(f"IO finished on pod {pod_obj.name}")
        is_block = (
            True
            if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK
            else False
        )
        expanded_file_name_pod = (
            expanded_file_name
            if not is_block
            else pod_obj.get_storage_path(storage_type="block")
        )

        # Verify presence of the file
        expanded_file_path = (
            expanded_file_name_pod
            if is_block
            else pod.get_file_path(pod_obj, expanded_file_name_pod)
        )
        log.info(f"Actual file path on the pod {expanded_file_path}")
        assert pod.check_file_existence(
            pod_obj, expanded_file_path
        ), f"File {expanded_file_name_pod} does not exist"
        log.info(f"File {expanded_file_name_pod} exists in {pod_obj.name}")


def _multi_pvc_pod_lifecycle_factory(
    project_factory, multi_pvc_factory, pod_factory, teardown_factory
):
    """
    Creates a factory that is used to:
    1. Create/Delete PVCs of type:
        a. CephFileSystem - RWO
        b. CephFileSystem - RWX
        c. CephBlockPool - RWO
        d. CephBlockPool - RWO - Block
        e. CephBlockPool - RWX - Block
    2. Create/Delete PODs.
    2. Measure the PVC creation/deletion time and POD to PVC attach time.

    """

    def factory(
        num_of_pvcs=100,
        pvc_size=2,
        bulk=False,
        project=None,
        measure=True,
        delete=True,
        file_name=None,
        fio_percentage=25,
        verify_fio=False,
        expand=False,
    ):
        """
        Args:
            num_of_pvcs (int) : Number of PVCs / PODs we want to create.
            pvc_size (int) : Size of each PVC in GB.
            bulk (bool) : True for bulk operations, False otherwise.
            project (obj) : Project obj inside which the PODs/PVCs are created.
            measure (bool) : True if we want to measure the PVC creation/deletion time and POD to PVC attach time,
                                False otherwise.
            delete (bool) : True if we want to delete PVCs and PODs, False otherwise
            file_name (str) : Name of the file on which FIO is performed.
            fio_percentage (float) : Percentage of PVC space we want to be utilized for FIO.
            verify_fio (bool) : True if we want to verify FIO, False otherwise.
            expand (bool) : True if we want to verify_fio for expansion of PVCs operation, False otherwise.

        """

        if not project:
            project = project_factory("longevity")
        pvc_objs = list()
        executor = ThreadPoolExecutor(max_workers=1)
        start_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        for interface in (constants.CEPHFILESYSTEM, constants.CEPHBLOCKPOOL):
            if interface == constants.CEPHFILESYSTEM:
                access_modes = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
                num_of_pvc = num_of_pvcs // 2
            else:
                access_modes = [
                    constants.ACCESS_MODE_RWO,
                    constants.ACCESS_MODE_RWO + "-" + constants.VOLUME_MODE_BLOCK,
                    constants.ACCESS_MODE_RWX + "-" + constants.VOLUME_MODE_BLOCK,
                ]
                num_of_pvc = num_of_pvcs - num_of_pvcs // 2

            # Create PVCs
            if num_of_pvc > 0:
                pvc_objs_tmp = multi_pvc_factory(
                    interface=interface,
                    size=pvc_size,
                    project=project,
                    access_modes=access_modes,
                    status=constants.STATUS_BOUND,
                    num_of_pvc=num_of_pvc,
                    wait_each=not bulk,
                )
                log.info("PVC creation was successful.")
                pvc_objs.extend(pvc_objs_tmp)

                if measure:
                    # Measure PVC Creation Time
                    measure_pvc_creation_time(interface, pvc_objs_tmp, start_time)

            else:
                log.info(
                    f"Num of PVCs of interface - {interface} = {num_of_pvc}. So no PVCs created."
                )

        # PVC and PV Teardown
        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
            teardown_factory(pvc_obj.backed_pv_obj)

        # Create PODs
        pod_objs = list()
        for pvc_obj in pvc_objs:
            if pvc_obj.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
                if not bulk:
                    pod_objs.append(
                        pod_factory(
                            pvc=pvc_obj,
                            raw_block_pv=True,
                            status=constants.STATUS_RUNNING,
                            pod_dict_path=constants.PERF_BLOCK_POD_YAML,
                        )
                    )
                else:
                    pod_objs.append(
                        pod_factory(
                            pvc=pvc_obj,
                            raw_block_pv=True,
                            pod_dict_path=constants.PERF_BLOCK_POD_YAML,
                        )
                    )
            else:
                if not bulk:
                    pod_objs.append(
                        pod_factory(
                            pvc=pvc_obj,
                            status=constants.STATUS_RUNNING,
                            pod_dict_path=constants.PERF_POD_YAML,
                        )
                    )
                else:
                    pod_objs.append(
                        pod_factory(pvc=pvc_obj, pod_dict_path=constants.PERF_POD_YAML)
                    )

            log.info(f"POD {pod_objs[-1].name} creation was successful.")
        log.info("All PODs are created.")

        if bulk:
            for pod_obj in pod_objs:
                executor.submit(
                    helpers.wait_for_resource_state,
                    pod_obj,
                    constants.STATUS_RUNNING,
                    timeout=300,
                )
                log.info(f"POD {pod_obj.name} reached Running State.")

            log.info("All PODs reached Running State.")

        if measure:
            # Measure POD to PVC attach time
            measure_pod_to_pvc_attach_time(pod_objs)

        # POD Teardown
        for pod_obj in pod_objs:
            teardown_factory(pod_obj)

        # Run FIO on PODs
        fio_size = int((fio_percentage / 100) * pvc_size * 1000)
        for pod_obj in pod_objs:
            storage_type = (
                "block"
                if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK
                else "fs"
            )
            pod_obj.wl_setup_done = True
            pod_obj.wl_obj = workload.WorkLoad(
                "test_workload_fio",
                pod_obj.get_storage_path(storage_type),
                "fio",
                storage_type,
                pod_obj,
                1,
            )
            if not file_name:
                pod_obj.run_io(storage_type, f"{fio_size}M")
            else:
                pod_obj.run_io(
                    storage_type=storage_type,
                    size=f"{fio_size}M",
                    runtime=20,
                    fio_filename=file_name,
                    end_fsync=1,
                )

        if verify_fio:
            log.info(
                "Waiting for IO to complete on all pods to utilise 25% of PVC used space"
            )

            for pod_obj in pod_objs:
                # Wait for IO to finish
                pod_obj.get_fio_results(3600)
                log.info(f"IO finished on pod {pod_obj.name}")
                is_block = (
                    True
                    if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK
                    else False
                )
                file_name_pod = (
                    file_name
                    if not is_block
                    else pod_obj.get_storage_path(storage_type="block")
                )
                # Verify presence of the file
                file_path = (
                    file_name_pod
                    if is_block
                    else pod.get_file_path(pod_obj, file_name_pod)
                )
                log.info(f"Actual file path on the pod {file_path}")
                assert pod.check_file_existence(
                    pod_obj, file_path
                ), f"File {file_name_pod} does not exist"
                log.info(f"File {file_name_pod} exists in {pod_obj.name}")

                if expand and is_block:
                    # Read IO from block PVCs using dd and calculate md5sum.
                    # This dd command reads the data from the device, writes it to
                    # stdout, and reads md5sum from stdin.
                    pod_obj.pvc.md5sum = pod_obj.exec_sh_cmd_on_pod(
                        command=(
                            f"dd iflag=direct if={file_path} bs=10M "
                            f"count={fio_size // 10} | md5sum"
                        )
                    )
                    log.info(f"md5sum of {file_name_pod}: {pod_obj.pvc.md5sum}")
                else:
                    # Calculate md5sum of the file
                    pod_obj.pvc.md5sum = pod.cal_md5sum(pod_obj, file_name_pod)

        log.info("POD FIO was successful.")

        if delete:
            # Delete PODs
            pod_delete = executor.submit(delete_pods, pod_objs, wait=not bulk)
            pod_delete.result()

            log.info("Verified: Pods are deleted.")

            # Delete PVCs
            pvc_delete = executor.submit(delete_pvcs, pvc_objs, concurrent=bulk)
            res = pvc_delete.result()
            if not res:
                raise ex.UnexpectedBehaviour("Deletion of PVCs failed")
            log.info("PVC deletion was successful.")

            # Validate PV Deletion
            for pvc_obj in pvc_objs:
                helpers.validate_pv_delete(pvc_obj.backed_pv)
            log.info("PV deletion was successful.")

            if measure:
                # Measure PVC Deletion Time
                for interface in (constants.CEPHFILESYSTEM, constants.CEPHBLOCKPOOL):
                    if interface == constants.CEPHFILESYSTEM:
                        measure_pvc_deletion_time(
                            interface,
                            pvc_objs[: num_of_pvcs // 2],
                        )
                    else:
                        measure_pvc_deletion_time(
                            interface,
                            pvc_objs[num_of_pvcs // 2 :],
                        )

            log.info(f"Successfully deleted {num_of_pvcs} PVCs")
        else:
            return pvc_objs, pod_objs

    return factory


def _multi_obc_lifecycle_factory(
    bucket_factory, mcg_obj, awscli_pod_session, mcg_obj_session, test_directory_setup
):
    """
    Creates a factory that is used to:
    1. Create/Delete OBCs of type:
        a. NS Bucket
        b. BS Bucket
        c. Cached Bucket
        d. Replica Pair Buckets
    2. Measure the OBC creation/deletion.

    """

    def factory(num_of_obcs=20, bulk=False, measure=True):
        """
        Args:
            num_of_obcs (int) : Number of OBCs we want to create of each type mentioned above.
                                (Total OBCs = num_of_obcs * 5)
            bulk (bool) : True for bulk operations, False otherwise.
            measure (bool) : True if we want to measure the OBC creation/deletion time, False otherwise.

        """

        # Create OBCs - bs, ns, cached and create random files
        obc_objs = list()
        obc_names = list()
        obc_params = [
            (
                "OC",
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
            ),
            ("OC", None),
            (
                "OC",
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Cache",
                        "ttl": 3600,
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                    "placement_policy": {
                        "tiers": [
                            {"backingStores": [constants.DEFAULT_NOOBAA_BACKINGSTORE]}
                        ]
                    },
                },
            ),
        ]
        for _interface, _bucketclass in obc_params:
            if num_of_obcs > 0:
                buckets = bucket_factory(
                    amount=num_of_obcs,
                    interface=_interface,
                    bucketclass=_bucketclass,
                    timeout=300,
                )
                for bucket in buckets:
                    bucket.verify_health(timeout=600)
                obc_objs.extend(buckets)
                written_objs_names = write_empty_files_to_bucket(
                    mcg_obj, awscli_pod_session, buckets[0].name, test_directory_setup
                )
                if (
                    _bucketclass
                    and _bucketclass["namespace_policy_dict"]["type"] == "Cache"
                ):
                    wait_for_cache(mcg_obj, buckets[0].name, list(written_objs_names))

        # Create OBCs - Replica Pair, create random files and verify replication

        target_bucketclass = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {"aws": [(1, None)]},
            },
        }

        source_bucketclass = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {"aws": [(1, None)]},
            },
        }

        target_buckets = list()
        source_buckets = list()
        for _num in range(num_of_obcs):
            target_bucket = bucket_factory(
                bucketclass=target_bucketclass, verify_health=False
            )[0]
            target_buckets.append(target_bucket)
            target_bucket_name = target_bucket.name
            target_bucket.verify_health(timeout=300)

            replication_policy = ("basic-replication-rule", target_bucket_name, None)
            source_bucket = bucket_factory(
                1,
                bucketclass=source_bucketclass,
                replication_policy=replication_policy,
                verify_health=False,
            )[0]
            source_bucket.verify_health(timeout=300)
            source_buckets.append(source_bucket)

            write_empty_files_to_bucket(
                mcg_obj, awscli_pod_session, source_bucket.name, test_directory_setup
            )
            compare_bucket_object_list(
                mcg_obj_session, source_bucket.name, target_bucket_name
            )
        obc_objs.extend(target_buckets)
        obc_objs.extend(source_buckets)

        for obc in obc_objs:
            obc_names.append(obc.name)

        if measure:
            # Measure OBC Creation Time
            scale_noobaa_lib.measure_obc_creation_time(obc_name_list=obc_names)

        # Delete OBCs
        for bucket in obc_objs:
            log.info(f"Deleting bucket: {bucket.name}")
            bucket.delete()

        if measure:
            # Measure OBC Deletion Time
            scale_noobaa_lib.measure_obc_deletion_time(obc_name_list=obc_names)

    return factory
