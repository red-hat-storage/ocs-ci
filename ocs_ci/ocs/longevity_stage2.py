import time
import logging
import statistics
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants, scale_noobaa_lib, ocp, workload
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    sync_object_directory,
    wait_for_cache,
)
from ocs_ci.ocs.ocp import switch_to_project
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.ocs.resources.pod import delete_pods
import ocs_ci.ocs.exceptions as ex

logger = logging.getLogger(__name__)


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

    logger.info("Successfully created files.")

    obj_set = set(obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucket_name))
    test_set = set("test" + str(file_no + 1) for file_no in range(1000))

    if test_set != obj_set:
        raise ex.UnexpectedBehaviour("File name set does not match")
    logger.info("File name set match")

    return obj_set


def measure_pod_to_pvc_attach_time(pod_objs):
    """
    Measures and Logs Attach Time of all PODs.

    Args:
        pod_objs (list) : List of POD objects for which we have to measure the time.

    Raises:
        PerformanceException : Raises an exception if POD attach time is greater than the accepted time.

    Logs:
        Attach time of all PODs, as well as the average time.

    """
    pod_start_time_dict_list = []
    for pod in pod_objs:
        pod_start_time_dict_list.append(helpers.pod_start_time(pod))
    logger.info(str(pod_start_time_dict_list))
    time_measures = []
    for attach_time in pod_start_time_dict_list:
        if "my-container" in attach_time:
            time_measures.append(attach_time["my-container"])
        elif "web-server" in attach_time:
            time_measures.append(attach_time["web-server"])
        else:
            time_measures.append(attach_time["performance"])
    for index, start_time in enumerate(time_measures):
        logger.info(f"POD {pod_objs[index].name} attach time: {start_time} seconds")
        if start_time > 30:
            raise ex.PerformanceException(
                f"POD {pod_objs[index].name} attach time is {start_time},"
                f"which is greater than 30 seconds"
            )
    if time_measures:
        average = statistics.mean(time_measures)
        logger.info(
            f"The average attach time for the sampled {len(time_measures)} pods is {average} seconds."
        )


def measure_pod_creation_time(namespace, num_of_pods):
    """
    Measures and Logs the POD Creation Time of all the PODs.

    Args:
        namespace (str) : Namespace in which the PODs are created.
        num_of_pods (int) : Number of PODs created.

    Raises:
        PerformanceException : Raises an exception if POD creation time is greater than the accepted time.

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
        logger.info(line)
        if "Scheduled" in line:
            scheduled_time = int(line.split()[0][:-1])
        elif "Created" in line:
            created_time = int(line.split()[0][:-1])
            creation_time = scheduled_time - created_time
            logger.info(f"POD number {pod_no} was created in {creation_time} seconds.")
            if creation_time > accepted_creation_time:
                raise ex.PerformanceException(
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

    Raises:
        PerformanceException : Raises an exception if PVC creation time is greater than the accepted time.

    Logs:
        PVC Creation Time of all the PVCs.

    """
    accepted_creation_time = 1
    for pvc_obj in pvc_objs:
        creation_time = performance_lib.measure_pvc_creation_time(
            interface, pvc_obj.name, start_time
        )

        logger.info(f"PVC {pvc_obj.name} was created in {creation_time} seconds.")
        if creation_time > accepted_creation_time:
            raise ex.PerformanceException(
                f"PVC {pvc_obj.name} creation time is {creation_time} and is greater than "
                f"{accepted_creation_time} seconds."
            )


def measure_pvc_deletion_time(interface, pvc_objs):
    """
    Measures and Logs PVC Deletion Time of all PVCs.

    Args:
        interface (str) : an interface (RBD or CephFS) to run on.
        pvc_objs (list) : List of PVC objects for which we have to measure the time.

    Raises:
        PerformanceException : Raises an exception if PVC deletion time is greater than the accepted time.

    Logs:
        PVC Deletion Time of all the PVCs.

    """
    accepted_deletion_time = 2 if interface == constants.CEPHFILESYSTEM else 1
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
        logger.info(f"PVC {pv_to_pvc[pv_name]} was deleted in {deletion_time} seconds.")
        if deletion_time > accepted_deletion_time:
            raise ex.PerformanceException(
                f"PVC {pv_to_pvc[pv_name]} deletion time is {deletion_time} and is greater than "
                f"{accepted_deletion_time} seconds."
            )


def _multi_pvc_pod_lifecycle_factory(project_factory, multi_pvc_factory, pod_factory, teardown_factory):
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
        num_of_pvcs=100, pvc_size=2, bulk=False, namespace="stage-2", measure=True
    ):
        """
        Args:
            num_of_pvcs (int) : Number of PVCs / PODs we want to create.
            pvc_size (int) : Size of each PVC in GB.
            bulk (bool) : True for bulk operations, False otherwise.
            namespace (str) : Name of the namespace inside which the PODs/PVCs are created.
            measure (bool) : True if we want to measure the PVC creation/deletion time and POD to PVC attach time,
                                False otherwise.

        """
        project = project_factory(namespace)
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
                logger.info("PVC creation was successful.")
                pvc_objs.extend(pvc_objs_tmp)

                if measure:
                    # Measure PVC Creation Time
                    measure_pvc_creation_time(interface, pvc_objs_tmp, start_time)

            else:
                logger.info(
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

            logger.info(f"POD {pod_objs[-1].name} creation was successful.")
        logger.info("All PODs are created.")

        if bulk:
            for pod_obj in pod_objs:
                executor.submit(
                    helpers.wait_for_resource_state,
                    pod_obj,
                    constants.STATUS_RUNNING,
                    timeout=300,
                )
                logger.info(f"POD {pod_obj.name} reached Running State.")

            logger.info("All PODs reached Running State.")

        if measure:
            # Measure POD to PVC attach time
            measure_pod_to_pvc_attach_time(pod_objs)

        # POD Teardown
        for pod_obj in pod_objs:
            teardown_factory(pod_obj)

        # Run FIO on PODs
        fio_size = int(0.25 * pvc_size * 1024)
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
            pod_obj.run_io(storage_type, f"{fio_size}M")
        logger.info("POD FIO was successful.")

        # Delete PODs
        pod_delete = executor.submit(delete_pods, pod_objs, wait=not bulk)
        pod_delete.result()

        logger.info("Verified: Pods are deleted.")

        # Delete PVCs
        pvc_delete = executor.submit(delete_pvcs, pvc_objs, concurrent=bulk)
        res = pvc_delete.result()
        if not res:
            raise ex.UnexpectedBehaviour("Deletion of PVCs failed")
        logger.info("PVC deletion was successful.")

        # Validate PV Deletion
        for pvc_obj in pvc_objs:
            helpers.validate_pv_delete(pvc_obj.backed_pv)
        logger.info("PV deletion was successful.")

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

        logger.info(f"Successfully deleted {num_of_pvcs} PVCs")

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
                        "namespacestore_dict": {"rgw": [(1, None)]},
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
                        "namespacestore_dict": {"rgw": [(1, None)]},
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
                    verify_health=not bulk,
                )
                if bulk:
                    for bucket in buckets:
                        bucket.verify_health()
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
                "namespacestore_dict": {"rgw": [(1, None)]},
            },
        }

        source_bucketclass = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestore_dict": {"rgw": [(1, None)]},
            },
        }

        target_buckets = list()
        source_buckets = list()
        for _num in range(num_of_obcs):
            target_bucket = bucket_factory(bucketclass=target_bucketclass)[0]
            target_buckets.append(target_bucket)
            target_bucket_name = target_bucket.name

            replication_policy = ("basic-replication-rule", target_bucket_name, None)
            source_bucket = bucket_factory(
                1, bucketclass=source_bucketclass, replication_policy=replication_policy
            )[0]
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
            logger.info(f"Deleting bucket: {bucket.name}")
            bucket.delete()

        if measure:
            # Measure OBC Deletion Time
            scale_noobaa_lib.measure_obc_deletion_time(obc_name_list=obc_names)

    return factory


def stage2(
    multi_pvc_pod_lifecycle_factory,
    multi_obc_lifecycle_factory,
    num_of_pvcs=100,
    pvc_size=2,
    num_of_obcs=20,
    run_time=1440,
    measure=True,
    delay=600,
):
    """
    Function to handle automation of Longevity Stage 2 Sequential Steps i.e. Creation / Deletion of PVCs, PODs and OBCs
    and measurement of creation / deletion times of the mentioned resources.

    Args:
        multi_pvc_pod_lifecycle_factory : Fixture to create/delete multiple pvcs and pods and
                                            measure pvc creation/deletion time and pod attach time.
        multi_obc_lifecycle_factory : Fixture to create/delete multiple obcs and
                                        measure their creation/deletion time.
        num_of_pvcs (int) : Total Number of PVCs / PODs we want to create.
        pvc_size (int) : Size of each PVC in GB.
        num_of_obcs (int) : Number of OBCs we want to create of each type. (Total OBCs = num_of_obcs * 5)
        run_time (int) : Total Run Time in minutes.
        measure (bool) : True if we want to measure the performance metrics, False otherwise.
        delay (int) : Delay time (in seconds) between sequential and bulk operations as well as between cycles.

    """
    end_time = datetime.now() + timedelta(minutes=run_time)
    cycle_no = 0

    while datetime.now() < end_time:
        cycle_no += 1
        logger.info(f"#################[STARTING CYCLE:{cycle_no}]#################")

        for bulk in (False, True):
            current_ops = "BULK-OPERATION" if bulk else "SEQUENTIAL-OPERATION"
            logger.info(f"#################[{current_ops}]#################")
            multi_pvc_pod_lifecycle_factory(
                num_of_pvcs=num_of_pvcs,
                pvc_size=pvc_size,
                bulk=bulk,
                namespace=f"stage-2-cycle-{cycle_no}-{current_ops.lower()}",
                measure=measure,
            )
            multi_obc_lifecycle_factory(
                num_of_obcs=num_of_obcs, bulk=bulk, measure=measure
            )

            # Delay between Sequential and Bulk Operations
            if not bulk:
                logger.info(
                    f"#################[WAITING FOR {delay} SECONDS AFTER {current_ops}.]#################"
                )
                time.sleep(delay)

        logger.info(f"#################[ENDING CYCLE:{cycle_no}]#################")

        logger.info(
            f"#################[WAITING FOR {delay} SECONDS AFTER {cycle_no} CYCLE.]#################"
        )
        time.sleep(delay)
