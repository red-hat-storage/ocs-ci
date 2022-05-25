import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants, workload
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.ocs.resources.pod import delete_pods
from ocs_ci.ocs.resources import pod
import ocs_ci.ocs.exceptions as ex

logger = logging.getLogger(__name__)


def stage4(
    project_factory,
    multi_pvc_factory,
    pod_factory,
    multi_pvc_clone_factory,
    multi_snapshot_factory,
    multi_snapshot_restore_factory,
    teardown_factory,
    num_of_pvcs=100,
    pvc_size=2,
    run_time=1440,
    pvc_size_new=4,
):
    """
    Function to handle automation of Longevity Stage 4 i.e.
        1. Creation / Deletion of PODs, PVCs of different types + fill data upto 25% of mount point space.
        2. Creation / Deletion of Clones of the given PVCs.
        3. Creation / Deletion of VolumeSnapshots of the given PVCs.
        4. Restore the created VolumeSnapshots into a new set of PVCs.
        5. Expansion of size of the original PVCs.


    Args:
        project_factory : Fixture to create a new Project.
        multi_pvc_factory : Fixture to create multiple PVCs of different access modes and interface types.
        pod_factory : Fixture to create new PODs.
        multi_pvc_clone_factory : Fixture to create a clone from each PVC in the provided list of PVCs.
        multi_snapshot_factory : Fixture to create a VolumeSnapshot of each PVC in the provided list of PVCs.
        multi_snapshot_restore_factory : Fixture to create a set of new PVCs out of each VolumeSnapshot provided in the
                                            list.
        teardown_factory : Fixture to tear down a resource that was created during the test.
        num_of_pvcs (int) : Total Number of PVCs we want to create.
        pvc_size (int) : Size of each PVC in GB.
        run_time (int) : Total Run Time in minutes.
        pvc_size_new (int) : Size of the expanded PVC in GB.

    """
    end_time = datetime.now() + timedelta(minutes=run_time)
    cycle_no = 0

    while datetime.now() < end_time:
        cycle_no += 1
        logger.info(f"------------STARTING CYCLE:{cycle_no}------------")

        namespace = "stage-4-cycle-" + str(cycle_no)
        project = project_factory(project_name=namespace)
        executor = ThreadPoolExecutor(max_workers=1)
        pvc_objs = list()

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
                    wait_each=True,
                )
                logger.info("PVC creation was successful.")
                pvc_objs.extend(pvc_objs_tmp)

            else:
                logger.info(
                    f"Num of PVCs of interface - {interface} = {num_of_pvc}. So no PVCs created."
                )

        # Create PODs
        pod_objs = list()
        for pvc_obj in pvc_objs:
            if pvc_obj.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
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
                        status=constants.STATUS_RUNNING,
                        pod_dict_path=constants.PERF_POD_YAML,
                    )
                )

        logger.info("POD creation was successful.")

        # Run IO to utilize 25% of volume
        logger.info("Run IO on all pods to utilise 25% of PVCs")
        file_name = "fio_25"
        for pod_obj in pod_objs:
            logger.info(f"Running IO on pod {pod_obj.name}")
            logger.info(f"File created during IO {file_name}")
            fio_size = int(0.25 * pvc_size * 1024)
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
                fio_filename=file_name,
                end_fsync=1,
            )

        logger.info("Started IO on all pods to utilise 25% of PVCs")

        for pod_obj in pod_objs:
            # Wait for IO to finish
            pod_obj.get_fio_results(3600)
            logger.info(f"IO finished on pod {pod_obj.name}")
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
                file_name_pod if is_block else pod.get_file_path(pod_obj, file_name_pod)
            )
            logger.info(f"Actual file path on the pod {file_path}")
            assert pod.check_file_existence(
                pod_obj, file_path
            ), f"File {file_name} does not exist"
            logger.info(f"File {file_name} exists in {pod_obj.name}")

            # Calculate md5sum of the file
            pod_obj.pvc.md5sum = pod.cal_md5sum(pod_obj, file_name_pod, block=is_block)

        # Create Clones
        cloned_pvcs = multi_pvc_clone_factory(pvc_obj=pvc_objs, wait_each=True)
        logger.info("Successfully Created clones of the PVCs.")

        # Attach PODs to cloned PVCs
        cloned_pod_objs = list()
        for cloned_pvc_obj in cloned_pvcs:
            if cloned_pvc_obj.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
                cloned_pod_objs.append(
                    pod_factory(
                        pvc=cloned_pvc_obj,
                        raw_block_pv=True,
                        status=constants.STATUS_RUNNING,
                        pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
                    )
                )
            else:
                cloned_pod_objs.append(
                    pod_factory(pvc=cloned_pvc_obj, status=constants.STATUS_RUNNING)
                )

        # Verify that the md5sum matches
        for pod_obj in cloned_pod_objs:
            logger.info(f"Verifying md5sum of {file_name} " f"on pod {pod_obj.name}")
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
            pod.verify_data_integrity(pod_obj, file_name_pod, pod_obj.pvc.parent.md5sum)
            logger.info(
                f"Verified: md5sum of {file_name} on pod {pod_obj.name} "
                f"matches with the original md5sum"
            )

        # Create Snapshots
        snapshots = multi_snapshot_factory(
            pvc_obj=pvc_objs, snapshot_name_suffix=namespace
        )
        logger.info(
            "Created snapshots from all the PVCs and snapshots are in Ready state"
        )

        # Restore Snapshots
        restored_pvc_objs = multi_snapshot_restore_factory(
            snapshot_obj=snapshots, restore_pvc_suffix="restore"
        )
        logger.info("Created new PVCs from all the snapshots")

        # Expand original PVCs
        for pvc_obj in pvc_objs:
            logger.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
            pvc_obj.resize_pvc(pvc_size_new, True)

        total_pvcs = pvc_objs + cloned_pvcs + restored_pvc_objs
        total_pods = pod_objs + cloned_pod_objs

        # PVC and PV Teardown
        pv_objs = list()
        for pvc_obj in total_pvcs:
            teardown_factory(pvc_obj)
            pv_objs.append(pvc_obj.backed_pv_obj.name)
            teardown_factory(pvc_obj.backed_pv_obj)

        # POD Teardown
        for pod_obj in total_pods:
            teardown_factory(pod_obj)

        # Delete PODs
        pod_delete = executor.submit(delete_pods, total_pods)
        pod_delete.result()

        logger.info("Verified: Pods are deleted.")

        # Delete PVCs
        pvc_delete = executor.submit(delete_pvcs, total_pvcs)
        res = pvc_delete.result()
        if not res:
            raise ex.UnexpectedBehaviour("Deletion of PVCs failed")
        logger.info("PVC deletion was successful.")

        logger.info(f"------------ENDING CYCLE:{cycle_no}------------")
