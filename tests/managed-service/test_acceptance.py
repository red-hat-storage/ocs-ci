import logging

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import yellow_squad
from ocs_ci.framework.testlib import ManageTest, acceptance, managed_service_required
from ocs_ci.ocs.resources import pvc
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.helpers import helpers

logger = logging.getLogger(__name__)


@yellow_squad
class TestAcceptance(ManageTest):
    """
    Acceptance test Managed Service

    """

    @acceptance
    @managed_service_required
    def test_acceptance(self, pvc_factory, pod_factory, teardown_factory):
        """
        Acceptance test
        1.Create pvc with all relevant modes
        2.Create new FIO pod for each pvc
        3.Run FIO with verify flag
        4.Create clone to all PVCs
        5.Resize all PVCs
        """
        modes = [
            (
                constants.CEPHBLOCKPOOL,
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_FILESYSTEM,
            ),
            (
                constants.CEPHFILESYSTEM,
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_FILESYSTEM,
            ),
            (
                constants.CEPHFILESYSTEM,
                constants.ACCESS_MODE_RWX,
                constants.VOLUME_MODE_FILESYSTEM,
            ),
            (
                constants.CEPHBLOCKPOOL,
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_BLOCK,
            ),
            (
                constants.CEPHBLOCKPOOL,
                constants.ACCESS_MODE_RWX,
                constants.VOLUME_MODE_BLOCK,
            ),
        ]
        self.pod_objs = list()
        self.pvc_objs = list()
        for mode in modes:
            pvc_obj = pvc_factory(
                interface=mode[0],
                access_mode=mode[1],
                size=2,
                volume_mode=mode[2],
                status=constants.STATUS_BOUND,
            )
            logger.info(
                f"Created new pvc {pvc_obj.name}  sc_name={mode[0]} size=2Gi, "
                f"access_mode={mode[1]}, volume_mode={mode[2]}"
            )
            self.pvc_objs.append(pvc_obj)
            if mode[2] == constants.VOLUME_MODE_BLOCK:
                pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
                storage_type = constants.WORKLOAD_STORAGE_TYPE_BLOCK
                raw_block_pv = True
            else:
                pod_dict_path = constants.NGINX_POD_YAML
                storage_type = constants.WORKLOAD_STORAGE_TYPE_FS
                raw_block_pv = False
            logger.info(
                f"Created new pod sc_name={mode[0]} size=2Gi, access_mode={mode[1]}, volume_mode={mode[2]}"
            )
            pod_obj = pod_factory(
                interface=mode[0],
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
                pod_dict_path=pod_dict_path,
                raw_block_pv=raw_block_pv,
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1GB",
                verify=True,
            )
            self.pod_objs.append(pod_obj)

        for pod_obj in self.pod_objs:
            fio_result = pod_obj.get_fio_results()
            logger.info("IOPs after FIO:")
            reads = fio_result.get("jobs")[0].get("read").get("iops")
            writes = fio_result.get("jobs")[0].get("write").get("iops")
            logger.info(f"Read: {reads}")
            logger.info(f"Write: {writes}")

        self.clone_pvc(teardown_factory=teardown_factory)
        self.resize_pvc(pvc_size_new=3)

    def clone_pvc(self, teardown_factory):
        """
        Clone PVC

        Args:
            teardown_factory: teardown fixture
        """
        for pvc_obj in self.pvc_objs:
            logger.info(
                f"Clone pvc {pvc_obj.name} sc_name={pvc_obj.storageclass.name} size=2Gi, "
                f"access_mode={pvc_obj.access_mode},volume_mode={pvc_obj.get_pvc_vol_mode}"
            )
            clone_yaml = (
                constants.CSI_CEPHFS_PVC_CLONE_YAML
                if pvc_obj.backed_sc == constants.CEPHFILESYSTEM_SC
                else constants.CSI_RBD_PVC_CLONE_YAML
            )
            cloned_pvc_obj = pvc.create_pvc_clone(
                sc_name=pvc_obj.backed_sc,
                parent_pvc=pvc_obj.name,
                clone_yaml=clone_yaml,
                namespace=pvc_obj.namespace,
                storage_size="2Gi",
                volume_mode=pvc_obj.get_pvc_vol_mode,
                access_mode=pvc_obj.access_mode,
            )
            teardown_factory(cloned_pvc_obj)
            helpers.wait_for_resource_state(cloned_pvc_obj, constants.STATUS_BOUND)
            cloned_pvc_obj.reload()

    def resize_pvc(self, pvc_size_new):
        """
        Resize PVC

        Args:
            pvc_size_new (int): new pvc size

        """
        for pvc_obj in self.pvc_objs:
            logger.info(
                f"Resize pvc {pvc_obj.name} sc_name={pvc_obj.storageclass.name}, "
                f"resize from {pvc_obj.size} to {pvc_size_new}, access_mode="
                f"{pvc_obj.access_mode},volume_mode={pvc_obj.get_pvc_vol_mode}"
            )
            pvc_obj.resize_pvc(new_size=pvc_size_new, verify=True)

        logger.info(f"Verified: Size of all PVCs are expanded to {pvc_size_new}G")
        logger.info("Verifying new size on pods.")
        for pod_obj in self.pod_objs:
            if pod_obj.pvc.get_pvc_vol_mode == "Block":
                logger.info(
                    f"Skipping check on pod {pod_obj.name} as volume " f"mode is Block."
                )
                continue

            # Wait for 240 seconds to reflect the change on pod
            logger.info(f"Checking pod {pod_obj.name} to verify the change.")
            for df_out in TimeoutSampler(
                240, 3, pod_obj.exec_cmd_on_pod, command="df -kh"
            ):
                if not df_out:
                    continue
                df_out = df_out.split()
                new_size_mount = df_out[df_out.index(pod_obj.get_storage_path()) - 4]
                if new_size_mount in [
                    f"{pvc_size_new - 0.1}G",
                    f"{float(pvc_size_new)}G",
                    f"{pvc_size_new}G",
                ]:
                    logger.info(
                        f"Verified: Expanded size of PVC {pod_obj.pvc.name} "
                        f"is reflected on pod {pod_obj.name}"
                    )
                    break
                logger.info(
                    f"Expanded size of PVC {pod_obj.pvc.name} is not reflected"
                    f" on pod {pod_obj.name}. New size on mount is not "
                    f"{pvc_size_new}G as expected, but {new_size_mount}. "
                    f"Checking again."
                )
        logger.info(
            f"Verified: Modified size {pvc_size_new}G is reflected on all pods."
        )
