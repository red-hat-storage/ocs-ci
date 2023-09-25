# ## implemented ###################
# tests/manage/pv_services/test_dynamic_pvc_accessmodes_with_reclaim_policies.py::TestDynamicPvc
#     test_rwo_dynamic_pvc[CephBlockPool-Retain]
#     test_rwo_dynamic_pvc[CephBlockPool-Delete]
#     test_rwo_dynamic_pvc[CephFileSystem-Retain]
#     test_rwo_dynamic_pvc[CephFileSystem-Delete]
#     test_rwx_dynamic_pvc[CephFileSystem-Retain]
#     test_rwx_dynamic_pvc[CephFileSystem-Delete]
# tests/manage/pv_services/test_pvc_assign_pod_node.py::TestPvcAssignPodNode
#     test_rwo_pvc_assign_pod_node[CephBlockPool]
#     test_rwo_pvc_assign_pod_node[CephFileSystem]
#     test_rwx_pvc_assign_pod_node[CephBlockPool]
#     test_rwx_pvc_assign_pod_node[CephFileSystem]
#
# ## partially implemented
# tests/manage/pv_services/pvc_resize/test_pvc_expansion.py::TestPvcExpand::test_pvc_expansion
#   - the original test_pvc_expansion performs expansion on 5 PVCs (2 cephfs, 3 rbd)
#   - some of the configuration of PVCs and maybe also PODs is/might be different
#
# ## not implemented ###############
# tests/manage/pv_services/test_pvc_delete_verify_size_is_returned_to_backendpool.py
#     test_pvc_delete_and_verify_size_is_returned_to_backend_pool
# tests/manage/pv_services/test_raw_block_pv.py::TestRawBlockPV
#     test_raw_block_pv[Delete]
#     test_raw_block_pv[Retain]

# 1. Create PVCs according to this table:
#   Type                        RWO    RWX  Recliam policy
#   RBD-Filesystemvolume mode   yes    No   delete  storage class default class is delete
#   CEPHFS                      yes    yes  delete
#   RBD-Block                   yes    yes  delete
#   RBD-Filesystemvolume mode   yes    no   retain
#   CEPHFS                      yes    yes  retain
#   RBD-Block                   yes    yes  retain
# 2. Create pods on these PVCs by assigning nodeName to these pods
# 3. Run IOs. Check data integrity
# 4. Expand the PVCs and verify that new size is in effect
# 5. Run IOs. Check data integrity
# 6. Delete Pods
# 7. Delete PVCs. For the PVCs with Reclaim policy set to Delete, make sure the PV is deleted
#    and for those with policy set to retain the PV remains undestroyed

import functools

import logging

# from pytest_check import check

from ocs_ci.framework.testlib import (
    ManageTest,
    acceptance,
)

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.cluster import is_managed_service_cluster
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


@green_squad
@acceptance
class TestPvcAcceptance(ManageTest):
    """
    Acceptance tests for PVC

    Automates the following test cases:
    OCS-530 - RBD Based RWO Dynamic PVC creation with Reclaim policy set to Retain
    OCS-533 - RBD Based RWO Dynamic PVC creation with Reclaim policy set to Delete
    OCS-525 - CephFS Based RWO Dynamic PVC creation with Reclaim policy set to Retain
    OCS-526 - CephFS Based RWO Dynamic PVC creation with Reclaim policy set to Delete

    OCS-542 - CephFS Based RWX Dynamic PVC creation with Reclaim policy set to Retain
    OCS-529 - CephFS Based RWX Dynamic PVC creation with Reclaim policy set to Delete
    """

    # "interface_type", "reclaim_policy"],
    variants = [
        # RWO
        {
            "interface_type": constants.CEPHBLOCKPOOL,
            "reclaim_policy": constants.RECLAIM_POLICY_RETAIN,
            "access_mode": constants.ACCESS_MODE_RWO,
            "skip": is_managed_service_cluster(),
        },
        {
            "interface_type": constants.CEPHBLOCKPOOL,
            "reclaim_policy": constants.RECLAIM_POLICY_DELETE,
            "access_mode": constants.ACCESS_MODE_RWO,
        },
        {
            "interface_type": constants.CEPHFILESYSTEM,
            "reclaim_policy": constants.RECLAIM_POLICY_RETAIN,
            "access_mode": constants.ACCESS_MODE_RWO,
            "skip": is_managed_service_cluster(),
        },
        {
            "interface_type": constants.CEPHFILESYSTEM,
            "reclaim_policy": constants.RECLAIM_POLICY_DELETE,
            "access_mode": constants.ACCESS_MODE_RWO,
        },
        # RWX
        {
            "interface_type": constants.CEPHFILESYSTEM,
            "reclaim_policy": constants.RECLAIM_POLICY_RETAIN,
            "access_mode": constants.ACCESS_MODE_RWX,
            "skip": is_managed_service_cluster(),
        },
        {
            "interface_type": constants.CEPHFILESYSTEM,
            "reclaim_policy": constants.RECLAIM_POLICY_DELETE,
            "access_mode": constants.ACCESS_MODE_RWX,
        },
    ]

    @acceptance
    def test_pvc_acceptance(
        self, pvc_factory, pod_factory, storageclass_factory, teardown_factory
    ):
        """
        RWO Dynamic PVC creation tests with Reclaim policy set to Retain/Delete

        """
        test_variants = [
            PvcAcceptance(
                interface_type=variant["interface_type"],
                reclaim_policy=variant["reclaim_policy"],
                access_mode=variant["access_mode"],
                pvc_factory=pvc_factory,
                pod_factory=pod_factory,
                storageclass_factory=storageclass_factory,
                teardown_factory=teardown_factory,
            )
            for variant in TestPvcAcceptance.variants
            if not variant.get("skip")
        ]

        for test_variant in test_variants:
            test_variant.setup()

        for test_variant in test_variants:
            test_variant.create_pvc()

        for test_variant in test_variants:
            test_variant.create_pods()

        for test_variant in test_variants:
            test_variant.check_pod_running_on_selected_node()

        for test_variant in test_variants:
            test_variant.run_io_on_first_pod()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWX:
                test_variant.run_io_on_second_pod()

        for test_variant in test_variants:
            test_variant.get_iops_from_first_pod()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWX:
                test_variant.get_iops_from_second_pod()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWO:
                test_variant.check_pod_state_containercreating()

        for test_variant in test_variants:
            test_variant.expand_pvc()

        for test_variant in test_variants:
            test_variant.verify_expansion()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWO:
                test_variant.delete_first_pod()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWO:
                test_variant.check_pod_state_running()

        for test_variant in test_variants:
            test_variant.verify_data_on_second_pod()
            if test_variant.access_mode == constants.ACCESS_MODE_RWX:
                test_variant.verify_data_on_first_pod()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWO:
                test_variant.run_io_on_second_pod()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWO:
                test_variant.get_iops_from_second_pod()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWO:
                test_variant.verify_data_on_second_pod()

        for test_variant in test_variants:
            if test_variant.access_mode == constants.ACCESS_MODE_RWX:
                test_variant.verify_data_is_mutable_from_any_pod()

        ocs_version = version.get_semantic_ocs_version_from_config()
        if (ocs_version >= version.VERSION_4_12) and (
            config.ENV_DATA.get("platform") != constants.FUSIONAAS_PLATFORM
        ):
            self.verify_access_token_notin_odf_pod_logs()

    def verify_access_token_notin_odf_pod_logs(self):
        """
        This function will verify logs of kube-rbac-proxy container in
        odf-operator-controller-manager pod shouldn't contain api access token
        """
        odf_operator_pod_objs = pod.get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector_label="app.kubernetes.io/name",
            selector=[constants.ODF_SUBSCRIPTION],
        )
        error_msg = "Authorization: Bearer"
        pod_log = pod.get_pod_logs(
            pod_name=odf_operator_pod_objs[0].name, container="kube-rbac-proxy"
        )
        assert not (
            error_msg in pod_log
        ), f"Logs should not contain the error message '{error_msg}'"


class PvcAcceptance:
    def log_execution(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            logger.info(
                f"Executing '{f.__name__}' for interface type: '{self.interface_type}', "
                f"reclaim policy: '{self.reclaim_policy}', access mode: '{self.access_mode}'"
            )
            return f(self, *args, **kwargs)

        return wrapper

    # @log_execution
    def __init__(
        self,
        interface_type,
        reclaim_policy,
        access_mode,
        pvc_factory,
        pod_factory,
        storageclass_factory,
        teardown_factory,
    ):
        """

        Args:
            interface_type (str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            reclaim_policy (str): The type of reclaim policy
                (eg., 'Delete', 'Retain')
            access_mode (str): access mode (rwo or rwx)
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod
            storageclass_factory: A fixture to create new storage class
            teardown_factory: A fixture to cleanup created resources

        """
        self.interface_type = interface_type
        self.reclaim_policy = reclaim_policy
        self.access_mode = access_mode
        self.pvc_factory = pvc_factory
        self.pod_factory = pod_factory
        self.storageclass_factory = storageclass_factory
        self.teardown_factory = teardown_factory

        self.expected_failure_str = "Multi-Attach error for volume"
        self.storage_type = "fs"
        self.pvc_size = 10  # size in Gi
        # Expand PVC with a small amount to fall behind default quota (100 Gi) for
        # openshift dedicated
        if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
            self.pvc_size_new = 15
        else:
            self.pvc_size_new = 25

    @log_execution
    def setup(self):
        """
        Creates storage class with specified interface and reclaim policy.
        Fetches all worker nodes.

        """
        # Create storage class if reclaim policy is not "Delete"
        self.sc_obj = (
            default_storage_class(self.interface_type)
            if self.reclaim_policy == constants.RECLAIM_POLICY_DELETE
            else self.storageclass_factory(
                interface=self.interface_type, reclaim_policy=self.reclaim_policy
            )
        )
        self.worker_nodes_list = node.get_worker_nodes()

    @log_execution
    def create_pvc(self):
        """
        Create PVC
        """
        logger.info(f"Creating PVC with {self.access_mode} access mode")
        self.pvc_obj = self.pvc_factory(
            interface=self.interface_type,
            storageclass=self.sc_obj,
            size=self.pvc_size,
            access_mode=self.access_mode,
            status=constants.STATUS_BOUND,
        )

    @log_execution
    def create_pods(self):
        """
        Create pods
        """
        logger.info(
            f"Creating first pod on node: {self.worker_nodes_list[0]} "
            f"with pvc {self.pvc_obj.name}"
        )
        self.pod_obj1 = self.pod_factory(
            interface=self.interface_type,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            node_name=self.worker_nodes_list[0],
            pod_dict_path=constants.NGINX_POD_YAML,
        )
        self.teardown_factory(self.pod_obj1)

        logger.info(
            f"Creating second pod on node: {self.worker_nodes_list[1]} "
            f"with pvc {self.pvc_obj.name}"
        )
        self.pod_obj2 = self.pod_factory(
            interface=self.interface_type,
            pvc=self.pvc_obj,
            status=None
            if self.access_mode == constants.ACCESS_MODE_RWO
            else constants.STATUS_RUNNING,
            node_name=self.worker_nodes_list[1],
            pod_dict_path=constants.NGINX_POD_YAML,
        )
        self.teardown_factory(self.pod_obj2)

        node_pod1 = self.pod_obj1.get().get("spec").get("nodeName")
        node_pod2 = self.pod_obj2.get().get("spec").get("nodeName")
        assert node_pod1 != node_pod2, "Both pods are on the same node"

    @log_execution
    def check_pod_running_on_selected_node(self):
        # Confirm that the pods are running on the selected nodes
        helpers.wait_for_resource_state(
            resource=self.pod_obj1, state=constants.STATUS_RUNNING, timeout=120
        )
        self.pod_obj1.reload()
        assert pod.verify_node_name(
            self.pod_obj1, self.worker_nodes_list[0]
        ), "Pod is running on a different node than the selected node"

        if self.access_mode == constants.ACCESS_MODE_RWX:
            helpers.wait_for_resource_state(
                resource=self.pod_obj2, state=constants.STATUS_RUNNING, timeout=120
            )
            self.pod_obj2.reload()
            assert pod.verify_node_name(
                self.pod_obj2, self.worker_nodes_list[1]
            ), "Pod is running on a different node than the selected node"

    @log_execution
    def run_io_on_first_pod(self):
        """
        Run IO on first pod
        """
        logger.info(f"Running IO on first pod {self.pod_obj1.name}")
        self.file_name1 = self.pod_obj1.name
        self.pod_obj1.run_io(
            storage_type=self.storage_type, size="1G", fio_filename=self.file_name1
        )

    @log_execution
    def run_io_on_second_pod(self):
        """
        Run IO on second pod
        """
        logger.info(f"Running IO on second pod {self.pod_obj2.name}")
        self.file_name2 = self.pod_obj2.name
        self.pod_obj2.run_io(
            storage_type=self.storage_type, size="1G", fio_filename=self.file_name2
        )

    @log_execution
    def get_iops_from_first_pod(self):
        """
        get fio iops and checksum from first pod
        """
        pod.get_fio_rw_iops(self.pod_obj1)
        self.md5sum_pod1_data = pod.cal_md5sum(
            pod_obj=self.pod_obj1, file_name=self.file_name1
        )

    @log_execution
    def get_iops_from_second_pod(self):
        """
        get fio iops and checksum from second pod
        """
        pod.get_fio_rw_iops(self.pod_obj2)
        self.md5sum_pod2_data = pod.cal_md5sum(
            pod_obj=self.pod_obj2, file_name=self.file_name2
        )

    @log_execution
    def check_pod_state_containercreating(self):
        """
        If ODF < 4.12 verify that second pod is still in ContainerCreating state
        and not able to attain Running state due to expected failure
        """
        if self.access_mode == constants.ACCESS_MODE_RWO and (
            version.get_semantic_ocs_version_from_config() < version.VERSION_4_12
            or self.interface_type == constants.CEPHBLOCKPOOL
        ):
            logger.info(
                f"Verify that second pod {self.pod_obj2.name} is still in ContainerCreating state"
            )
            helpers.wait_for_resource_state(
                resource=self.pod_obj2, state=constants.STATUS_CONTAINER_CREATING
            )
            self.verify_expected_failure_event(
                ocs_obj=self.pod_obj2, failure_str=self.expected_failure_str
            )

    @log_execution
    def expand_pvc(self):
        """
        Modify size of PVC
        """

        logger.info(
            f"Expanding size of PVC {self.pvc_obj.name} to {self.pvc_size_new}G"
        )
        self.pvc_obj.resize_pvc(self.pvc_size_new, False)

    @log_execution
    def verify_expansion(self):
        """
        Verify new size of pvc on pods
        """
        self.pvc_obj.verify_pvc_size(self.pvc_size_new)

        logger.info("Verifying new size on pods.")
        # Wait for 240 seconds to reflect the change on pod
        pods_for_check = [self.pod_obj1]
        if self.access_mode == constants.ACCESS_MODE_RWX:
            pods_for_check.append(self.pod_obj2)
        for pod_obj in pods_for_check:
            logger.info(f"Checking pod {pod_obj.name} to verify the change.")
            for df_out in TimeoutSampler(
                240, 3, pod_obj.exec_cmd_on_pod, command="df -kh"
            ):
                if not df_out:
                    continue
                df_out = df_out.split()
                new_size_mount = df_out[df_out.index(pod_obj.get_storage_path()) - 4]
                if new_size_mount in [
                    f"{self.pvc_size_new - 0.1}G",
                    f"{float(self.pvc_size_new)}G",
                    f"{self.pvc_size_new}G",
                ]:
                    logger.info(
                        f"Verified: Expanded size of PVC {pod_obj.pvc.name} "
                        f"is reflected on pod {pod_obj.name}"
                    )
                    break
                logger.info(
                    f"Expanded size of PVC {pod_obj.pvc.name} is not reflected"
                    f" on pod {pod_obj.name}. New size on mount is not "
                    f"{self.pvc_size_new}G as expected, but {new_size_mount}. "
                    f"Checking again."
                )

    @log_execution
    def delete_first_pod(self):
        """
        Delete firts pod
        """
        logger.info(
            f"Deleting first pod so that second pod can attach PVC {self.pvc_obj.name}"
        )
        self.pod_obj1.delete()
        self.pod_obj1.ocp.wait_for_delete(resource_name=self.pod_obj1.name)

    @log_execution
    def check_pod_state_running(self):
        """
        Wait for second pod to be in Running state
        """
        helpers.wait_for_resource_state(
            resource=self.pod_obj2, state=constants.STATUS_RUNNING, timeout=240
        )

    @log_execution
    def verify_data_on_first_pod(self):
        """
        Verify data on first pod (generated on second pod)
        """
        logger.info(f"Verify data on first pod {self.pod_obj1.name}")
        pod.verify_data_integrity(
            pod_obj=self.pod_obj1,
            file_name=self.file_name2,
            original_md5sum=self.md5sum_pod2_data,
        )

    @log_execution
    def verify_data_on_second_pod(self):
        """
        Verify data on second pod (generated on first pod)
        """
        logger.info(f"Verify data on second pod {self.pod_obj2.name}")
        pod.verify_data_integrity(
            pod_obj=self.pod_obj2,
            file_name=self.file_name1,
            original_md5sum=self.md5sum_pod1_data,
        )

    @log_execution
    def verify_data_is_mutable_from_any_pod(self):
        """
        Verify that data is mutable from any pod
        """
        logger.info("Perform modification of files from alternate pod")
        # Access and rename file written by pod-2 from pod-1
        file_path2 = pod.get_file_path(self.pod_obj2, self.file_name2)
        logger.debug(file_path2)
        self.pod_obj1.exec_cmd_on_pod(
            command=f'bash -c "mv {file_path2} {file_path2}-renamed"',
            out_yaml_format=False,
        )

        # Access and rename file written by pod-1 from pod-2
        file_path1 = pod.get_file_path(self.pod_obj1, self.file_name1)
        logger.debug(file_path1)
        self.pod_obj2.exec_cmd_on_pod(
            command=f'bash -c "mv {file_path1} {file_path1}-renamed"',
            out_yaml_format=False,
        )

        logger.info("Verify presence of renamed files from both pods")
        file_names = [f"{file_path1}-renamed", f"{file_path2}-renamed"]
        for file in file_names:
            assert pod.check_file_existence(
                self.pod_obj1, file
            ), f"File {file} doesn't exist"
            logger.info(f"File {file} exists in {self.pod_obj1.name} ")
            assert pod.check_file_existence(
                self.pod_obj2, file
            ), f"File {file} doesn't exist"
            logger.info(f"File {file} exists in {self.pod_obj2.name}")

    @retry(UnexpectedBehaviour, tries=10, delay=5, backoff=1)
    def verify_expected_failure_event(self, ocs_obj, failure_str):
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
            raise UnexpectedBehaviour(
                f"Failure string {failure_str} is not found in oc describe" f" command"
            )
