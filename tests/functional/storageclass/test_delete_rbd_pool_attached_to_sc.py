import logging
import subprocess
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    ignore_resource_not_found_error_label,
    tier3,
    green_squad,
    skipif_ibm_cloud_managed,
    runs_on_provider,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import get_percent_used_capacity, CephCluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator

logger = logging.getLogger(__name__)


def preconditions_rbd_pool_created_associated_to_sc(
    compression,
    pod_factory,
    pvc_factory,
    pvc_status,
    replica,
    storageclass_factory_class,
    volume_binding_mode,
):
    """
    Helper method to create storageclass with the pool and verify that in pool list
    and page the storageclass is there.
    Loads the pod deployed on basis of created storageclass using IO job.

    :param compression: compression mode
    :param pod_factory: pod factory fixture
    :param pvc_factory: pvc factory fixture
    :param pvc_status: pvc status
    :param replica: replica size
    :param storageclass_factory_class: storageclass factory fixture
    :param volume_binding_mode: volume binding mode
    :return: ceph blockpool name
    """
    interface_type = constants.CEPHBLOCKPOOL
    sc_obj = storageclass_factory_class(
        interface=interface_type,
        new_rbd_pool=True,
        replica=replica,
        compression=compression,
        volume_binding_mode=volume_binding_mode,
        pool_name="test-pool",
    )
    logger.info(f"Creating a PVC using {sc_obj.name}")
    pvc_obj = pvc_factory(
        interface=interface_type,
        storageclass=sc_obj,
        size=10,
        status=pvc_status,
    )
    logger.info(f"PVC: {pvc_obj.name} created successfully using {sc_obj.name}")
    logger.info(f"Creating an app pod and mounting {pvc_obj.name}")
    pod_obj = pod_factory(interface=interface_type, pvc=pvc_obj)
    logger.info(f"{pod_obj.name} created successfully and mounted {pvc_obj.name}")
    logger.info(f"Running FIO on {pod_obj.name}")
    pod_obj.run_io(
        "fs",
        size="1G",
        rate="1500m",
        runtime=60,
        buffer_compress_percentage=60,
        buffer_pattern="0xdeadface",
        bs="8K",
        jobs=5,
        readwrite="readwrite",
    )
    cluster_used_space = get_percent_used_capacity()
    logger.info(
        f"Cluster used space with replica size {replica}, "
        f"compression mode {compression} = {cluster_used_space}"
    )
    cbp_name = sc_obj.get().get("parameters").get("pool")
    return cbp_name


@green_squad
@runs_on_provider
@ignore_resource_not_found_error_label
class TestDeleteRbdPool(ManageTest):
    @skipif_external_mode
    @pytest.mark.parametrize(
        argnames=["replica", "compression", "volume_binding_mode", "pvc_status"],
        argvalues=[
            pytest.param(
                *[
                    2,
                    "aggressive",
                    constants.WFFC_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                ],
                marks=[tier3, pytest.mark.polarion_id("OCS-5134")],
            ),
            pytest.param(
                *[
                    3,
                    "aggressive",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                ],
                marks=[tier3, pytest.mark.polarion_id("OCS-5135")],
            ),
            pytest.param(
                *[
                    2,
                    "none",
                    constants.WFFC_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                ],
                marks=[tier3, pytest.mark.polarion_id("OCS-5136")],
            ),
            pytest.param(
                *[
                    3,
                    "none",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                ],
                marks=[tier3, pytest.mark.polarion_id("OCS-5137")],
            ),
        ],
    )
    def test_delete_rbd_pool_associated_with_sc(
        self,
        replica,
        compression,
        volume_binding_mode,
        pvc_status,
        storageclass_factory_class,
        pvc_factory,
        pod_factory,
        distribute_storage_classes_to_all_consumers_factory,
    ):
        """
        1. Create storageclass with the pool.
        2. Check that in pool list and page the storageclass is there.
        3. Try to delete the pool while it is attached to the storageclass.
        4. Verify pool is Ready.

        """

        logger.test_step(
            f"Create StorageClass with pool (replica={replica}, "
            f"compression={compression}, binding={volume_binding_mode})"
        )
        cbp_name = preconditions_rbd_pool_created_associated_to_sc(
            compression,
            pod_factory,
            pvc_factory,
            pvc_status,
            replica,
            storageclass_factory_class,
            volume_binding_mode,
        )

        logger.test_step("Distribute storage classes to all consumers")
        distr_res = distribute_storage_classes_to_all_consumers_factory()
        if isinstance(distr_res, bool):
            logger.assertion(
                f"Storage classes distribution: expected='True', actual='{distr_res}'"
            )
            assert distr_res, (
                "After distribution storage classes in clients inventories and on provider are not "
                "matching"
            )

        logger.test_step(f"Attempt to delete CephBlockPool {cbp_name}")
        logger.info(f"CephBlockPool name is {cbp_name}. Attempting deletion via CLI")
        try:
            OCP().exec_oc_cmd(
                f"delete cephblockpool {cbp_name} -n {config.ENV_DATA.get('cluster_namespace')}",
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            logger.info(
                f"CephBlockPool {cbp_name} deletion timed out as expected - "
                f"it is referenced by a StorageClass and data loss may happen"
            )

        logger.test_step(f"Verify CephBlockPool {cbp_name} is still Ready")
        ceph_cluster = CephCluster()
        res = ceph_cluster.get_blockpool_status(cbp_name)
        if not res:
            pytest.fail(
                f"cephblockpool '{cbp_name}' state is not ready after deletion. "
                "cephblockpool deletion should fail if referenced by storageclass"
            )
        logger.info(f"CephBlockPool {cbp_name} is still in Ready state")

        logger.test_step("Verify storage classes distribution after deletion attempt")
        distr_res = distribute_storage_classes_to_all_consumers_factory()
        if isinstance(distr_res, bool):
            logger.assertion(
                f"Storage classes distribution after deletion: expected='True', actual='{distr_res}'"
            )
            assert distr_res, (
                "After distribution storage classes in clients inventories and on provider are not "
                "matching"
            )

    @tier3
    @skipif_external_mode
    @skipif_ibm_cloud_managed
    @pytest.mark.parametrize(
        argnames=["replica", "compression", "volume_binding_mode", "pvc_status"],
        argvalues=[
            pytest.param(
                *[
                    3,
                    "aggressive",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                ],
                marks=pytest.mark.polarion_id("OCS-5151"),
            )
        ],
    )
    def test_delete_rbd_pool_attached_to_sc_UI(
        self,
        replica,
        compression,
        volume_binding_mode,
        pvc_status,
        storageclass_factory_class,
        pvc_factory,
        pod_factory,
        setup_ui,
    ):
        logger.test_step(
            f"Create StorageClass with pool for UI test (replica={replica}, "
            f"compression={compression})"
        )
        cbp_name = preconditions_rbd_pool_created_associated_to_sc(
            compression,
            pod_factory,
            pvc_factory,
            pvc_status,
            replica,
            storageclass_factory_class,
            volume_binding_mode,
        )

        logger.test_step(f"Attempt to delete block pool {cbp_name} via UI")
        blocking_pool_tab = PageNavigator().navigate_storage_pools_page()
        logger.assertion(
            f"UI block pool deletion for {cbp_name}: expected='False' (cannot be deleted)"
        )
        assert not blocking_pool_tab.delete_block_pool(
            cbp_name, cannot_be_deleted=True
        ), "blocking pool attached by storage class was deleted, no Warning message was shown"

        sleep_time = 15
        logger.info(f"Waiting {sleep_time}s for UI to update pool state")
        time.sleep(sleep_time)

        logger.test_step(f"Verify block pool {cbp_name} still exists in UI and CLI")
        block_pool_present_ui = blocking_pool_tab.is_block_pool_exist(cbp_name)
        logger.info(f"Block pool present in UI: {block_pool_present_ui}")

        ceph_cluster = CephCluster()
        block_pool_present_cli = ceph_cluster.get_blockpool_status(cbp_name)
        logger.info(f"Block pool present in CLI: {block_pool_present_cli}")

        logger.assertion(
            f"Block pool {cbp_name} existence: "
            f"expected_ui='True', actual_ui='{block_pool_present_ui}', "
            f"expected_cli='True', actual_cli='{block_pool_present_cli}'"
        )
        assert block_pool_present_ui and block_pool_present_cli, (
            "blocking pool attached by storage class was deleted "
            f"UI: {block_pool_present_ui}, "
            f"CLI: {block_pool_present_cli}"
        )
