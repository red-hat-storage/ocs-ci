import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs.ui.block_pool import BlockPoolUI
from ocs_ci.ocs.ui.storageclass import StorageClassUI
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    skipif_ocs_version,
    ui,
    skipif_ibm_cloud_managed,
    green_squad,
    skipif_hci_provider_or_client,
)
from ocs_ci.ocs.cluster import (
    get_percent_used_capacity,
    validate_compression,
    validate_replica_data,
)
from ocs_ci.ocs.constants import CEPHBLOCKPOOL

log = logging.getLogger(__name__)


@green_squad
@ui
@tier1
@skipif_external_mode
@skipif_ocs_version("<4.9")
@skipif_ibm_cloud_managed
@skipif_hci_provider_or_client
class TestRbDPool(ManageTest):
    """
    Create a new  Storage Class on a new rbd pool with
    different replica and compression options
    """

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
                marks=pytest.mark.polarion_id("OCS-3886"),
            ),
            pytest.param(
                *[
                    3,
                    "aggressive",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                ],
                marks=pytest.mark.polarion_id("OCS-3885"),
            ),
        ],
    )
    def test_new_sc_new_rbd_pool(
        self,
        setup_ui_class,
        replica,
        compression,
        volume_binding_mode,
        pvc_status,
        storageclass_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        This test function does below,
        *. Creates Storage Class with creating new rbd pool
        *. Creates PVCs using new Storage Class
        *. Mount PVC to an app pod
        *. Run IO on an app pod
        """
        interface_type = constants.CEPHBLOCKPOOL
        sc_obj = storageclass_factory(
            interface=interface_type,
            new_rbd_pool=True,
            replica=replica,
            compression=compression,
            volume_binding_mode=volume_binding_mode,
        )

        log.info(f"Creating a PVC using {sc_obj.name}")
        pvc_obj = pvc_factory(
            interface=interface_type, storageclass=sc_obj, size=10, status=pvc_status
        )
        log.info(f"PVC: {pvc_obj.name} created successfully using " f"{sc_obj.name}")

        # Create app pod and mount each PVC
        log.info(f"Creating an app pod and mount {pvc_obj.name}")
        pod_obj = pod_factory(interface=interface_type, pvc=pvc_obj)
        log.info(f"{pod_obj.name} created successfully and mounted {pvc_obj.name}")

        # verifying rbd pool in ui
        blockpool_name = sc_obj.interface_name
        blockpool_ui_obj = BlockPoolUI()
        assert blockpool_ui_obj.check_pool_existence(blockpool_name)

        # verify storage classs in UI
        storageclass_name = sc_obj.name
        storageclass_ui_obj = StorageClassUI()
        assert storageclass_ui_obj.verify_storageclass_existence(storageclass_name)

        # Run IO on each app pod for sometime
        log.info(f"Running FIO on {pod_obj.name}")
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
        log.info(
            f"Cluster used space with replica size {replica}, "
            f"compression mode {compression}={cluster_used_space}"
        )
        cbp_name = sc_obj.get().get("parameters").get("pool")
        if compression != "none":
            validate_compression(cbp_name)
        validate_replica_data(cbp_name, replica)

        # printing out values of the stats of blockpool
        blockpool_ui_obj.check_pool_used_capacity(blockpool_name)
        blockpool_ui_obj.check_pool_avail_capacity(blockpool_name)
        blockpool_ui_obj.check_pool_compression_ratio(blockpool_name)
        blockpool_ui_obj.check_pool_compression_eligibility(blockpool_name)
        blockpool_ui_obj.check_pool_compression_savings(blockpool_name)

        # verify block pool stats post running of IO
        checks = {
            "block_pool_ready_state": (
                blockpool_ui_obj.check_pool_status(blockpool_name) == "Ready"
            ),
            "replica_match": (
                blockpool_ui_obj.check_pool_replicas(blockpool_name) == replica
            ),
            "compression_status_enabled": (
                blockpool_ui_obj.check_pool_compression_status(blockpool_name)
            ),
        }
        assert all(checks.values())

    @pytest.mark.polarion_id("OCS-3890")
    def test_multiple_sc_one_pool(
        self,
        setup_ui_class,
        ceph_pool_factory,
        storageclass_factory,
    ):
        """
        This test function does below,
        *. Creates 2 Storage Class with creating one rbd pool for both
        *. Verify the UI for storage class and rbd
        """

        log.info("Creating new pool with replica2 and compression")
        pool_obj = ceph_pool_factory(
            interface=CEPHBLOCKPOOL,
            compression="aggressive",
        )

        log.info(f"Creating first storageclass with pool {pool_obj.name}")
        sc_obj1 = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            new_rbd_pool=False,
            pool_name=pool_obj.name,
        )

        log.info(f"Creating second storageclass with pool {pool_obj.name}")
        sc_obj2 = storageclass_factory(
            interface=CEPHBLOCKPOOL,
            new_rbd_pool=False,
            pool_name=pool_obj.name,
        )

        sc_obj_list = [sc_obj1, sc_obj2]

        # Check if 2 storage class exists in the pool page
        blockpool_ui_obj = BlockPoolUI()
        assert blockpool_ui_obj.check_pool_existence(pool_obj.name)
        assert (
            blockpool_ui_obj.check_storage_class_attached(pool_obj.name) == 2
        ), "The Storage class didnot matched."

        # Check storage class existence in UI
        storageclass_ui_obj = StorageClassUI()
        for storageclass in sc_obj_list:
            storageclass_name = storageclass.name
            assert storageclass_ui_obj.verify_storageclass_existence(storageclass_name)
