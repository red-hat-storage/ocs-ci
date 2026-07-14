import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier1, tier2
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    skipif_ocs_version,
    green_squad,
    ec_allowed,
)
from ocs_ci.ocs.cluster import (
    get_percent_used_capacity,
    is_ec_pool_supported,
    validate_compression,
    validate_replica_data,
)

log = logging.getLogger(__name__)


@green_squad
@skipif_external_mode
@skipif_ocs_version("<4.6")
class TestCreateNewScWithNeWRbDPool(ManageTest):
    """
    Create a new  Storage Class on a new rbd pool with
    different replica and compression options
    """

    @pytest.mark.parametrize(
        argnames=[
            "replica",
            "compression",
            "volume_binding_mode",
            "pvc_status",
            "erasure_coded",
        ],
        argvalues=[
            pytest.param(
                *[
                    2,
                    "aggressive",
                    constants.WFFC_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                    False,
                ],
                marks=[tier1, pytest.mark.polarion_id("OCS-2400")],
            ),
            pytest.param(
                *[
                    3,
                    "aggressive",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                    False,
                ],
                marks=[tier2, pytest.mark.polarion_id("OCS-2397")],
            ),
            pytest.param(
                *[
                    2,
                    "none",
                    constants.WFFC_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                    False,
                ],
                marks=[tier2, pytest.mark.polarion_id("OCS-2401")],
            ),
            pytest.param(
                *[
                    3,
                    "none",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                    False,
                ],
                marks=[tier2, pytest.mark.polarion_id("OCS-2406")],
            ),
            pytest.param(
                *[
                    2,
                    "none",
                    constants.WFFC_VOLUMEBINDINGMODE,
                    constants.STATUS_PENDING,
                    True,
                ],
                marks=[
                    ec_allowed,
                    tier2,
                    pytest.mark.polarion_id("OCS-7957"),
                    pytest.mark.skipif(
                        not is_ec_pool_supported(),
                        reason="Erasure coded pools are not supported on this cluster",
                    ),
                ],
            ),
            pytest.param(
                *[
                    3,
                    "none",
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                    constants.STATUS_BOUND,
                    True,
                ],
                marks=[
                    ec_allowed,
                    tier2,
                    pytest.mark.polarion_id("OCS-7958"),
                    pytest.mark.skipif(
                        not is_ec_pool_supported(),
                        reason="Erasure coded pools are not supported on this cluster",
                    ),
                ],
            ),
        ],
    )
    def test_new_sc_new_rbd_pool(
        self,
        replica,
        compression,
        volume_binding_mode,
        pvc_status,
        erasure_coded,
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
            erasure_coded=erasure_coded,
        )

        log.info(f"Creating a PVC using {sc_obj.name}")
        pvc_obj = pvc_factory(
            interface=interface_type, storageclass=sc_obj, size=10, status=pvc_status
        )
        log.info(f"PVC: {pvc_obj.name} created successfully using " f"{sc_obj.name}")

        log.info(f"Creating an app pod and mount {pvc_obj.name}")
        pod_obj = pod_factory(interface=interface_type, pvc=pvc_obj)
        log.info(f"{pod_obj.name} created successfully and mounted {pvc_obj.name}")

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
        if not erasure_coded:
            cbp_name = sc_obj.get().get("parameters").get("pool")
            if compression != "none":
                validate_compression(cbp_name)
            validate_replica_data(cbp_name, replica)
