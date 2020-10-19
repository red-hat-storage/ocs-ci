import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.framework.pytest_customization.marks import skipif_external_mode
from ocs_ci.ocs.cluster import get_percent_used_capacity

log = logging.getLogger(__name__)


@tier1
@skipif_external_mode
class TestCreateNewScWithNeWRbDPool(ManageTest):
    """
    Create a new  Storage Class on a new rbd pool with
    different replica and compression options
    """
    @pytest.mark.parametrize(
        argnames=["replica", "compression"],
        argvalues=[
            pytest.param(
                *[2, 'aggressive']
            ),
            pytest.param(
                *[3, 'aggressive']
            ),
            pytest.param(
                *[2, 'none']
            ),
            pytest.param(
                *[3, 'none']
            ),
        ]
    )
    def test_new_sc_new_rbd_pool(self, replica, compression,
                                 storageclass_factory, pvc_factory, pod_factory
                                 ):
        """
        This test function does below,
        *. Creates Storage Class with creating new rbd pool
        *. Creates PVCs using new Storage Class
        *. Mount PVC to an app pod
        *. Run IO on an app pod
        """
        interface_type = constants.CEPHBLOCKPOOL
        sc_obj = storageclass_factory(interface=interface_type,
                                      new_rbd_pool=True, replica=replica, compression=compression
                                      )

        log.info(f"Creating a PVC using {sc_obj.name}")
        pvc_obj = pvc_factory(interface=interface_type, storageclass=sc_obj)
        log.info(
            f"PVC: {pvc_obj.name} created successfully using "
            f"{sc_obj.name}"
        )

        # Create app pod and mount each PVC
        log.info(f"Creating an app pod and mount {pvc_obj.name}")
        pod_obj = pod_factory(interface=interface_type, pvc=pvc_obj)
        log.info(
            f"{pod_obj.name} created successfully and mounted {pvc_obj.name}"
        )

        # Run IO on each app pod for sometime
        log.info(f"Running FIO on {pod_obj.name}")
        pod_obj.run_io('fs', size='1G')
        get_fio_rw_iops(pod_obj)
        cluster_used_space = get_percent_used_capacity()
        log.info(
            f'Cluster used space with replica size {replica}, '
            f'compression mode {compression}={cluster_used_space}'
        )
