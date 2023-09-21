import logging
import pytest
from time import sleep
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    skipif_ocs_version,
    green_squad,
)
from ocs_ci.ocs.exceptions import (
    PoolDataNotErased,
    PvcNotDeleted,
)
from ocs_ci.ocs.cluster import get_byte_used_by_pool
from ocs_ci.ocs.resources.pod import delete_pods
from ocs_ci.ocs.resources.pvc import (
    delete_pvcs,
    get_all_pvcs_in_storageclass,
)
from ocs_ci.ocs.defaults import MAX_BYTES_IN_POOL_AFTER_DATA_DELETE


log = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_external_mode
@skipif_ocs_version("<4.6")
@pytest.mark.polarion_id("OCS-2393")
class TestMultipleScCompRepDataDelete(ManageTest):
    """
    Create a 2 Storage Class on a new rbd pool with
    different replica and compression options. Create pvc
    and pods and write IO. After that delete the data and
    check that the data is erased
    """

    def test_multiple_sc_comp_rep_data_deletion(
        self, storageclass_factory, pvc_factory, pod_factory
    ):
        """
        This test function does below,
        *. Creates 2 Storage Class with creating new rbd pool
        *. Creates PVCs using new Storage Class
        *. Mount PVC to an app pod
        *. Run IO on an app pod
        *. Delete the pods and pvc
        *. Verify that the data is deleted

        """
        log.info("Creating storageclasses with compression and replica3")
        interface_type = constants.CEPHBLOCKPOOL
        sc_obj1 = storageclass_factory(
            interface=interface_type,
            new_rbd_pool=True,
            replica=3,
            compression="aggressive",
        )
        log.info("Creating storageclasses with compression and replica2")
        sc_obj2 = storageclass_factory(
            interface=interface_type,
            new_rbd_pool=True,
            replica=2,
            compression="aggressive",
        )

        sc_obj_list = [sc_obj1, sc_obj2]
        pod_obj_list = []
        pvc_obj_list = []

        log.info("Creating PVCs and PODs")
        for sc_obj in sc_obj_list:
            pvc_obj = pvc_factory(
                interface=interface_type, storageclass=sc_obj, size=10
            )
            pvc_obj_list.append(pvc_obj)
            pod_obj_list.append(pod_factory(interface=interface_type, pvc=pvc_obj))

        log.info("Running IO on pods")
        for pod_obj in pod_obj_list:
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

        log.info("deleting PODs and PVCs")
        delete_pods(pod_obj_list, wait=True)
        delete_pvcs(pvc_obj_list, concurrent=True)

        log.info("Wait for 15 seconds for all data to delete")
        sleep(15)
        log.info("Checking stats after deleting PODs and PVCs")
        for sc_obj in sc_obj_list:
            pvc_list = get_all_pvcs_in_storageclass(sc_obj.name)
            if len(pvc_list) == 0:
                cbp_name = sc_obj.get()["parameters"]["pool"]
                ceph_pool_byte_used = get_byte_used_by_pool(cbp_name)
                log.info(f"pool {cbp_name} has {ceph_pool_byte_used} bytes used")
                if ceph_pool_byte_used > MAX_BYTES_IN_POOL_AFTER_DATA_DELETE:
                    raise PoolDataNotErased(
                        f"Pool {cbp_name} has {ceph_pool_byte_used} bytes which were not deleted"
                    )
            else:
                raise PvcNotDeleted(f"PVC {pvc_list} were not deleted")
