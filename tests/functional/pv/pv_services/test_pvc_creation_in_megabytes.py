import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import E2ETest, tier1
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@tier1
@green_squad
@pytest.mark.polarion_id("OCS-5476")
class TestPvcCreationInMegabytes(E2ETest):
    """
    1. This class takes care of creating Cephfs PVC with small size 499M, 500M, 501M.
    2.Passing size_unit as 'M' [mega-bytes] and the created PVC size will be displayed in 'Mi' format in pvc yaml.
    3.Verifying the created PVC size by converting it from 'Mi' [mebi-bytes] to 'bytes'
    and comparing it with the given size.
    4. The expectation here is the created PVC sized should be greater than or equal to the given size.
    It should not be lesser than the given size.
    """

    def test_pvc_creation_in_megabytes(self, pvc_factory):
        """
        This function facilitates
        1. Create PVC in Cephfs with size_unit as "M" . Used access mode is RWX.
        2. Convert the PVC's size from 'Mi' to bytes.
        3. Validate created PVC size with the given_pvc_size in "M" format after converting it into bytes.
        """
        access_mode = constants.ACCESS_MODE_RWX
        interface = constants.CEPHFILESYSTEM
        given_pvc_sizes_mb = ["499", "500", "501"]
        # conversion formulae for 'M' to bytes
        mega_bytes_to_bytes = 1000 * 1000
        # conversion formulae for 'Mi' to bytes
        mebi_bytes_to_bytes = 1024 * 1024
        for size_mb in given_pvc_sizes_mb:
            log.info(f"Creating {interface} based PVC with the given size {size_mb}M")
            # Creating PVC using pvc_factory
            pvc_obj = pvc_factory(
                interface=interface,
                access_mode=access_mode,
                size=size_mb,
                size_unit="M",
            )
            # getting pvc yaml using pvc_obj.get() and getting the pvc capacity from the yaml
            actual_pvc_size = pvc_obj.get().get("status").get("capacity").get("storage")
            log.info(f"PVC created with the capacity of size {actual_pvc_size} ")
            created_pvc_size_bytes = int(actual_pvc_size[:-2]) * mebi_bytes_to_bytes
            requested_pvc_size_bytes = int(size_mb) * mega_bytes_to_bytes
            assert created_pvc_size_bytes >= requested_pvc_size_bytes, (
                f"Actual PVC size {created_pvc_size_bytes} bytes is less"
                f" than the given PVC size {requested_pvc_size_bytes}"
            )
            log.info(
                f"Created PVC {created_pvc_size_bytes} bytes where the requested was {requested_pvc_size_bytes} bytes"
            )
