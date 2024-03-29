import pytest
import logging
from ocs_ci.framework.testlib import bugzilla, tier1

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import blue_squad

log = logging.getLogger(__name__)


@tier1
@bugzilla("2239208")
@blue_squad
@pytest.mark.polarion_id("OCS-5476")
class TestPvcCreationInMegabytes:
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
        given_pvc_size = ["499", "500", "501"]
        # conversion formulae for 'M' to bytes
        mega_bytes_to_bytes = 1000 * 1000
        # conversion formulae for 'Mi' to bytes
        mebi_bytes_to_bytes = 1024 * 1024
        for size in given_pvc_size:
            log.info(f"Creating {interface} based PVC with the given size {size}M")
            # Creating PVC using pvc_factory
            pvc_obj = pvc_factory(
                interface=interface, access_mode=access_mode, size=size, size_unit="M"
            )
            # getting pvc yaml using pvc_obj.get() and getting the pvc capacity from the yaml
            actual_pvc_size = pvc_obj.get().get("status").get("capacity").get("storage")
            log.info(f"PVC created with the capacity of {actual_pvc_size} ")
            created_pvc_in_bytes = int(actual_pvc_size[:-2]) * mebi_bytes_to_bytes
            requested_pvc_in_bytes = int(size) * mega_bytes_to_bytes
            if created_pvc_in_bytes >= requested_pvc_in_bytes:
                log.info(
                    f"Created PVC {created_pvc_in_bytes} bytes where the requested was {requested_pvc_in_bytes} bytes"
                )
            else:
                assert (
                    False
                ), f"Actual PVC size {actual_pvc_size} is different than the given PVC size {size}M"
