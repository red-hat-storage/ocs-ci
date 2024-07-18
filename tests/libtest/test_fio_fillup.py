"""
Module to perform IOs with several weights
"""
import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import libtest
from ocs_ci.framework.testlib import BaseTest
from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


@libtest
class TestPVCFillup(BaseTest):
    """
    Test PVC Fillup

    """

    @pytest.mark.parametrize(
        argnames=["size", "percentage"], argvalues=[pytest.param(*["10", "50"])]
    )
    def test_fillup_fs(
        self,
        size,
        percentage,
        teardown_factory,
        storageclass_factory,
        interface_iterate,
        pod_factory,
        pvc_factory,
    ):
        """
        Test Fill up the filesystem
        """
        pvc_size = int(size)
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

        # Creating PVC
        self.pvc_obj = pvc_factory(
            interface=self.interface, size=size, status=constants.STATUS_BOUND
        )
        self.pvc_obj.reload()
        teardown_factory(self.pvc_obj)

        # Creating POD which will connect to the PVC
        self.pod_obj = pod_factory(
            interface=self.interface, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

        # Calculation the amount of data to write
        filesize = int(pvc_size * 1024 * (int(percentage) / 100))

        logger.info(
            f"Going to run on PVC of {pvc_size} GB and will fill up {percentage} %"
        )
        logger.info(f"the filesize will be {filesize} MB")
        self.pod_obj.fillup_fs(
            size=filesize,
        )
        logger.info("Waiting for results")

        # Getting the FIO output and verify all data was written
        fio_result = self.pod_obj.get_fio_results()
        writes = int(fio_result.get("jobs")[0].get("write").get("io_kbytes") / 1024)
        logger.info(f"Total Write: {writes} MB")
        assert filesize <= writes, "Not all required data was written"
