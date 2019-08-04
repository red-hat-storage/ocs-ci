import pytest
import logging

logger = logging.getLogger(__name__)


class TestPVC:
    """
    usage of class with three Examples
    a) Single use of pvc(or any ocs) resource
    b) Test using 2 or more Resources
    c) Tests needing to create N number of PVC (or OCS objects)
    """
    def test_one_pvc(self, cls_pvc):
        logger.info(
            f"This test is using one pvc: {cls_pvc.name} created in fixture "
            f"setup"
        )
        # use cls_pvc inside your test to do operations on pvc object
        # same concept can be applied to other OCS objects

    def test_use_same_one_pvc_plus_storage_class(
        self, cls_pvc, storage_class
    ):
        """
        In this test cls_pvc and storage_class are provided from conftest.py
        the fxitures objects can then be used inside the test to perform
        required operations on those objects.
        """
        logger.info(f"This test is using same one pvc:  {cls_pvc.name}")
        logger.info(
            f"Storage class is: {storage_class.name}"
        )
        # make use of cls_pvc and storage_class objects

    def test_need_n_pvc(self, pvc_factory):
        """
        Test that needs to create N PVC's or OCS using factory method
        pvc_factory provides closure function that keeps track of created
        objects and deletes them automatically
        """
        # call pvc_factory that creates pvc
        pvc_1 = pvc_factory()
        pvc_2 = pvc_factory()
        # and so on - or use a loop
        # use the pvc
        # perform rest of the test
