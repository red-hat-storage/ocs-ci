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


class TestPVCsCreatedInSetup:
    """
    This is an example of a test which needs PVC's precreated and does some op
    with them and delete's 1 PVC as part of test case.
    1) Setup is done outside of test (in setup part of the fixture)
    2) Test is using PVCs, delete on of them
    3) Teardown of PVCs is done outside of test and shouldn't fail cause one
        PVC ore more were already deleted inside test.

    Same concept can be applied to other OCS objects
    """
    def test_precreate_pvc(self, test_pre_create_n_pvc):
        logger.info(
            f"Pre-creating some number of PVC objects"
        )
        pvc = test_pre_create_n_pvc[1]
        logger.info(f"Will delete PVC {pvc.name} as part of test")
        pvc.delete()
        logger.info(
            "Test finished, the rest of PVCs will be deleted in finalizer"
        )
