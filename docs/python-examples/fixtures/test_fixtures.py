import logging

import pytest

logger = logging.getLogger(__name__)


def alter_pvc(pvc):
    """
    Dummy helper function to alter pvc

    Args:
        pvc (PVC): PVC object reference
    """
    logger.info(f"Doing something with pvc: {pvc}")


@pytest.fixture(scope='class')
def three_pvcs(create_pvcs, storage_class):
    """
    This is fixture related just to one specific test in this module so no
    need to have it defined in conftest
    """
    return create_pvcs(3, storage_class)


class TestCreatingPVCsFromTest:
    # If the test suite needs to share some resources it's possible to do it
    # via class member like below, when you fill resources from test case.
    # We just need to document that test suit needs run like whole cause if
    # some test case depends on resources created by other test case in same 
    # test suite (class) it's not possible to run just last test case.
    shared_pvcs = []

    @pytest.mark.parametrize("pvcs_number", (2, 4))
    def test_create_pvcs(self, pvcs_number, create_pvcs, storage_class):
        """
        You can access all needed resources via fixtures as parameters in
        method definition like in example above (create_pvcs or
        storage_class).
        """
        pvcs_created = create_pvcs(pvcs_number, storage_class)
        len(pvcs_created) == pvcs_number
        logger.info([p.name for p in pvcs_created])

    def test_share_pvcs(self, create_pvcs, storage_class):
        my_shared_pvcs = create_pvcs(2, storage_class)
        logger.info(f"Shared pvcs usage: {[p.name for p in my_shared_pvcs]}")

        # Is this acceptable to do below for share PVCs to test 3rd test?
        self.shared_pvcs.extend(my_shared_pvcs)

    def test_use_shared_pvcs(self):
        logger.info(
            f"self.shared_pvcs are: {[p.name for p in self.shared_pvcs]}"
        )
        pv_to_delete = self.shared_pvcs[0]
        logger.info(f"Deleting shared pvc: {pv_to_delete.name} from test")
        pv_to_delete.delete()
        logger.info(
            f"Deleted pvc {pv_to_delete.name} from test, shouldn't be deleted "
            f"in finalizer!"
        )

        # If you need to do something with pvc outside class you need to just
        # send whatever as reference like in example bellow:
        pvc_to_alter = self.shared_pvcs[1]
        alter_pvc(pvc_to_alter)


class TestPVCsCreatedInSetup:
    """
    This is an example of a test which needs 3 PVCs and does some operation on
    with them and delete 1 PVC as part of test case.
    1) Setup is done outside of test (in setup part of the fixture)
    2) Test is using PVCs, delete on of them
    3) Teardown of PVCs is done outside of test and shouldn't fail cause one
        PVC ore more were already deleted inside test.
    """
    def test_need_3_pvc(self, three_pvcs):
        logger.info(
            f"Here you can use those 3 pvcs: {[p.name for p in three_pvcs]}"
        )
        pvc = three_pvcs[1]
        logger.info(f"Will delete PVC {pvc.name} as part of test")
        pvc.delete()
        logger.info("Test finished")


class TestPVC:
    """
    Simple usage of class which needs to use two resources, PVC and storage
    class where setup and teardown is done outside of tests.
    """
    def test_one_pvc(self, cls_pvc):
        logger.info(
            f"This test is using one pvc: {cls_pvc.name} created in fixture "
            f"setup"
        )

    def test_use_same_one_pvc_plus_storage_class(
        self, cls_pvc, storage_class
    ):
        """
        This test needs to use the same cls_pvc object as previous test and
        also the storage_class object. Because this pvc comes from fixture
        which depends also on storage_class fixture which is class level scope
        here you will get the same object of storage class which has been used
        in cls_pvc fixture.
        """
        logger.info(f"This test is using same one pvc:  {cls_pvc.name}")
        logger.info(f"Storage class used is {storage_class}")
