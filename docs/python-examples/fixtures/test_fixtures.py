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
def three_pvcs(pvc_factory, storage_class):
    """
    This is fixture related just to one specific test in this module so no
    need to have it defined in conftest. This returns you three objects of PVC
    which are created in setup phase.

    """
    return [pvc_factory(storage_class) for x in range(3)]


class TestCreatingPVCsFromTest:
    # If the test suite needs to share some resources it's possible to do it
    # via class member like below, when you fill resources from test case.
    # We just need to document that test suite needs run like whole cause if
    # some test case depends on resources created by other test case in the
    # same test suite (class) it's not possible to run just last test case.
    shared_pvcs = []

    @pytest.mark.parametrize("pvcs_number", (2, 4))
    def test_create_pvcs(self, pvcs_number, pvc_factory, storage_class):
        """
        You can access all needed resources via fixtures as parameters in
        method definition like in example above (pvc_factory or storage_class).
        """
        logger.info(
            f"Here you can do something with storage class: "
            f"{storage_class.name}, it should be the same class level scope SC"
            f" which will be used in pvc_factory function as well"
        )
        pvcs_created = [
            pvc_factory(some_parameter="Created from test_create_pvcs") for x
            in range(pvcs_number)
        ]
        self.shared_pvcs.extend(pvcs_created)
        assert len(pvcs_created) == pvcs_number
        logger.info(f"Created pvcs: {[p.name for p in pvcs_created]}")

    def test_alter_shared_pvcs(self, pvc_factory):
        # create one additional pvc and not put it to shared one.
        not_shared_pvc = pvc_factory(some_parameter="Not shared")
        logger.info(f"Not shared pvc has name {not_shared_pvc.name}")
        for i, pvc in enumerate(self.shared_pvcs):
            if i % 2 == 0:
                logger.info(f"Mark PVC: {pvc.name} for delete in next test.")
                pvc.some_parameter = "DELETE"

        logger.info(f"Shared pvcs: {[p.name for p in self.shared_pvcs]}")

    def test_delete_some_shared_pvcs(self):
        logger.info(
            f"self.shared_pvcs are: {[p.name for p in self.shared_pvcs]}"
        )
        deleted_pvcs = []
        for pvc in [
            pvc for pvc in self.shared_pvcs if pvc.some_parameter == "DELETE"
        ]:
            logger.info(f"Deleting shared pvc with name: {pvc.name} from test")
            pvc.delete()
            deleted_pvcs.append(pvc)
        logger.info(
            f"Deleted pvc {[p.name for p in deleted_pvcs]}, shouldn't be "
            f"deleted in finalizer!"
        )


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
        logger.info(
            "Test finished, the rest of PVCs will be deleted in finalizer"
        )


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
        logger.info(
            f"Storage class in class level scope is: {storage_class.name}"
        )
        logger.info(
            f"Storage class in cls_pvc should be the same: "
            f"{cls_pvc.storage_class.name}"
        )
        assert storage_class.name == storage_class.name
