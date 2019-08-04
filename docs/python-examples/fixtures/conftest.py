import logging

import pytest

import ocs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope='class')
def storage_class(request):
    """
    Storage class fixture

    Returns:
        StorageClass: object of storage class
    """
    def fin():
            sc.delete()

    request.addfinalizer(fin)

    sc_name = ocs.get_random_name('storage_class')
    logger.info(f"Creating storage class: {sc_name}")
    sc = ocs.StorageClass(sc_name)
    return sc


@pytest.fixture(scope='class')
def cls_pvc(request, storage_class):
    """
    PVC fixture

    Returns:
        PVC: object of PVC class
    """
    def fin():
            pvc.delete()

    request.addfinalizer(fin)
    pvc_name = ocs.get_random_name('pvc')
    logger.info(f"Creating pvc: {pvc_name}")
    pvc = ocs.PVC(pvc_name, storage_class, some_parameter="Default value")
    return pvc


@pytest.fixture(scope='class')
def pvc_factory(request, storage_class):
    """
    Fixture factory for creating pvcs.
    This fixture returns function with which you can create objects of PVC as
    a part of test and cares about teardown of created PVCs.

    Usage of this fixture is for use cases when:
    * you need to create/delete PVC (or any OCS) resources as part of test setup
      and teardown is done automatically by its teardown
    * The factory returns a closure function which keeps track of objects
      it has created and call's the objects delete method to cleanup resources.

    Returns:
        function wrapper wrapper_create_pvcs for ocs.create_pvcs function.
    """
    # create pvcs as list - where we will append all created pvcs inside
    # wrapper function ()
    pvcs = []

    def fin():
        logger.info("In finalizer")
        for pvc in pvcs:
            if not pvc.is_deleted:
                pvc.delete()

    request.addfinalizer(fin)

    logger.info("Setup of pvcs")

    def wrapper_create_pvc(some_parameter="default_value"):
        """
        This wrapper appends created PVCs into pvcs list
        and the list will iterated to teardown the created objects.

        Args:
            some_parameter (str): you can have some parameter here used below
        """
        pvc = ocs.create_pvc(storage_class, some_parameter)
        pvcs.append(pvc)
        return pvc

    return wrapper_create_pvc
