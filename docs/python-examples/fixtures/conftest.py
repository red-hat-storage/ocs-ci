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
        if not sc.is_deleted:
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
        if not pvc.is_deleted:
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
    * you need to create PVC resources as part of test and the teardown is
      done automatically by its teardown
    * you need to create PVC resources as part of test and you can also delete
      as part of test, thanks to is_deleted flag the teardown is skipped for
      already deleted PVCs
    * if you need to prepare those resources as part of setup fixture, please
      create wrapper for this factory fixture like will be showed in another
      example.

    Returns:
        function wrapper wrapper_crate_pvcs for ocs.create_pvcs function.
    """
    # this is list of pvcs where we will append all created pvcs by helper
    # function for creating PVC and will be used in finalizer as well.
    pvcs = []

    def fin():
        logger.info("In finalizer")
        for pvc in pvcs:
            if not pvc.is_deleted:
                pvc.delete()

    request.addfinalizer(fin)

    logger.info("Setup of pvcs")

    # There is possibility to define helper function inside this factory like
    # in example:
    # https://docs.pytest.org/en/latest/fixture.html#factories-as-fixtures
    # But then we will use capability of reusability of the code. So this
    # doesn't work for us.
    def wrapper_crate_pvc(some_parameter="default_value"):
        """
        Function wrapper for ocs.create_pvcs. This wrapper append created PVCs
        into pvcs list referenced from create_pvcs factroy which allows to do
        proper teardown of created objects.

        Args:
            some_parameter (str): you can have some parameter here used below
        """
        pvc = ocs.create_pvc(storage_class, some_parameter)
        pvcs.append(pvc)
        return pvc

    return wrapper_crate_pvc
