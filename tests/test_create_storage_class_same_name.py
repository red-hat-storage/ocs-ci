"""
A test for confirming that creation of two Storageclasses
with the same name fails
TC Name = OCS-322
"""
import os
import yaml
import logging
import ocs.defaults as defaults
import pytest

from ocs import ocp
from ocs import exceptions
from utility import templating, utils
from ocsci import tier1, ManageTest

log = logging.getLogger(__name__)
TEMPLATE_DIR = "templates/CSI/rbd/"
TEMP_YAML_FILE = 'test_storageclass.yaml'
SC = ocp.OCP(
    kind='StorageClass', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    This fixture defines the teardown function.
    """
    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)


def teardown(self):
    """
    Tearing down the environment
    """
    assert SC.delete(yaml_file=TEMP_YAML_FILE)
    utils.delete_file(TEMP_YAML_FILE)


def create_storageclass(expect_fail=False, **kwargs):
    """
    Function to create a storage class and check for
    duplicate storage class name

    Args:
        template_name (str): name of the storageclass template
        expect_fail (bool): To catch the incorrect scenario in OC - in case
            two SCs are indeed created with same name

    """
    SC_YAML = os.path.join(TEMPLATE_DIR, "storageclass.yaml")

    log.info(f'Creating yaml from template')

    sc_yaml_file = templating.generate_yaml_from_jinja2_template_with_data(
        SC_YAML, **kwargs)

    with open(TEMP_YAML_FILE, 'w') as yaml_file:
        yaml.dump(sc_yaml_file, yaml_file, default_flow_style=False)

    # Create a storage class and check for duplicate name
    log.info(
        f"Creating a new Storage Class "
        f"{kwargs['rbd_storageclass_name']} "
    )
    try:
        out = SC.create(yaml_file=TEMP_YAML_FILE)
        sc_name = out.metadata.name
        assert not expect_fail, (
            "SC creation with same name passed. Expected to fail!!!"
        )
        cmd_out = SC.get(resource_name=sc_name)
        if cmd_out['metadata']['name']:

            log.info(f"Storage Class {sc_name} created successfully \n ")
        else:
            log.info(f"Failed to create Storage Class {sc_name} \n ")

    except exceptions.CommandFailed as ecf:
        assert "AlreadyExists" in str(ecf)
        log.info(f"Cannot create two StorageClasses with same name : \n"
                 f"{ecf}")


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestCreateSCWithSameName(ManageTest):
    def test_create_storageclass_with_same_name(self):
        """
        A simple function to exercise a resource creation through api-client
        """
        sc_data = {}
        sc_name = "ocs-322-sc"
        sc_data['rbd_storageclass_name'] = sc_name
        create_storageclass(**sc_data)
        log.info(f"Attempting to create a storageclass "
                 f"with duplicate name {sc_name}")
        create_storageclass(expect_fail=True, **sc_data)
