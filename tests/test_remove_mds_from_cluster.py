"""
A Testcase to remove one or more mons
when I/O's are happening.

Polarion-ID- OCS-354

"""

import os
import logging
import yaml
import re
import random
import pytest
from ocs import ocp
from ocsci.testlib import tier1, ManageTest
from ocs import defaults

log = logging.getLogger(__name__)


DEP = ocp.OCP(
    kind='Deployment', namespace=defaults.ROOK_CLUSTER_NAMESPACE
)

MON_YAML = os.path.join("templates/ocs-deployment", "cluster-minimal.yaml")


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Fixture for the test
    """

    self = request.node.cls

    def finalizer():
        teardown(self)
    request.addfinalizer(finalizer)
    setup(self)


def setup(self):
    """
    Setting up the environment for the test
    """

    assert verify_mons_are_present()


def teardown(self):
    """
    Tearing down the environment for the test
    """

    assert add_mons_to_the_cluster(count=1)


def get_mons_from_cluster():
    """
    Getting the list of active mons in the cluster
    if available = 1 then pod is active or else its inactive/
    does not exist.
    """

    ret = DEP.get(resource_name='', out_yaml_format=False,
                  selector='app=rook-ceph-mon')
    available_mon = re.findall(r'[\w-]+mon-+[\w-]', ret)
    return available_mon


def verify_mons_are_present():
    """
    To verify if there are more than 1 mon
    present in the cluster
    """
    list_mons = get_mons_from_cluster()
    mon_count = len(list_mons)
    if (mon_count >= 1):
        return True
    return False


def add_mons_to_the_cluster(count):
    """
    Adding Mons to the cluster
    """

    with open(MON_YAML, 'r') as yaml_file:
        mon_obj = yaml.safe_load(yaml_file)
    mon_obj['spec']['mon']['count'] = count
    with open(MON_YAML, 'w') as yaml_file:
        yaml.dump(mon_obj, yaml_file, default_flow_style=False)
    log.info(f"Change the mon count to {count}")
    assert DEP.apply(yaml_file=MON_YAML)
    return True


def remove_mon_from_cluster(mon):
    """
    Removing the mon pod from deployment
    """

    ret = DEP.delete(resource_name=mon)
    if 'deleted' in ret:
        log.info('The Mon pod %s is successfully removed', mon)
    else:
        log.error('The Mon pod %s is not deleted', mon)


def run_io(pod_name):
    """
    Run io on the mount point
    """
    # To add I/O


@pytest.mark.usefixtures(
    test_fixture.__name__,
)
@tier1
class TestOcs354(ManageTest):
    def test_ocs_354(self):
        list_mons = get_mons_from_cluster()
        # run_io()
        ret = verify_mons_are_present()
        if ret == 0:
            remove_mon_from_cluster(random.choice(list_mons))
        add_mons_to_the_cluster(count=1)

