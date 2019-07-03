"""
OCS-368
"""
import logging
import pytest
import json
from time import sleep
import threading

from ocs_ci.ocs import ocp, constants
from ocs_ci.framework import config
from ocs_ci.framework.testlib import tier1, ManageTest
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating

log = logging.getLogger(__name__)

OCS_BUG_ID = 'test-ocs-368'

NAMESPACE = ocp.OCP(kind='namespace')
OUR_PVC = None
POD = None


@pytest.fixture(scope='class')
def test_fixture(request):
    """
    Finalize teardown and call setup
    """
    def finalizer():
        teardown()
    request.addfinalizer(finalizer)
    setup()


def check_obj(obj_type):
    """
    Check pvc and pod.  Only one of each will be created in this test.

    Return metadata
    """
    cmd_str = f'oc -n {config.ENV_DATA["my_namespace"]} get {obj_type} -o json'
    objs = json.loads(run_cmd(cmd_str))
    assert len(objs['items']) == 1
    log.info(f"{obj_type} created successfully.")
    return objs['items'][0]['metadata']


def setup():
    """
    Setting up the environment for the test

    Create namespace
    Create storageclass
    Create pvc
    Create pod
    """
    NAMESPACE.create(resource_name=OCS_BUG_ID)
    config.ENV_DATA["my_namespace"] = OCS_BUG_ID

    pvc_data = templating.load_yaml_to_dict(constants.CSI_PVC_YAML)
    pvc_data['metadata']['namespace'] = config.ENV_DATA["my_namespace"]
    pvc_data['spec']['storageClassName'] = 'rook-ceph-block'
    pvc_data['spec']['resources']['requests']['storage'] = '100Gi'
    pvc_name = pvc_data['metadata']['name']
    global OUR_PVC
    OUR_PVC = PVC(**pvc_data)
    OUR_PVC.create()
    pvc_info = check_obj('pvc')['annotations']
    while 'pv.kubernetes.io/bind-completed' not in pvc_info:
        sleep(1)
        pvc_info = check_obj('pvc')['annotations']
    assert pvc_info['pv.kubernetes.io/bind-completed'] == 'yes'
    assert pvc_info['pv.kubernetes.io/bound-by-controller'] == 'yes'

    pod_data = templating.load_yaml_to_dict(constants.CSI_RBD_POD_YAML)
    pod_data['metadata']['namespace'] = config.ENV_DATA["my_namespace"]
    pod_data['metadata']['name'] = f'{OCS_BUG_ID}-pod'
    first_claim = pod_data['spec']['volumes'][0]
    first_claim['persistentVolumeClaim']['claimName'] = pvc_name
    first_container = pod_data['spec']['containers'][0]
    first_volume_mount = first_container['volumeMounts'][0]
    first_volume_mount['mountPath'] = f'/mnt/{OCS_BUG_ID}'

    global POD
    POD = Pod(**pod_data)
    POD.create()
    pod_info = check_obj('pod')
    assert pod_info['namespace'] == OCS_BUG_ID
    global CEPH_TOOL
    CEPH_TOOL = get_ceph_tools_pod()


def teardown():
    """
    Cleanup

    Delete pod
    Delete pvc
    Delete namespace
    Delete pv
    """
    pv_name = OUR_PVC.backed_pv
    POD.delete()
    POD.delete_temp_yaml_file()
    OUR_PVC.delete()
    OUR_PVC.delete_temp_yaml_file()
    NAMESPACE.delete(resource_name=OCS_BUG_ID)
    run_cmd(f'oc delete pv {pv_name}')


def cmd_on_my_namespace(real_cmd):
    """
    Run an oc command on app pod in the test namespace

    Retries if the command fails.

    Raises:
        CommandFailed if a failure limit is reached

    Args:
        real_cmd (str): oc command to be run ('get pods', for example)
    """
    FAIL_LIMIT = 100
    setup_cmd = f'rsh {OCS_BUG_ID}-pod {real_cmd}'
    full_cmd = f'oc -n {config.ENV_DATA["my_namespace"]} {setup_cmd}'
    not_run = True
    fcount = 0
    while not_run:
        try:
            run_cmd(full_cmd)
            not_run = False
        except CommandFailed as ex:
            log.info(f'{ex} raised on {setup_cmd}')
            fcount += 1
            if fcount > FAIL_LIMIT:
                raise CommandFailed(f'Too many failures: {setup_cmd}')
            sleep(5)


def test_add_stuff_to_rbd():
    """
    Make sure that there is data in the rbd pool.

    The /mnt/{OCS_BUG_ID} diretory on the app pod is mounted on a
    block device that is implemented by openshift-storage rbd.
    Writing here will add data to that storage.
    """
    df_info = CEPH_TOOL.exec_ceph_cmd('ceph df')
    prev_avail = df_info['stats']['total_avail_bytes']
    BIG_FILE = '/usr/lib/x86_64-linux-gnu/libicudata.so.57.1'
    for i in range(0, 10):
        param = f'cp {BIG_FILE} /mnt/{OCS_BUG_ID}/xx.{i}'
        cmd_on_my_namespace(param)
    df_info = CEPH_TOOL.exec_ceph_cmd('ceph df')
    now_avail = df_info['stats']['total_avail_bytes']
    assert now_avail < prev_avail


def update_pvc():
    """
    Update the size of the pvc.

    First modify the storage class to allow expansion.
    Then read the pv so that we are sure that the change has taken effect 
    before increasing the size of the pvc
    """
    NEW_SIZE='200Gi'
    cmd_front = f"oc -n {config.ENV_DATA['my_namespace']} patch"
    cmd1_middle = 'storageclass/"rook-ceph-block"'
    cmd2_middle = 'pvc/ocs-pvc'
    cmd_1 = ' '.join([cmd_front, cmd1_middle, '--patch'])
    cmd_1 += " '{\"allowVolumeExpansion\": true}'"
    run_cmd(cmd_1)
    run_cmd('oc get pv')
    cmd_2 = ' '.join([cmd_front, cmd2_middle, '--patch'])
    cmd_2 += " '{\"spec\": {\"resources\": {\"requests\": {\"storage\": \""
    cmd_2 += NEW_SIZE
    cmd_2 += "\"}}}}'"
    run_cmd(cmd_2)


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
@pytest.mark.polarion_id("OCS-368")
class TestOcs368(ManageTest):
    """
    Actual OCS-368 test.
    """
    def test_ocs_368(self):
        """
        The test_add_stuff_to_rbd call makes sure that there is data written
        to the rbd pool

        Two threads are started.  One thread removes an OSD.  A new OSD
        will then be started by Rook.  The other thread modifies the
        pvc size for the pvc corresponding to the rbd pool

        After both thread complete, we wait until all osds are up.

        After all OSDs are up, we wait until ceph is HEALTHY
        """
        test_add_stuff_to_rbd()
        pvc_chg = threading.Thread(target=update_pvc)
        pvc_chg.start()
        cmd_str = 'delete deployment.apps/rook-ceph-osd-2'
        cmd_str = f'oc -n {config.ENV_DATA["cluster_namespace"]} {cmd_str}'
        run_cmd(cmd_str)
        pvc_chg.join()
        while True:
            sleep(300)
            sval = CEPH_TOOL.exec_ceph_cmd('ceph -s')
            num_osds = sval['osdmap']['osdmap']['num_osds']
            num_up_osds = sval['osdmap']['osdmap']['num_up_osds']
            if num_osds == num_up_osds:
                break
        while True:
            health = CEPH_TOOL.exec_ceph_cmd('ceph health')
            if health['status'] == 'HEALTH_OK':
                break
            sleep(300)
            break
