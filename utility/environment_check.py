"""
Util for environment check befora and after test to compare and find stale
leftovers
"""
import logging
import pytest

from ocs import ocp, defaults, constants, exceptions
from deepdiff import DeepDiff

log = logging.getLogger(__name__)


POD = ocp.OCP(kind=constants.POD)
SC = ocp.OCP(kind=constants.STORAGECLASS)
CEPHFILESYSTEM = ocp.OCP(kind=constants.CEPHFILESYSTEM)
CEPHBLOCKPOOL = ocp.OCP(kind=constants.CEPHBLOCKPOOL)
PV = ocp.OCP(kind=constants.PV)
PVC = ocp.OCP(kind=constants.PVC)
SECRET = ocp.OCP(kind=constants.SECRET)
NS = ocp.OCP(kind=constants.NAMESPACE)

ADDED_RESOURCE = 'iterable_item_added'
REMOVED_RESOURCE = 'iterable_item_removed'


@pytest.fixture(scope='class')
def environment_checker(request):
    request.addfinalizer(get_status_after_execution)
    get_status_before_execution()


def get_status_before_execution():
    """

    """
    defaults.ENV_STATUS_PRE['pod'] = POD.get(all_namespaces=True)['items']
    defaults.ENV_STATUS_PRE['sc'] = SC.get(all_namespaces=True)['items']
    defaults.ENV_STATUS_PRE['cephfs'] = CEPHFILESYSTEM.get(
        all_namespaces=True
    )['items']
    defaults.ENV_STATUS_PRE['cephbp'] = CEPHBLOCKPOOL.get(
        all_namespaces=True
    )['items']
    defaults.ENV_STATUS_PRE['pv'] = PV.get(all_namespaces=True)['items']
    defaults.ENV_STATUS_PRE['pvc'] = PVC.get(all_namespaces=True)['items']
    defaults.ENV_STATUS_PRE['secret'] = SECRET.get(
        all_namespaces=True
    )['items']
    defaults.ENV_STATUS_PRE['namespace'] = NS.get(all_namespaces=True)['items']


@pytest.fixture(scope='class')
def get_status_after_execution():
    """

    """
    defaults.ENV_STATUS_POST['pod'] = POD.get(all_namespaces=True)['items']
    defaults.ENV_STATUS_POST['sc'] = SC.get(all_namespaces=True)['items']
    defaults.ENV_STATUS_POST['cephfs'] = CEPHFILESYSTEM.get(
        all_namespaces=True
    )['items']
    defaults.ENV_STATUS_POST['cephbp'] = CEPHBLOCKPOOL.get(
        all_namespaces=True
    )['items']
    defaults.ENV_STATUS_POST['pv'] = PV.get(all_namespaces=True)['items']
    defaults.ENV_STATUS_POST['pvc'] = PVC.get(all_namespaces=True)['items']
    defaults.ENV_STATUS_POST['secret'] = SECRET.get(
        all_namespaces=True
    )['items']
    defaults.ENV_STATUS_POST['namespace'] = NS.get(
        all_namespaces=True
    )['items']
    pod_diff = DeepDiff(
        defaults.ENV_STATUS_PRE['pod'], defaults.ENV_STATUS_POST['pod']
    )
    sc_diff = DeepDiff(
        defaults.ENV_STATUS_PRE['sc'], defaults.ENV_STATUS_POST['sc']
    )
    cephfs_diff = DeepDiff(
        defaults.ENV_STATUS_PRE['cephfs'], defaults.ENV_STATUS_POST['cephfs']
    )
    cephbp_diff = DeepDiff(
        defaults.ENV_STATUS_PRE['cephbp'], defaults.ENV_STATUS_POST['cephbp']
    )
    pv_diff = DeepDiff(
        defaults.ENV_STATUS_PRE['pv'], defaults.ENV_STATUS_POST['pv']
    )
    pvc_diff = DeepDiff(
        defaults.ENV_STATUS_PRE['pvc'], defaults.ENV_STATUS_POST['pvc']
    )
    secret_diff = DeepDiff(
        defaults.ENV_STATUS_PRE['secret'], defaults.ENV_STATUS_POST['secret']
    )
    namespace_diff = DeepDiff(
        defaults.ENV_STATUS_PRE['namespace'],
        defaults.ENV_STATUS_POST['namespace']
    )
    diffs_list = [
        {'pods': pod_diff}, {'sc': sc_diff}, {'cephfs': cephfs_diff},
        {'cephbp': cephbp_diff}, {'pvs': pv_diff}, {'pvcs': pvc_diff},
        {'secret': secret_diff}, {'ns': namespace_diff},
    ]
    leftover_detected = False
    leftovers = {'Leftovers added': [], 'Leftovers removed': []}
    for kind in diffs_list:
        if ADDED_RESOURCE in kind[''.join(kind.keys())]:
            leftovers['Leftovers added'].append({
                ''.join(kind.keys()):
                kind[''.join(kind.keys())][ADDED_RESOURCE][
                    ''.join(kind[''.join(kind.keys())][ADDED_RESOURCE])
                ]
            })
            leftover_detected = True
        if REMOVED_RESOURCE in kind[''.join(kind.keys())]:
            leftovers['Leftovers removed'].append({
                ''.join(kind.keys()):
                kind[''.join(kind.keys())][REMOVED_RESOURCE][
                    ''.join(kind[''.join(kind.keys())][REMOVED_RESOURCE])
                ]
            })
            leftover_detected = True
    if leftover_detected:
        raise exceptions.LeftoversDetected(
            f"\nThere are leftovers in the environment after test case:"
            f"\nResources added: {leftovers['Leftovers added']}"
            f"\nResources removed: {leftovers['Leftovers removed']}"
        )
