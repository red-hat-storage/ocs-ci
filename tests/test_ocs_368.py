"""
Test OCS-368
"""
import logging
import subprocess
import tempfile
import json
import shlex
from ocs import ocp
from ocs.volumes import PVC

logger = logging.getLogger(__name__)

STORAGE_CLASS = 'rook-ceph-block'


def run_oc_command(cmd):
    """
    Run an oc command and extract json output.

    This uses temp files because pipes were breaking (please help me reviewers).

    Args:
        cmd (str): oc command to be run

    Returns:
        odata (dict): json information returned from the command run.

    """
    l_cmd = shlex.split(cmd + " -o json")
    tfile = tempfile.mkstemp()
    with open(tfile[1], 'w') as ofile:
        subprocess.call(l_cmd, stdout=ofile)
    with open(tfile[1]) as ifile:
        odata = json.load(ifile)
    return odata


def is_pvc_bound(namespace):
    """
    Look at pvc created for this namespace and check if it is bound.

    Args:
        namespace (str): namespace being checked

    Returns:
        True if pvc is bound, False if not.

    """
    cmd = f"oc -n {namespace} get pvc"
    odata = run_oc_command(cmd)
    return odata['items'][0]['status']['phase'] == 'Bound'


def run(namespace_name):
    """
    Do the steps to test OCS-368.

    Args:
        namespace_name (str): namespace being checked

    Returns:
        True if test passed, False otherwise

    """
    logger.info(f"Storage class is {STORAGE_CLASS} (Step 1 completed)")
    onamespace = ocp.OCP(kind='namespace')
    onamespace.create(resource_name=namespace_name)
    pvc_value = namespace_name + '-pvc'
    newpvc = PVC(name=pvc_value, namespace=namespace_name)
    pvc_name = newpvc.create_pvc(STORAGE_CLASS, pvc_size=100)
    logger.info(f"pvc {pvc_name} created (Step 2 completed)")
    if not is_pvc_bound(namespace_name):
        logger.error('{pvc_name} is not bound')
        return False
    logger.info(f"{pvc_name} is bound (Step 3 completed)")
    # TO DO: More steps needed
    return True
