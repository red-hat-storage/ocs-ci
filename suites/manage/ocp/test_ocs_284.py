import os
import logging
import ocs.defaults as defaults
from ocs.ocp import OCP
from utility import utils, templating
from ocsci.enums import TestStatus

log = logging.getLogger(__name__)

RBD_SC_YAML = os.path.join("ocs-deployment", "storage-manifest.yaml")
TEMP_SC_YAML_FILE = '/tmp/tmp-storage-manifest.yaml'

RBD_PVC_YAML = os.path.join("ocs-deployment", "PersistentVolumeClaim.yaml")
TEMP_PVC_YAML_FILE = '/tmp/tmp-persistentVolumeClaim.yaml'

occli = OCP(kind='service', namespace=defaults.ROOK_CLUSTER_NAMESPACE)


def create_rbd_cephpool(poolname, storageclassname):
    """
    Creates rbd storage class and ceph pool
    """
    data = {}
    data['metadata_name'] = poolname
    data['storage_class_name'] = storageclassname
    data['blockpool_name'] = poolname
    _templating = templating.Templating()
    tmp_yaml_file = _templating.render_template(RBD_SC_YAML, data)

    with open(TEMP_SC_YAML_FILE, 'w') as fd:
        fd.write(tmp_yaml_file)
        log.info(f"Creating RBD pool and storage class")
    assert occli.create(TEMP_SC_YAML_FILE)
    log.info(f"RBD pool: {poolname} storage class: {storageclassname} "
             "created successfully")
    log.info(TEMP_SC_YAML_FILE)

def create_pvc(pvcname):
    """
    Creates a pvc with an user provided name
    """
    data = {}
    data['pvc_name'] = pvcname
    _templating = templating.Templating()
    tmp_yaml_file = _templating.render_template(RBD_PVC_YAML, data)
    with open(TEMP_PVC_YAML_FILE, 'w') as fd:
        fd.write(tmp_yaml_file)
        log.info(f"Creating a pvc with name {pvcname}")

    # Code from line:49 to line:76 will be converted to a library so that
    # it can be consumed whenever required
    import shlex
    import subprocess

    def run_ocp_cmd(cmd, **kwargs):

        """
           Run an ocp command locally

           Args:
               cmd (str): command to run

           Returns:
               stdout (str): Decoded stdout of command
               stderr (str): Decoded stderr of command
               returncode (str): return code of command

           """
        log.info(f"Executing ocp command: {cmd}")
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        r= subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            **kwargs
        )
        return r.stdout.decode(), r.stderr.decode(), r.returncode

    oc_cmd = "oc "
    kubeconfig = f"--kubeconfig {os.getenv('KUBECONFIG')}"
    cmd = f"{oc_cmd} {kubeconfig} create -f {TEMP_PVC_YAML_FILE}"
    _, stderr, ret= run_ocp_cmd(cmd)
    if "error" in stderr:
        log.info(f"PVC creation failed with error \n {stderr} \nas EXPECTED")
    else:
        if ret !=0:
            assert "PVC creation succeeded : NOT expected"

def delete_rbd_cephpool():
    """
    Deletes the created ceph pool and storage class
    """
    data = {}
    _templating = templating.Templating()
    tmp_yaml_file = _templating.render_template(RBD_SC_YAML, data)
    log.info("Deleting created temporary sc yaml file")
    assert occli.delete(TEMP_SC_YAML_FILE)
    log.info("Successfully deleted temporary sc yaml file")

def run(**kwargs):
    """
    A simple function to exercise a resource creation through api-client
    """
    pvcname = '@123'
    create_rbd_cephpool("autopoo1","autosc1")
    create_pvc(pvcname)
    utils.delete_file(TEMP_SC_YAML_FILE)
    utils.delete_file(TEMP_PVC_YAML_FILE)
    return TestStatus.PASSED
