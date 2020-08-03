import os
import logging
import shutil
from ocs_ci.utility.utils import run_cmd, clone_repo
from ocs_ci.ocs import ocp

log = logging.getLogger(__name__)


def svt_project_clone():
    """
    This function clones the SVT project.
    """
    clone_repo("https://github.com/openshift/svt.git", "/tmp/svt")


def svt_create_venv_setup():
    """
    This function creates Virtual environemt for SVT project,
    and installs all the dependencies and activate the environment

    """

    run_cmd("virtualenv -p /bin/python2 /tmp/venv")
    run_cmd("/bin/sh -c 'source /tmp/venv/bin/activate && python --version'")
    run_cmd("/bin/sh -c 'source /tmp/venv/bin/activate && pip install -r registry_requirement.txt'")


def svt_cluster_loader(clusterload_file="/tmp/svt/openshift_scalability/config/master-vert.yaml"):
    KUBECONFIG = os.getenv('KUBECONFIG')
    """
    This function can be used to create an environment on top of an OpenShift installation.
    So, basically you can create any number of projects,
    each having any number of following objects -- ReplicationController, Pods, Services, etc..
    https://github.com/openshift/svt/blob/master/openshift_scalability/README.md
    Arguments for cluster-loader.py:
        -f : This is the input config file used to define the test.
        -kubeconfig : kubeconfig path

    Args:
        clusterload_file : clusterloader file

    """

    cwd = os.getcwd()
    os.chdir('/tmp/svt/openshift_scalability/')
    cmd = (
        "/bin/sh -c 'source /tmp/venv/bin/activate && python /tmp/svt/openshift_scalability/cluster-loader.py "
        f"-f {clusterload_file} --kubeconfig {KUBECONFIG}'"
    )
    run_cmd(cmd)
    os.chdir(cwd)


def svt_cleanup():
    """
    Removes clonned SVT project and virtual environemt and Projects
    Created while running SVT

    Raises:
        BaseException: In case any erros occured while removing project and ENV.

    Returns:
        bool: True if No exceptions, False otherwise

    """
    ns_obj = ocp.OCP(kind='namespace')
    try:
        shutil.rmtree('/tmp/svt')
        shutil.rmtree('/tmp/venv')
    except BaseException:
        log.error("Error while cleaning SVT project")

    try:
        project_list = [
            "cakephp-mysql0",
            "dancer-mysql0",
            "django-postgresql0",
            "eap64-mysql0",
            "nodejs-mongodb0",
            "rails-postgresql0",
            "tomcat8-mongodb0"]
        # Reset namespace to default
        ocp.switch_to_default_rook_cluster_project()
        for project in project_list:
            ns_obj.wait_for_delete(resource_name=project)

        return True
    except Exception:
        return False
