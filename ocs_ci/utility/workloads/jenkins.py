"""
This module implements all the functionalities required for setting up and
running "Jenkins" like workloads on the pods.

This module implements few functions::

    setup(): for setting up git utility on the pod and any necessary
        environmental params.
    run(): for running 'git clone' on pod to simulate a working environment for
        a developer

Note: The above mentioned functions will be invoked from Workload.setup()
and Workload.run() methods along with user provided parameters.
"""
import logging
from ocs_ci.utility.workloads.helpers import find_distro, DISTROS

log = logging.getLogger(__name__)


def setup(**kwargs):
    """
    setup git workload

    Args:
        **kwargs (dict): git setup configuration.
            The only argument present in kwargs is 'pod' on which we
            want to setup

    Returns:
        bool: True if setup succeeds else False
    """
    io_pod = kwargs['pod']
    # For first cut doing simple fio install
    distro = find_distro(io_pod)
    pkg_mgr = DISTROS[distro]

    if distro == 'Debian':
        cmd = f'{pkg_mgr} update'
        io_pod.exec_cmd_on_pod(cmd, out_yaml_format=False)

    cmd = f"{pkg_mgr} -y install git"
    return io_pod.exec_cmd_on_pod(cmd, out_yaml_format=False)


def run(**kwargs):
    """
    Run git clone

    Returns:
        str: result of command
    """
    io_pod = kwargs.pop('pod')
    git_repo = kwargs.get('repo', "https://github.com/ceph/ceph.git")
    git_clone_cmd = f"git clone --recursive {git_repo}"
    log.info(f"Running cmd: {git_clone_cmd}")
    return io_pod.exec_cmd_on_pod(git_clone_cmd, out_yaml_format=False)
