"""
This module implements all the functionalities required for setting up and
running Fio workloads on the pods.

This module implements few functions::

    setup(): for setting up fio utility on the pod and any necessary
        environmental params.
    run(): for running fio on pod on specified mount point

Note: The above mentioned functions will be invoked from Workload.setup()
and Workload.run() methods along with user provided parameters.
"""
import logging
from ocs_ci.utility.workloads.helpers import find_distro, DISTROS

log = logging.getLogger(__name__)


def setup(**kwargs):
    """
    setup fio workload

    Args:
        **kwargs (dict): fio setup configuration.
            At this point in time only argument present in kwargs will be
            'pod' on which we want to setup. In future if we move to
            containerized fio then pod.yaml will be presented in kwargs.

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

    cmd = f"{pkg_mgr} -y install fio"
    return io_pod.exec_cmd_on_pod(cmd, out_yaml_format=False)


def run(**kwargs):
    """
    Run fio with params from kwargs.
    Default parameter list can be found in
    templates/workloads/fio/workload_io.yaml and user can update the
    dict as per the requirement.

    Args:
        kwargs (dict): IO params for fio

    Result:
        result of command
    """
    io_pod = kwargs.pop('pod')
    st_type = kwargs.pop('type')
    path = kwargs.pop('path')

    fio_cmd = "fio"
    args = ""
    for k, v in kwargs.items():
        if k == 'filename':
            if st_type == 'fs':
                args = args + f" --{k}={path}/{v}"
            else:
                # For raw block device
                args = args + f" --{k}={path}"
        else:
            args = args + f" --{k}={v}"
    fio_cmd = fio_cmd + args
    fio_cmd += " --output-format=json"
    log.info(f"Running cmd: {fio_cmd}")

    return io_pod.exec_cmd_on_pod(fio_cmd, out_yaml_format=False)
