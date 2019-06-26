import logging

from ocs import exceptions

log = logging.getLogger(__name__)

DISTROS = {"Debian": "apt-get", "RHEL": "yum"}


def find_distro(io_pod):
    """
    Find whats the os distro on pod

    Args:
        io_pod (Pod): app pod object

    Returns:
        distro (str): representing 'Debian' or 'RHEL' as of now
    """
    for distro, pkg_mgr in DISTROS.items():
        try:
            io_pod.exec_cmd_on_pod(f"which {pkg_mgr}")
        except exceptions.CommandFailed:
            log.debug(f"Distro is not {distro}")
        else:
            return distro


def setup(**kwargs):
    """
    setup fio workload

    Args:
        **kwargs (dict): fio setup configuration

    Returns:
        bool: True if setup succeeds else False
    """
    io_pod = kwargs['pod']
    # For first cut doing simple fio install
    distro = find_distro(io_pod)
    pkg_mgr = DISTROS[distro]

    if distro == 'Debian':
        cmd = f'{pkg_mgr} update'
        io_pod.exec_cmd_on_pod(cmd)

    cmd = f"{pkg_mgr} -y install fio"
    return io_pod.exec_cmd_on_pod(cmd)


def run(**kwargs):
    """
    Run fio with params from kwargs

    Args:
        kwargs (dict): params for fio

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
    log.info(f"Running cmd: {fio_cmd}")

    return io_pod.exec_cmd_on_pod(fio_cmd)
