"""
Helper function for workloads to use
"""
import logging
from ocs_ci.ocs import exceptions
from ocs_ci.ocs import constants

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
            label_dict = io_pod.get_labels()
            if label_dict and constants.DEPLOYMENTCONFIG in label_dict:
                io_pod.exec_cmd_on_pod(f"{pkg_mgr}", out_yaml_format=False)
            else:
                io_pod.exec_cmd_on_pod(f"which {pkg_mgr}", out_yaml_format=False)
        except exceptions.CommandFailed:
            log.debug(f"Distro is not {distro}")
        else:
            return distro
