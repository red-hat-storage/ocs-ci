from ocs_ci.ocs import constants
from ocs_ci.utility.utils import wait_for_machineconfigpool_status
from ocs_ci.ocs.ocp import OCP


def pause_machinehealthcheck():
    """
    During the upgrade process, nodes in the cluster might become temporarily
    unavailable. In the case of worker nodes, the machine health check might
    identify such nodes as unhealthy and reboot them. To avoid rebooting such
    nodes, pause all the MachineHealthCheck resources before updating the
    cluster.
    This step is based on OCP documentation for OCP 4.9 and above.
    """
    ocp = OCP(
        kind=constants.MACHINEHEALTHCHECK,
        namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
    )
    mhcs = ocp.get()
    for mhc in mhcs["items"]:
        ocp.annotate("cluster.x-k8s.io/paused=''", mhc["metadata"]["name"])


def resume_machinehealthcheck(wait_for_mcp_complete=False):
    """
    Resume the machine health checks after updating the cluster. To resume the
    check, remove the pause annotation from the MachineHealthCheck resource.

    Arg:
        wait_for_mcp_complete(bool): If True run wait_for_machineconfigpool_status()
    """
    ocp = OCP(
        kind=constants.MACHINEHEALTHCHECK,
        namespace=constants.OPENSHIFT_MACHINE_API_NAMESPACE,
    )
    mhcs = ocp.get()
    for mhc in mhcs["items"]:
        ocp.annotate("cluster.x-k8s.io/paused-", mhc["metadata"]["name"])
    if wait_for_mcp_complete:
        wait_for_machineconfigpool_status(node_type=constants.WORKER_MACHINE)
