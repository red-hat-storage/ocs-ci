import logging
from ocs_ci.ocs import ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import exec_cmd


logger = logging.getLogger(__name__)


def link_sa_and_secret(sa_name, secret_name, namespace):
    """
    Link service account and secret for pulling of images.

    Args:
        sa_name (str): service account name
        secret_name (str): secret name
        namespace (str): namespace name

    """
    exec_cmd(f"oc secrets link {sa_name} {secret_name} --for=pull -n {namespace}")


def link_all_sa_and_secret(secret_name, namespace):
    """
    Link all service accounts in specified namespace with the secret for pulling
    of images.

    Args:
        secret_name (str): secret name
        namespace (str): namespace name

    """
    service_account = ocp.OCP(kind="serviceAccount", namespace=namespace)
    service_accounts = service_account.get()
    for sa in service_accounts.get("items", []):
        sa_name = sa["metadata"]["name"]
        logger.info(f"Linking secret: {secret_name} with SA: {sa_name}")
        link_sa_and_secret(sa_name, secret_name, namespace)


@retry(CommandFailed, tries=3, delay=60, backoff=1)
def link_all_sa_and_secret_and_delete_pods(secret_name, namespace):
    """
    Link all service accounts in specified namespace with the secret for pulling
    of images.

    Args:
        secret_name (str): secret name
        namespace (str): namespace name

    """
    link_all_sa_and_secret(secret_name, namespace)
    all_pods = get_all_pods(
        namespace=namespace,
        field_selector="status.phase=Pending",
    )
    for pod in all_pods:
        logger.info(f"Deleting pod: {pod.name} in: Pending phase")
        pod.delete(wait=False)
