import logging
import yaml

from ocs_ci.framework import config

logger = logging.getLogger(__name__)


def update_kubeconfig_with_proxy_url_for_client(kubeconfig):
    """
    If it is a proxy environment and client_http_proxy is defined, update
    each cluster configuration in kubeconfig with proxy-url parameter to
    redirect client access through proxy server

    Args:
        kubeconfig (str): path to kubeconfig file

    """
    if (
        config.DEPLOYMENT.get("proxy")
        or config.DEPLOYMENT.get("disconnected")
        or config.ENV_DATA.get("private_link")
    ) and config.ENV_DATA.get("client_http_proxy"):
        logger.info(
            f"Updating kubeconfig '{kubeconfig}' with 'proxy-url: "
            f"{config.ENV_DATA.get('client_http_proxy')}' parameter."
        )
        with open(kubeconfig, "r") as f:
            kd = yaml.safe_load(f)
        for cluster in kd["clusters"]:
            cluster["cluster"]["proxy-url"] = config.ENV_DATA.get("client_http_proxy")
        with open(kubeconfig, "w") as f:
            yaml.dump(kd, f)
