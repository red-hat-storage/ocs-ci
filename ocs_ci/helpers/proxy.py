import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


def get_cluster_proxies():
    """
    Get http and https proxy configuration:

    * If both ``DEPLOYMENT["proxy"]`` and ``DEPLOYMENT["disconnected"]`` are
      not set or set to ``false``, return empty strings for all ``http_proxy``,
      ``https_proxy`` and ``no_proxy``.
    * Next statements apply, if one of ``DEPLOYMENT["proxy"]`` or
      ``DEPLOYMENT["disconnected"]`` is set to true.
    * If configuration ``ENV_DATA["http_proxy"]`` and ``ENV_DATA["no_proxy"]``
      (and prospectively ``ENV_DATA["https_proxy"]``) exists, return the
      respective values. (If https_proxy not defined, use value from http_proxy.)
    * If configuration ``ENV_DATA["http_proxy"]`` or ``ENV_DATA["no_proxy"]``
      doesn't exist, try to gather cluster wide proxy configuration.
      (If just one of those parameters exists, the configuration value have
      higher priority and the other value is gathered from cluster wide proxy
      configuration object.)
    * Additionally if ``http_proxy`` is gathered from cluster wide proxy
      configuration and ``DEPLOYMENT["disconnected"]`` is not defined or set to
      ``false``, mark the cluster as cluster behind proxy by setting
      ``DEPLOYMENT["proxy"]`` to ``true``.

    Returns:
        tuple: (http_proxy, https_proxy, no_proxy)

    """
    if "http_proxy" in config.ENV_DATA and "no_proxy" in config.ENV_DATA:
        http_proxy = config.ENV_DATA["http_proxy"]
        https_proxy = config.ENV_DATA.get("https_proxy", config.ENV_DATA["http_proxy"])
        no_proxy = config.ENV_DATA["no_proxy"]
    else:
        ocp_obj = OCP(kind=constants.PROXY, resource_name="cluster")
        proxy_obj = ocp_obj.get()
        http_proxy = proxy_obj.get("spec", {}).get("httpProxy", "")
        https_proxy = proxy_obj.get("spec", {}).get("httpsProxy", "")
        no_proxy = proxy_obj.get("status", {}).get("noProxy", "")
        config.ENV_DATA["http_proxy"] = config.ENV_DATA.get("http_proxy", http_proxy)
        config.ENV_DATA["https_proxy"] = config.ENV_DATA.get("https_proxy", https_proxy)
        config.ENV_DATA["no_proxy"] = config.ENV_DATA.get("no_proxy", no_proxy)
        if http_proxy and not config.DEPLOYMENT.get("disconnected"):
            config.DEPLOYMENT["proxy"] = True

    http_proxy = config.ENV_DATA["http_proxy"]
    https_proxy = config.ENV_DATA["https_proxy"]
    no_proxy = config.ENV_DATA["no_proxy"]

    if config.DEPLOYMENT.get("proxy") or config.DEPLOYMENT.get("disconnected"):
        logger.debug("Using http_proxy: '%s'", http_proxy)
        logger.debug("Using https_proxy: '%s'", https_proxy)
        logger.debug("Using no_proxy: '%s'", no_proxy)
        return http_proxy, https_proxy, no_proxy
    else:
        return "", "", ""


def update_container_with_proxy_env(job_pod_dict):
    """
    If applicable, update Job or Pod configuration dict with http_proxy,
    https_proxy and no_proxy env variables (required for disconnected clusters
    and clusters behind proxy).

    Args:
        job_pod_dict (dict): dictionary with Job or Pod configuration (updated
            in-place)

    """
    # configure http[s]_proxy env variable, if required
    try:
        http_proxy, https_proxy, no_proxy = get_cluster_proxies()
        if config.DEPLOYMENT.get("proxy") or config.DEPLOYMENT.get("disconnected"):
            if "containers" in job_pod_dict["spec"]:
                container = job_pod_dict["spec"]["containers"][0]
            else:
                container = job_pod_dict["spec"]["template"]["spec"]["containers"][0]
            if "env" not in container:
                container["env"] = []
            container["env"].append({"name": "http_proxy", "value": http_proxy})
            container["env"].append({"name": "https_proxy", "value": https_proxy})
            container["env"].append({"name": "no_proxy", "value": no_proxy})
    except KeyError as err:
        logging.warning(
            "Http(s)_proxy variable wasn't configured, '%s' key not found.", err
        )
