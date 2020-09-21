"""
Utility functions that are used as a part of OCP or OCS deployments
"""
import logging

import requests

logger = logging.getLogger(__name__)


def get_ocp_ga_version(channel):
    """
    Retrieve the latest GA version for

    Args:
        channel (str): the OCP version channel to retrieve GA version for

    Returns:
        str: latest GA version for the provided channel.
            An empty string is returned if no version exists.


    """
    logger.debug("Retrieving GA version for channel: %s", channel)
    url = "https://api.openshift.com/api/upgrades_info/v1/graph"
    headers = {"Accept": "application/json"}
    payload = {"channel": f"stable-{channel}"}
    r = requests.get(url, headers=headers, params=payload)
    nodes = r.json()["nodes"]
    if nodes:
        versions = [node["version"] for node in nodes]
        versions.sort()
        ga_version = versions[-1]
        logger.debug("Found GA version: %s", ga_version)
        return ga_version
    logger.debug("No GA version found")
    return ""
