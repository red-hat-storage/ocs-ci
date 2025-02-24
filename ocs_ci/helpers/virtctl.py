import logging

from ocs_ci.framework import config

# from ocs_ci.ocs import constants
from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.helpers.github import get_asset_from_github
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


def get_virtctl_tool():
    """
    Download and install virtctl tool.

    """
    try:
        virtctl_version = exec_cmd("virtctl version -c", silent=True)
    except (CommandFailed, FileNotFoundError):
        logger.info("virtctl tool is not available, installing it")
        try:
            cnv_obj = CNVInstaller()
            cnv_obj.download_and_extract_virtctl_binary()
            virtctl_version = exec_cmd("virtctl version -c", silent=True)
        except Exception as err:
            logger.info(
                f"Downloading or extracting virtctl binary from OCP Cluster failed: {err}. "
                "Falling back to download it from upstream github repo."
            )
            virtctl_release_tag = config.ENV_DATA.get("virtctl_release_tag", "latest")
            get_asset_from_github(
                name="virtctl",
                owner_repo=config.ENV_DATA.get("virtctl_owner_repo"),
                release_tag=virtctl_release_tag,
            )
            virtctl_version = exec_cmd("virtctl version -c", silent=True)
    logger.info(f"virtctl tool is available: {virtctl_version.stdout.decode('utf-8')}")
