import json
import logging
import os
import platform

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    NotFoundError,
    UnsupportedOSType,
)
from ocs_ci.utility.utils import (
    download_file,
    exec_cmd,
    get_url_content,
    prepare_bin_dir,
)

logger = logging.getLogger(__name__)


def get_opm_tool():
    """
    Download and install opm tool.

    """
    try:
        opm_version = exec_cmd("opm version")
    except (CommandFailed, FileNotFoundError):
        logger.info("opm tool is not available, installing it")
        opm_release_tag = config.ENV_DATA.get("opm_release_tag", "latest")
        if opm_release_tag != "latest":
            opm_release_tag = f"tags/{opm_release_tag}"
        opm_releases_api_url = (
            f"https://api.github.com/repos/{config.ENV_DATA.get('opm_owner_repo')}/"
            f"releases/{opm_release_tag}"
        )
        if config.AUTH.get("github"):
            github_auth = (
                config.AUTH["github"].get("username"),
                config.AUTH["github"].get("token"),
            )
            logger.debug(f"Using github authentication (user: {github_auth[0]})")
        else:
            github_auth = None
            logger.warning(
                "Github credentials are not provided in data/auth.yaml file. "
                "You might encounter issues with accessing github api as it "
                "have very strict rate limit for unauthenticated requests "
                "(60 requests per hour). Please check docs/getting_started.md "
                "file to find how to configure github authentication."
            )
        release_data = json.loads(
            get_url_content(opm_releases_api_url, auth=github_auth)
        )

        if platform.system() == "Darwin":
            opm_asset_name = "darwin-amd64-opm"
        elif platform.system() == "Linux":
            opm_asset_name = "linux-amd64-opm"
        else:
            raise UnsupportedOSType

        for asset in release_data["assets"]:
            if asset["name"] == opm_asset_name:
                opm_download_url = asset["browser_download_url"]
                break
        else:
            raise NotFoundError(
                f"opm binary for selected type {opm_asset_name} was not found"
            )
        prepare_bin_dir()
        bin_dir = os.path.expanduser(config.RUN["bin_dir"])
        logger.info(f"Downloading opm tool from '{opm_download_url}' to '{bin_dir}'")
        download_file(opm_download_url, os.path.join(bin_dir, "opm"))
        cmd = f"chmod +x {os.path.join(bin_dir, 'opm')}"
        exec_cmd(cmd)
        opm_version = exec_cmd("opm version")
    logger.info(f"opm tool is available: {opm_version.stdout.decode('utf-8')}")
