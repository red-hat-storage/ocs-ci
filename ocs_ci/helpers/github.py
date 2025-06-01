import json
import logging
import os
import platform

from ocs_ci.framework import config

from ocs_ci.ocs.exceptions import (
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


def get_asset_from_github(name, owner_repo, release_tag="latest"):
    """
    Download and install asset from github.

    Args:
        name (str): name of the tool which should be downloaded
        owner_repo (str): github repository with the tool in form owner/repo
        release_tag (str): release tag to download (default: latest)

    """
    if release_tag != "latest":
        release_tag = f"tags/{release_tag}"
    releases_api_url = (
        f"https://api.github.com/repos/{owner_repo}/releases/{release_tag}"
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
    release_data = json.loads(get_url_content(releases_api_url, auth=github_auth))

    if platform.system() == "Darwin" and platform.machine() == "x86_64":
        asset_name = "darwin-amd64"
    elif platform.system() == "Darwin" and platform.machine() == "arm64":
        asset_name = "darwin-arm64"
    elif platform.system() == "Linux" and platform.machine() == "x86_64":
        asset_name = "linux-amd64"
    else:
        raise UnsupportedOSType

    for asset in release_data["assets"]:
        if asset_name in asset["name"]:
            download_url = asset["browser_download_url"]
            break
    else:
        raise NotFoundError(
            f"{name} binary for selected type {asset_name} was not found"
        )
    prepare_bin_dir()
    bin_dir = os.path.expanduser(config.RUN["bin_dir"])
    logger.info(f"Downloading tool from '{download_url}' to '{bin_dir}'")
    download_file(download_url, os.path.join(bin_dir, name))
    cmd = f"chmod +x {os.path.join(bin_dir, name)}"
    exec_cmd(cmd)
