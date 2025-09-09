import logging
import os

from ocs_ci.framework import config
from ocs_ci.utility.retry import retry
from ocs_ci.helpers.github import get_asset_from_github
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
)
from ocs_ci.utility.utils import (
    clone_repo,
    exec_cmd,
    get_ocp_version,
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
        get_asset_from_github(
            name="opm",
            owner_repo=config.ENV_DATA.get("opm_owner_repo"),
            release_tag=opm_release_tag,
        )
        opm_version = exec_cmd("opm version")
    logger.info(f"opm tool is available: {opm_version.stdout.decode('utf-8')}")


@retry((CommandFailed,), tries=3, delay=10, backoff=2)
def get_oc_mirror_tool():
    """
    Download and install oc mirror tool.

    """
    try:
        oc_mirror_version = exec_cmd("oc mirror version --v2")
    except (CommandFailed, FileNotFoundError):
        logger.info("oc-mirror tool is not available, installing it")
        prepare_bin_dir()
        bin_dir = os.path.expanduser(config.RUN["bin_dir"])
        # it would be better to directly download pre-build binary, but it is
        # not available yet, so we have to build it from source from
        # https://github.com/openshift/oc-mirror
        oc_mirror_repo = "https://github.com/openshift/oc-mirror.git"
        oc_mirror_dir = os.path.join(constants.EXTERNAL_DIR, "oc-mirror")
        ocp_version = get_ocp_version()
        if ocp_version == "4.20":
            # W/A for https://issues.redhat.com/browse/OCPBUGS-61386
            # https://issues.redhat.com/browse/DFBUGS-3993
            ocp_version = "4.19"
        oc_mirror_branch = f"release-{ocp_version}"
        clone_repo(url=oc_mirror_repo, location=oc_mirror_dir, branch=oc_mirror_branch)
        # build oc-mirror tool
        exec_cmd("make build", cwd=oc_mirror_dir)
        os.rename(
            os.path.join(oc_mirror_dir, "bin/oc-mirror"),
            os.path.join(bin_dir, "oc-mirror"),
        )
        oc_mirror_version = exec_cmd("oc mirror version --v2")
    logger.info(
        f"oc-mirror tool is available: {oc_mirror_version.stdout.decode('utf-8')}"
    )
