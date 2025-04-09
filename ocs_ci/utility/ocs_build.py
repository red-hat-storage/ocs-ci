"""
This module is used to return latest OCS internal build for specified OCS
version.
"""

import argparse
import os

from ocs_ci.framework import config
from ocs_ci.utility.framework.initialization import load_config
from ocs_ci.ocs.constants import OCS_VERSION_CONF_DIR
from ocs_ci.utility.utils import get_latest_ds_olm_tag


def init_arg_parser():
    """
    Init argument parser.

    Returns:
        object: Parsed arguments

    """

    parser = argparse.ArgumentParser(description="OCS Internal build version")
    parser.add_argument(
        "--ocs-version",
        action="store",
        required=False,
        default=config.ENV_DATA["ocs_version"],
        help=f"""
            OCS version in format X.Y (e.g. 4.7). If not specified, the default
            value {config.ENV_DATA['ocs_version']} will be used.
        """,
    )
    parser.add_argument(
        "--upgrade",
        action="store_true",
        required=False,
        default=False,
        help="If used, then it will return the latest stable upgrade version instead of latest stable version",
    )
    parser.add_argument(
        "--image",
        action="store_true",
        required=False,
        default=False,
        help="If used the whole image of OCS internal build will be returned",
    )
    return parser.parse_args()


def main():
    """
    Main function
    """
    parser = init_arg_parser()
    ocs_version = parser.ocs_version
    upgrade = parser.upgrade
    image = parser.image
    config.ENV_DATA["ocs_version"] = ocs_version
    version_config_file = os.path.join(OCS_VERSION_CONF_DIR, f"ocs-{ocs_version}.yaml")
    load_config([version_config_file])
    latest_ocs_build = get_latest_ds_olm_tag(stable_upgrade_version=upgrade)
    if image:
        base_image = config.DEPLOYMENT["default_ocs_registry_image"].split(":")[0]
        print(f"{base_image}:{latest_ocs_build}")
        return
    print(latest_ocs_build)


if __name__ == "__main__":
    main()
