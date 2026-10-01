import pytest

from ocs_ci.framework import config


@pytest.fixture()
def fdf_target_channel():
    """
    Return the target FDF channel for Y-stream migration.

    Reads ``DEPLOYMENT.fdf_target_channel`` from config. Falls back to
    constructing ``stable-<version>`` from ``DEPLOYMENT.fdf_upgrade_image_tag``
    or ``ENV_DATA.ocs_version``.

    Example config::

        DEPLOYMENT:
          fdf_target_channel: "stable-4.23"
    """
    channel = config.DEPLOYMENT.get("fdf_target_channel")
    if channel:
        return channel

    version = config.DEPLOYMENT.get("fdf_upgrade_image_tag") or config.ENV_DATA.get(
        "ocs_version", ""
    )
    if version:
        major_minor = ".".join(version.split(".")[:2])
        return f"stable-{major_minor}"

    pytest.skip("fdf_target_channel not configured and version cannot be derived")
