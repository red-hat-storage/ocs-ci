import logging
from getpass import getuser

from ocs_ci.framework import config
from ocs_ci.utility.utils import get_ocp_version, get_testrun_name

log = logging.getLogger(__name__)


def get_polarion_id(upgrade=False):
    """
    Determine the polarion_id of the deployment or upgrade

    Args:
        upgrade (bool): get upgrade_id if true, else get deployment_id

    Returns:
        str: polarion_id of the deployment or upgrade

    """
    polarion_config = config.REPORTING.get("polarion")
    if polarion_config:
        if upgrade:
            upgrade_id = polarion_config.get("upgrade_id")
            log.info("polarion upgrade_id: %s", upgrade_id)
            return upgrade_id
        else:
            deployment_id = polarion_config.get("deployment_id")
            log.info("polarion deployment_id: %s", deployment_id)
            return deployment_id


def get_rp_launch_attributes():
    """
    Retrieve information from the config to use as launch attributes in ReportPortal.

    Returns:
        dict: ReportPortal launch attributes

    """
    rp_attrs = dict()
    rp_attrs["platform"] = config.ENV_DATA.get("platform")
    rp_attrs["deployment_type"] = config.ENV_DATA.get("deployment_type")
    if config.REPORTING.get("us_ds") == "us":
        rp_attrs["upstream"] = True
    else:
        rp_attrs["downstream"] = True
    rp_attrs["worker_instance_type"] = config.ENV_DATA.get("worker_instance_type")
    rp_attrs["ocp_version"] = get_ocp_version()
    rp_attrs["ocs_version"] = config.ENV_DATA.get("ocs_version")
    if config.ENV_DATA.get("sno"):
        rp_attrs["sno"] = True
    if config.ENV_DATA.get("lvmo"):
        rp_attrs["lvmo"] = True
    rp_attrs["run_id"] = config.RUN.get("run_id")
    if config.DEPLOYMENT.get("ocs_registry_image"):
        ocs_registry_image = config.DEPLOYMENT.get("ocs_registry_image")
        rp_attrs["ocs_registry_image"] = ocs_registry_image
        rp_attrs["ocs_registry_tag"] = ocs_registry_image.split(":")[1]
    if config.DEPLOYMENT.get("ui_deployment"):
        rp_attrs["ui_deployment"] = True
    if config.DEPLOYMENT.get("live_deployment"):
        rp_attrs["live_deployment"] = True
    if config.DEPLOYMENT.get("stage"):
        rp_attrs["stage"] = True
    if not config.DEPLOYMENT.get("allow_lower_instance_requirements"):
        rp_attrs["production"] = True
    if config.ENV_DATA.get("fips"):
        rp_attrs["fips"] = True
    if config.ENV_DATA.get("encryption_at_rest"):
        rp_attrs["encryption_at_rest"] = True
    if config.RUN["skipped_on_ceph_health_ratio"] > 0:
        rp_attrs["ceph_health_skips"] = True
    if (
        config.RUN["skipped_on_ceph_health_ratio"]
        > config.RUN["skipped_on_ceph_health_threshold"]
    ):
        rp_attrs["ceph_health_skips_over_threshold"] = True

    return rp_attrs


def get_rp_launch_name():
    """
    Construct and return the ReportPortal launch name
    Returns:
        str: ReportPortal launch name
    """
    return f"{get_testrun_name()}-{getuser()}"


def get_rp_launch_description():
    """
    Construct and return the ReportPortal launch description.

    Returns:
        (str): ReportPortal launch description

    """
    description = ""
    display_name = config.REPORTING.get("display_name")
    if display_name:
        description += f"Job name: {display_name}\n"
    jenkins_job_url = config.RUN.get("jenkins_build_url")
    if jenkins_job_url:
        description += f"Jenkins job: {jenkins_job_url}\n"
    logs_url = config.RUN.get("logs_url")
    if logs_url:
        description += f"Logs URL: {logs_url}\n"
    additional_info = config.REPORTING.get("rp_additional_info")
    if additional_info:
        description += f"Additional information: {additional_info}\n"
    return description


def update_live_must_gather_image():
    """
    Update live must gather image in the config.
    """
    must_gather_tag = f"v{config.ENV_DATA['ocs_version']}"
    must_gather_image = config.REPORTING["odf_live_must_gather_image"]
    live_must_gather_image = f"{must_gather_image}:{must_gather_tag}"
    log.info(f"Setting live must gather image to: {live_must_gather_image}")
    config.REPORTING["default_ocs_must_gather_latest_tag"] = must_gather_tag
    config.REPORTING["ocs_must_gather_image"] = must_gather_image
