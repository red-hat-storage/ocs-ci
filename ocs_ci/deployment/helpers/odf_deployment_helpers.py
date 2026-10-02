"""
This module contains helpers functions needed for
ODF deployment.
"""

import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import defaults
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_osd_pods, get_osd_pod_id
from ocs_ci.utility import templating, version
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.constants import (
    MCLOCK_HIGH_CLIENT_OPS,
    MCLOCK_BALANCED,
    MCLOCK_HIGH_RECOVERY_OPS,
)

logger = logging.getLogger(__name__)


def get_required_csvs():
    """
    Get the mandatory CSVs needed for the ODF cluster

    Returns:
        list: list of CSVs needed

    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    ocs_operator_names = [
        defaults.ODF_CSI_ADDONS_OPERATOR,
        defaults.ODF_OPERATOR_NAME,
        defaults.OCS_OPERATOR_NAME,
        defaults.MCG_OPERATOR,
    ]
    if ocs_version >= version.VERSION_4_16:
        operators_4_16_additions = [
            defaults.ROOK_CEPH_OPERATOR,
            defaults.ODF_PROMETHEUS_OPERATOR,
            defaults.ODF_CLIENT_OPERATOR,
            defaults.RECIPE_OPERATOR,
        ]
        ocs_operator_names.extend(operators_4_16_additions)
    if ocs_version >= version.VERSION_4_17:
        operators_4_17_additions = [defaults.CEPHCSI_OPERATOR]
        ocs_operator_names.extend(operators_4_17_additions)
    if ocs_version >= version.VERSION_4_18:
        operators_4_18_additions = [defaults.ODF_DEPENDENCIES]
        ocs_operator_names.extend(operators_4_18_additions)
    return ocs_operator_names


def set_ceph_config(entity, config_name, value):
    """
    Sets the ceph config values

    Args:
        entity (str): The Ceph entity like "osd", "mon", "mds", etc. but can be "global" as well.
        config_name (str): Name of the Ceph config option (e.g., "bluestore_slow_ops_warn_lifetime").
        value (str): The value to set for the config.

    """
    cmd = f"ceph config set {entity} {config_name} {value}"
    toolbox = get_ceph_tools_pod()
    toolbox.exec_ceph_cmd(cmd)


def is_storage_system_needed():
    """
    Checks whether creation of storage system is needed or not

    Returns:
        bool: True if storage system is need, otherwise False

    """
    storage_system_needed = True
    odf_running_version = version.get_ocs_version_from_csv(only_major_minor=True)
    if odf_running_version >= version.VERSION_4_19:
        storage_system_needed = False
    else:
        logger.debug("Storage system is needed")
    return storage_system_needed


def set_ceph_mclock_config_profile(profile_name, osd_ids=None):
    """
    Set the Ceph mClock profile for the specified OSDs.

    If no OSDs are specified, the profile is applied to all OSDs in the cluster.

    Args:
        profile_name (str): The mClock profile to set ('balanced', 'high_client_ops', 'high_recovery_ops').
        osd_ids (list): List of OSD IDs. If None, the profile is applied to all OSDs.

    Raises:
        ValueError: If the given profile name is invalid.
        CommandFailed: If enabling override or setting the profile fails.

    """
    valid_profiles = {MCLOCK_HIGH_CLIENT_OPS, MCLOCK_BALANCED, MCLOCK_HIGH_RECOVERY_OPS}
    if profile_name not in valid_profiles:
        raise ValueError(
            f"Invalid mClock profile: {profile_name}. Must be one of {valid_profiles}"
        )

    if not osd_ids:
        osd_pods = get_osd_pods()
        osd_ids = [get_osd_pod_id(p) for p in osd_pods]

    toolbox = get_ceph_tools_pod()

    logger.info("Enabling mClock override recovery settings")
    toolbox.exec_ceph_cmd(
        "ceph config set osd osd_mclock_override_recovery_settings true"
    )

    logger.info(f"Setting mClock profile '{profile_name}' for OSDs: {osd_ids}")
    for osd_id in osd_ids:
        set_profile_cmd = (
            f"ceph config set osd.{osd_id} osd_mclock_profile {profile_name}"
        )
        toolbox.exec_ceph_cmd(set_profile_cmd)


def set_ceph_mclock_high_client_recovery_profile(osd_ids=None):
    """
    Apply the 'high_client_ops' mClock profile to specified OSDs (or all OSDs if not provided).

    Args:
        osd_ids (list): List of OSD IDs. If None, the profile is applied to all OSDs.

    Raises:
        CommandFailed: If enabling override or setting the profile fails.

    """
    set_ceph_mclock_config_profile(MCLOCK_HIGH_CLIENT_OPS, osd_ids)


def set_ceph_mclock_balanced_profile(osd_ids=None):
    """
    Apply the 'balanced' mClock profile to specified OSDs (or all OSDs if not provided).

    Args:
        osd_ids (list): List of OSD IDs. If None, the profile is applied to all OSDs.

    Raises:
        CommandFailed: If enabling override or setting the profile fails.

    """
    set_ceph_mclock_config_profile(MCLOCK_BALANCED, osd_ids)


def set_ceph_mclock_high_recovery_profile(osd_ids=None):
    """
    Apply the 'high_recovery_ops' mClock profile to specified OSDs (or all OSDs if not provided).

    Args:
        osd_ids (list): List of OSD IDs. If None, the profile is applied to all OSDs.

    Raises:
        CommandFailed: If enabling override or setting the profile fails.

    """
    set_ceph_mclock_config_profile(MCLOCK_HIGH_RECOVERY_OPS, osd_ids)


def apply_csv_image_overrides(csv_data, overrides):
    """
    Apply image overrides to the deployments defined in a CSV.

    The CSV data is modified in place, nothing is sent to the cluster.

    Args:
        csv_data (dict): Data of the CSV to modify
        overrides (dict): Overrides to apply, see :func:`modify_csv_images`
            for the supported structure

    Returns:
        dict: Mapping of the modified location to the image which was set

    Raises:
        ValueError: If a container or an environment variable requested to be
            overridden is not defined in the CSV

    """
    container_overrides = overrides.get("containers") or {}
    env_overrides = overrides.get("env") or {}
    missing_containers = set(container_overrides)
    missing_envs = set(env_overrides)
    applied = {}

    for deployment in csv_data["spec"]["install"]["spec"]["deployments"]:
        deployment_name = deployment["name"]
        for container in deployment["spec"]["template"]["spec"]["containers"]:
            container_name = container["name"]
            if container_name in container_overrides:
                image = container_overrides[container_name]
                container["image"] = image
                applied[f"{deployment_name}/{container_name}/image"] = image
                missing_containers.discard(container_name)
            for env in container.get("env", []):
                if env["name"] not in env_overrides:
                    continue
                image = env_overrides[env["name"]]
                # An overridden variable is always a plain value, drop
                # valueFrom to keep the resulting env entry valid.
                env.pop("valueFrom", None)
                env["value"] = image
                applied[f"{deployment_name}/{container_name}/env/{env['name']}"] = image
                missing_envs.discard(env["name"])

    if missing_containers or missing_envs:
        raise ValueError(
            f"CSV {csv_data['metadata']['name']} doesn't define everything requested "
            f"to be overridden. Missing containers: {sorted(missing_containers)}, "
            f"missing environment variables: {sorted(missing_envs)}"
        )
    return applied


def modify_csv_images(csv_image_overrides, namespace=None):
    """
    Override container images and image related environment variables in CSVs.

    Compared to the plain string replacement done by
    :func:`ocs_ci.utility.utils.modify_csv`, this targets specific fields of the
    CSV. That makes it possible to replace an image in one place only, even when
    the very same image is referenced by multiple fields, which is the case for
    example for the ocs-operator image and the PROVIDER_API_SERVER_IMAGE
    environment variable.

    Args:
        csv_image_overrides (dict): Mapping of a CSV name prefix to the
            overrides which should be applied to it. Each override supports two
            optional keys, ``containers`` mapping a container name to an image
            and ``env`` mapping an environment variable name to an image.
            Example::

                {
                    "ocs-operator": {
                        "env": {
                            "PROVIDER_API_SERVER_IMAGE": "quay.io/my/ocs:latest",
                        },
                    },
                    "ocs-client-operator": {
                        "containers": {"manager": "quay.io/my/client:latest"},
                    },
                }

        namespace (str): Namespace of the CSVs. Defaults to the cluster
            namespace configured in ENV_DATA.

    Raises:
        ResourceNotFoundError: If no CSV matching one of the prefixes is found
        ValueError: If a container or an environment variable requested to be
            overridden is not defined in the CSV

    """
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    for csv_prefix, overrides in csv_image_overrides.items():
        csvs = get_csvs_start_with_prefix(csv_prefix=csv_prefix, namespace=namespace)
        if not csvs:
            raise ResourceNotFoundError(
                f"No CSV with prefix {csv_prefix} found in namespace {namespace}, "
                "cannot override its images"
            )
        for csv_data in csvs:
            csv_name = csv_data["metadata"]["name"]
            applied = apply_csv_image_overrides(csv_data, overrides)
            logger.info(f"CSV {csv_name} will be modified: {applied}")
            with tempfile.NamedTemporaryFile(
                mode="w+", prefix="csv_image_override", suffix=".yaml", delete=False
            ) as csv_file:
                templating.dump_data_to_temp_yaml(csv_data, csv_file.name)
                exec_cmd(f"oc replace -f {csv_file.name}")
