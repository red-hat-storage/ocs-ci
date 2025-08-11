"""
This module contains helper functions which is needed for MCG only deployment
"""

import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.utils import enable_console_plugin
from ocs_ci.utility import templating, version
from ocs_ci.utility.utils import get_primary_nb_db_pod, run_cmd

logger = logging.getLogger(__name__)


def mcg_only_deployment():
    """
    Creates cluster with MCG only deployment
    """
    logger.info("Creating storage cluster with MCG only deployment")
    cluster_data = templating.load_yaml(constants.STORAGE_CLUSTER_YAML)
    cluster_data["spec"]["multiCloudGateway"] = {}
    cluster_data["spec"]["multiCloudGateway"]["reconcileStrategy"] = "standalone"
    del cluster_data["spec"]["storageDeviceSets"]
    cluster_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="cluster_storage", delete=False
    )
    templating.dump_data_to_temp_yaml(cluster_data, cluster_data_yaml.name)
    run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=1200)


def mcg_only_post_deployment_checks():
    """
    Verification of MCG only after deployment
    """
    # check for odf-console
    ocs_version = version.get_semantic_ocs_version_from_config()
    pod = ocp.OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    if ocs_version >= version.VERSION_4_9:
        assert pod.wait_for_resource(
            condition="Running", selector="app=odf-console", timeout=600
        )

    # Enable console plugin
    enable_console_plugin()


def check_if_mcg_root_secret_public():
    """
    Verify if MCG root secret is public

    Returns:
        False if the secrets are not public and True otherwise

    """

    noobaa_endpoint_dep = ocp.OCP(
        kind="Deployment",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT,
    ).get()

    noobaa_core_sts = ocp.OCP(
        kind="Statefulset",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.NOOBAA_CORE_STATEFULSET,
    ).get()

    nb_endpoint_env = noobaa_endpoint_dep["spec"]["template"]["spec"]["containers"][0][
        "env"
    ]
    nb_core_env = noobaa_core_sts["spec"]["template"]["spec"]["containers"][0]["env"]

    def _check_env_vars(env_vars):
        """
        Method verifies the environment variable lists
        if the root secret is public

        """

        for env in env_vars:
            if env["name"] == "NOOBAA_ROOT_SECRET" and "value" in env.keys():
                return True
        return False

    return _check_env_vars(nb_core_env) or _check_env_vars(nb_endpoint_env)


def check_if_mcg_secrets_in_env():
    """
    Verify if mcg secrets are used in noobaa app environment variable except for the POSTGRES/POSTGRESQL

    Returns:
        True if secrets are used in env variable else False

    """
    with config.RunWithProviderConfigContextIfAvailable():
        noobaa_endpoint_env = ocp.OCP(
            kind=constants.DEPLOYMENT,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT,
        ).get()["spec"]["template"]["spec"]["containers"][0]["env"]

        noobaa_operator_env = ocp.OCP(
            kind=constants.DEPLOYMENT,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_OPERATOR_DEPLOYMENT,
        ).get()["spec"]["template"]["spec"]["containers"][0]["env"]

        noobaa_core_env = ocp.OCP(
            kind=constants.STATEFULSET,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_CORE_STATEFULSET,
        ).get()["spec"]["template"]["spec"]["containers"][0]["env"]

        noobaa_db_env = get_primary_nb_db_pod().get()["spec"]["containers"][0]["env"]

    def _check_env_vars(env_vars):
        """
        This will check if any secrets except POSTGRES present
        in the given env vars

        Args:
            env_vars(List): List of env vars ofn particular deployment or sts

        """

        for env in env_vars:
            if (
                env["name"].split("_")[0] != "POSTGRES"
                and env["name"].split("_")[0] != "POSTGRESQL"
            ) and ("valueFrom" in env and "secretKeyRef" in env["valueFrom"]):
                logger.info(
                    f"Non-psql secrets are referenced in the noobaa app env variable under {env}"
                )
                return True
        return False

    return (
        _check_env_vars(noobaa_endpoint_env)
        or _check_env_vars(noobaa_operator_env)
        or _check_env_vars(noobaa_db_env)
        or _check_env_vars(noobaa_core_env)
    )
