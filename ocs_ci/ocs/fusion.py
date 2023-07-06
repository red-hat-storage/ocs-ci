import os
import logging
import tempfile
import yaml

from ocs_ci.ocs.exceptions import ConfigurationError
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.managedservice import (
    remove_header_footer_from_key,
    generate_onboarding_token,
    get_storage_provider_endpoint,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.templating import Templating, load_yaml
from ocs_ci.utility.utils import TimeoutSampler, get_ocp_version, exec_cmd
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import (
    check_all_csvs_are_succeeded,
    get_csvs_start_with_prefix,
)

logger = logging.getLogger(name=__file__)

FUSION_TEMPLATE_DIR = os.path.join(constants.TEMPLATE_DIR, "fusion-aas")


def create_fusion_monitoring_resources():
    """
    Create resources used for Managed Fusion aaS Monitoring
    """
    templating = Templating(base_path=FUSION_TEMPLATE_DIR)
    ns_name = config.ENV_DATA["service_namespace"]
    logger.info(f"Creating {ns_name} namespace")
    exec_cmd(["oc", "new-project", ns_name])
    exec_cmd(f"oc label namespace {ns_name} misf.ibm.com/managed=true")
    logger.info("Creating an OperatorGroup")
    og_path = os.path.join(FUSION_TEMPLATE_DIR, "operatorgroup.yaml")
    og_data = load_yaml(og_path)
    helpers.create_resource(**og_data)
    logger.info("Creating a CatalogSource")
    catsource_data = dict()
    catsource_data["image"] = config.ENV_DATA["fusion_catalogsource"]
    template = templating.render_template(
        "catalogsource.yaml.j2",
        catsource_data,
    )
    template = yaml.load(template, Loader=yaml.Loader)
    helpers.create_resource(**template)
    logger.info("Creating a Subscription")
    og_path = os.path.join(FUSION_TEMPLATE_DIR, "subscription.yaml")
    og_data = load_yaml(og_path)
    helpers.create_resource(**og_data)
    logger.info("Waiting for catalogsource")
    catalog_source = CatalogSource(
        resource_name="managed-fusion-catsrc",
        namespace=ns_name,
    )
    catalog_source.wait_for_state("READY")
    logger.info("Creating a monitoring secret")
    secret_data = dict()
    secret_data["pagerduty_config"] = config.ENV_DATA["pagerduty_config"]
    secret_data["smtp_config"] = config.ENV_DATA["smtp_config"]
    template = templating.render_template(
        "monitoringsecret.yaml.j2",
        secret_data,
    )
    template = yaml.load(template, Loader=yaml.Loader)
    helpers.create_resource(**template)


def deploy_odf():
    """
    Create openshift-storage namespace and deploy managedFusionOffering CR there.
    """

    templating = Templating(base_path=FUSION_TEMPLATE_DIR)
    ns_name = config.ENV_DATA["cluster_namespace"]
    logger.info(f"Creating {ns_name} namespace")
    exec_cmd(["oc", "new-project", ns_name])
    exec_cmd(f"oc label namespace {ns_name} misf.ibm.com/managed=true")
    logger.info("Creating the offering CR")
    offering_data = dict()
    offering_data["namespace"] = ns_name
    offering_data["ocp_version"] = get_ocp_version()

    if config.ENV_DATA.get("cluster_type") == "provider":
        offering_data["size"] = config.ENV_DATA["size"]
        public_key = config.AUTH.get("managed_service", {}).get("public_key", "")
        if not public_key:
            raise ConfigurationError(
                "Public key for Managed Service not defined.\n"
                "Expected following configuration in auth.yaml file:\n"
                "managed_service:\n"
                '  private_key: "..."\n'
                '  public_key: "..."'
            )
        public_key_only = remove_header_footer_from_key(public_key)
        offering_data["onboarding_validation_key"] = public_key_only
        template_file = "managedfusionoffering.yaml.j2"

    elif config.ENV_DATA.get("cluster_type") == "consumer":
        if config.DEPLOYMENT.get("not_ga_wa"):
            # To use unreleased version of operators and DFC offering, create ImageContentSourcePolicy
            icsp_path = os.path.join(FUSION_TEMPLATE_DIR, "icsp.yaml")
            icsp_data = load_yaml(icsp_path)
            try:
                helpers.create_resource(**icsp_data)
            except CommandFailed as exc:
                # To unblock the deployment if the creation of ImageContentSourcePolicy fails.
                if "failed calling webhook" in str(exc):
                    logger.warning(
                        "Creation of ImageContentSourcePolicy failed due to the error given below. Create it manually."
                    )
                    logger.warning(str(exc))
                else:
                    raise

        onboarding_ticket = config.DEPLOYMENT.get("onboarding_ticket", "")
        if not onboarding_ticket:
            onboarding_ticket = generate_onboarding_token()
        offering_data["onboarding_ticket"] = onboarding_ticket.strip()
        provider_name = config.ENV_DATA.get("provider_name", "")
        offering_data["storage_provider_endpoint"] = get_storage_provider_endpoint(
            provider_name
        )
        template_file = "managedfusionoffering-dfc.yaml.j2"

    template = templating.render_template(template_file, offering_data)

    # TODO: Improve the creation of ManagedFusionOffering using exiting helper functions
    with tempfile.NamedTemporaryFile(
        mode="w+", prefix=constants.MANAGED_FUSION_OFFERING, delete=False
    ) as temp_file:
        temp_yaml = temp_file.name
        temp_file.write(template)

    # CRDs may have not be available yet
    offering_check_cmd = ["oc", "get", "crd", "managedfusionofferings.misf.ibm.com"]
    retry(CommandFailed, tries=6, delay=10,)(
        exec_cmd
    )(offering_check_cmd)

    # Create ManagedFusionOffering
    exec_cmd(cmd=f"oc create -f {temp_yaml}")

    operator_name = (
        defaults.OCS_OPERATOR_NAME
        if config.ENV_DATA.get("cluster_type") == "provider"
        else defaults.OCS_CLIENT_OPERATOR_NAME
    )

    # Sometimes it takes time before ocs operator csv is present
    for sample in TimeoutSampler(
        timeout=1800,
        sleep=15,
        func=get_csvs_start_with_prefix,
        csv_prefix=operator_name,
        namespace=ns_name,
    ):
        if sample:
            break
    # Wait for installation to be completed
    sample = TimeoutSampler(
        timeout=1200, sleep=15, func=check_all_csvs_are_succeeded, namespace=ns_name
    )
    sample.wait_for_func_value(value=True)


def remove_agent():
    """
    Remove agent and offering
    """
    if config.ENV_DATA.get("cluster_type") == "consumer":
        logger.error("Removal of agent on application cluster is not supported.")
        return
    logger.info(f"Deleting the secret {constants.FUSION_AGENT_CONFIG_SECRET}")
    managed_fusion_agent_config_secret = OCP(
        kind=constants.SECRET,
        namespace=config.ENV_DATA["service_namespace"],
        resource_name=constants.FUSION_AGENT_CONFIG_SECRET,
    )
    managed_fusion_agent_config_secret.delete(
        resource_name=constants.FUSION_AGENT_CONFIG_SECRET
    )
    managed_fusion_agent_config_secret.wait_for_delete(
        resource_name=constants.FUSION_AGENT_CONFIG_SECRET
    )
    project_obj = OCP(kind=constants.NAMESPACE)
    project_obj.wait_for_delete(resource_name=config.ENV_DATA["cluster_namespace"])
    project_obj.wait_for_delete(resource_name=config.ENV_DATA["service_namespace"])
