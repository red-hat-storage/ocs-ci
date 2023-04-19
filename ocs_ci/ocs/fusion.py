import os
import logging
import yaml

from ocs_ci.ocs.exceptions import ConfigurationError
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.utility.managedservice import remove_header_footer_from_key
from ocs_ci.utility.retry import retry
from ocs_ci.utility.templating import Templating, load_yaml
from ocs_ci.utility.utils import TimeoutSampler, get_ocp_version, exec_cmd
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded

logger = logging.getLogger(name=__file__)

FUSION_TEMPLATE_DIR = os.path.join(constants.TEMPLATE_DIR, "fusion-aas")


def create_fusion_monitoring_resources():
    """
    Create resources used for Managed Fusion aaS Monitoring
    """
    templating = Templating(base_path=FUSION_TEMPLATE_DIR)
    ns_name = constants.MANAGED_FUSION_NAMESPACE
    logger.info(f"Creating {ns_name} namespace")
    exec_cmd(["oc", "new-project", ns_name])
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
    ns_name = constants.OPENSHIFT_STORAGE_NAMESPACE
    logger.info(f"Creating {ns_name} namespace")
    exec_cmd(["oc", "create", "ns", ns_name])
    logger.info("Creating the offering CR")
    offering_data = dict()
    offering_data["ocp_version"] = get_ocp_version()
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
    template = templating.render_template(
        "managedfusionoffering.yaml.j2",
        offering_data,
    )
    template = yaml.load(template, Loader=yaml.Loader)
    # CRDs may have not be available yet
    offering_check_cmd = ["oc", "get", "crd", "managedfusionofferings.misf.ibm.com"]
    retry(CommandFailed, tries=6, delay=10,)(
        exec_cmd
    )(offering_check_cmd)
    helpers.create_resource(**template)
    # Wait for installation to be completed
    sample = TimeoutSampler(
        timeout=1200, sleep=15, func=check_all_csvs_are_succeeded, namespace=ns_name
    )
    sample.wait_for_func_value(value=True)
