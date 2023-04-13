import os
import logging
import yaml

from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.utility.templating import load_yaml, Templating
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs import constants

logger = logging.getLogger(name=__file__)

FUSION_TEMPLATE_DIR = os.path.join(constants.TEMPLATE_DIR, "fusion-aas")


def create_fusion_monitoring_resources():
    """
    Create resources used for Managed Fusion aaS Monitoring
    """
    templating = Templating(base_path=FUSION_TEMPLATE_DIR)
    project_name = "managed-fusion"
    logger.info(f"Creating {project_name} project")
    helpers.create_project(project_name=project_name)
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
    project_name = "openshift-storage"
    logger.info(f"Creating {project_name} project")
    helpers.create_project(project_name=project_name)
    logger.info("Creating the offering CRD")
    offering_data = dict()
    offering_data["ocp_version"] = get_ocp_version()
    offering_data["size"] = config.ENV_DATA["size"]
    offering_data["onboarding_validation_key"] = config.AUTH["managed_service"][
        "public_key"
    ]
    template = templating.render_template(
        "managedfusionoffering.yaml.j2",
        offering_data,
    )
    template = yaml.load(template, Loader=yaml.Loader)
    helpers.create_resource(**template)
