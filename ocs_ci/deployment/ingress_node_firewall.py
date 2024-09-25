import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    get_ocp_version,
    TimeoutSampler,
    wait_for_machineconfigpool_status,
)


logger = logging.getLogger(__name__)


def restrict_ssh_access_to_nodes():
    """
    Deploy IngressNodeFirewall and configure rules to restrict SSH access to nodes
    """
    logger.info(
        "Deploy and configure IngressNodeFirewall to restrict SSH access to nodes"
    )
    if config.ENV_DATA.get("allow_ssh_access_from_subnets"):
        logger.debug(
            "SSH access to nodes will be restricted except for clients from following subnets: "
            f"{config.ENV_DATA['allow_ssh_access_from_subnets']}"
        )
        rules = [
            {
                "sourceCIDRs": config.ENV_DATA["allow_ssh_access_from_subnets"],
                "rules": [
                    {
                        "order": 10,
                        "protocolConfig": {
                            "protocol": "TCP",
                            "tcp": {
                                "ports": "22",
                            },
                        },
                        "action": "Allow",
                    },
                ],
            },
        ]
    else:
        logger.warning(
            "SSH access to nodes will be restricted and no exceptions will be configured "
            "(ENV_DATA['allow_ssh_access_from_subnets'] is not configured)."
        )
        rules = []

    rules.append(
        {
            "sourceCIDRs": [
                "0.0.0.0/0",
                "::/0",
            ],
            "rules": [
                {
                    "order": 90,
                    "protocolConfig": {
                        "protocol": "TCP",
                        "tcp": {
                            "ports": "22",
                        },
                    },
                    "action": "Deny",
                },
            ],
        },
    )
    deploy_ingress_node_firewall(rules=rules)


def deploy_ingress_node_firewall(rules):
    """
    Deploy Ingress Node Firewall Operator used for example for restricting SSH access to nodes

    Args:
        rules (dict): dictionary of IngressNodeFirewall Rules (content of `spec.ingress`)

    """
    inf = IngressNodeFirewallInstaller()

    # check if Ingress Node Firewall Operator is available
    if not inf.check_existing_packagemanifests():
        # Ingress Node Firewall Operator is not available, we have to create QE App Registry Catalog Source
        # and related Image content source policy
        inf.create_brew_icsp()
        inf.create_catalog_source()
    # create openshift-ingress-node-firewall namespace
    inf.create_namespace()

    # create operator group
    inf.create_operatorgroup()

    # subscribe to the Ingress Node Firewall Operator
    inf.create_subscription()

    # verify installation
    inf.verify_csv_status()

    # create config
    inf.create_config()

    # add firewall rules
    inf.create_rules(rules=rules)


class IngressNodeFirewallInstaller(object):
    """
    IngressNodeFirewall Installer class for Ingress Node Firewall deployment

    """

    def __init__(self):
        self.namespace = constants.INGRESS_NODE_FIREWALL_NAMESPACE
        self.source = constants.OPERATOR_CATALOG_SOURCE_NAME

    def icsp_brew_registry_exists(self):
        """
        Check if the ICSP Brew registry exists

        Returns:
            bool: True if the ICSP Brew registry exists, False otherwise
        """
        return OCP(
            kind=constants.IMAGECONTENTSOURCEPOLICY_KIND, resource_name="brew-registry"
        ).check_resource_existence(timeout=10, should_exist=True)

    def create_brew_icsp(self):
        """
        Apply the ICSP to the cluster
        """
        if self.icsp_brew_registry_exists():
            logger.info("ICSP for Brew registry already exists")
            return
        icsp_data = templating.load_yaml(constants.SUBMARINER_DOWNSTREAM_BREW_ICSP)
        icsp = OCS(**icsp_data)
        icsp.create()
        wait_for_machineconfigpool_status(node_type="all")
        logger.info("ICSP applied successfully")

    def check_existing_packagemanifests(self):
        """
        Check if Ingress Node Firewall Operator is available or not.

        Returns:
            bool: True if Ingress Node Operator is available, False otherwise
        """
        try:
            pm = PackageManifest(constants.INGRESS_NODE_FIREWALL_OPERATOR_NAME)
            pm.get(silent=True)
            return True
        except (exceptions.CommandFailed, exceptions.ResourceNotFoundError):
            return False

    def create_catalog_source(self):
        """
        Create Catalog source from QE App registry

        """
        logger.info("Creating Catalog Source for IngressNodeFirewall")
        catalog_source_data = templating.load_yaml(constants.QE_APP_REGISTRY_SOURCE)

        image_placeholder = catalog_source_data.get("spec").get("image")
        catalog_source_data.get("spec").update(
            {"image": image_placeholder.format(get_ocp_version())}
        )
        catalog_source = OCS(**catalog_source_data)
        catalog_source.create()

        catalog_source = CatalogSource(
            resource_name=constants.QE_APP_REGISTRY_CATALOG_SOURCE_NAME,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )

        # wait for catalog source is ready
        catalog_source.wait_for_state("READY")
        logger.info("Catalog Source created successfully")
        self.source = constants.QE_APP_REGISTRY_CATALOG_SOURCE_NAME

    def create_namespace(self):
        """
        Creates the namespace for IngressNodeFirewall resources

        Raises:
            CommandFailed: If the 'oc create' command fails.

        """
        try:
            logger.info(
                f"Creating namespace {self.namespace} for IngressNodeFirewall resources"
            )
            namespace_yaml_file = templating.load_yaml(constants.INF_NAMESPACE_YAML)
            namespace_yaml = OCS(**namespace_yaml_file)
            namespace_yaml.create()
            logger.info(
                f"IngressNodeFirewall namespace {self.namespace} was created successfully"
            )
        except exceptions.CommandFailed as err:
            if (
                f'project.project.openshift.io "{self.namespace}" already exists'
                in str(err)
            ):
                logger.info(f"Namespace {self.namespace} already exists")
            else:
                raise err

    def create_operatorgroup(self):
        """
        Creates an OperatorGroup for IngressNodeFirewall

        """
        logger.info("Creating OperatorGroup for IngressNodeFirewall")
        operatorgroup_yaml_file = templating.load_yaml(constants.INF_OPERATORGROUP_YAML)
        operatorgroup_yaml = OCS(**operatorgroup_yaml_file)
        operatorgroup_yaml.create()
        logger.info("IngressNodeFirewall OperatorGroup created successfully")

    def create_subscription(self):
        """
        Creates subscription for IngressNodeFirewall operator

        """
        logger.info("Creating Subscription for IngressNodeFirewall")
        subscription_yaml_file = templating.load_yaml(constants.INF_SUBSCRIPTION_YAML)
        subscription_yaml_file["spec"]["source"] = self.source
        subscription_yaml = OCS(**subscription_yaml_file)
        subscription_yaml.create()
        logger.info("IngressNodeFirewall Subscription created successfully")

    def verify_csv_status(self):
        """
        Verify the CSV status for the IngressNodeFirewall Operator deployment equals Succeeded

        """
        for csv in TimeoutSampler(
            timeout=900,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=constants.INGRESS_NODE_FIREWALL_CSV_NAME,
            namespace=self.namespace,
        ):
            if csv:
                break
        csv_name = csv[0]["metadata"]["name"]
        csv_obj = CSV(resource_name=csv_name, namespace=self.namespace)
        csv_obj.wait_for_phase(phase="Succeeded", timeout=720)

    def create_config(self):
        """
        Creates configuration for IngressNodeFirewall

        """
        logger.info("Creating IngressNodeFirewallConfig")
        config_yaml_file = templating.load_yaml(constants.INF_CONFIG_YAML)
        config_yaml = OCS(**config_yaml_file)
        config_yaml.create()
        logger.info("IngressNodeFirewallConfig created successfully")

    def create_rules(self, rules):
        """
        Create IngressNodeFirewall Rules

        Args:
            rules (dict): dictionary of IngressNodeFirewall Rules (content of `spec.ingress`)

        """
        logger.info("Creating IngressNodeFirewall Rules")
        rules_yaml_file = templating.load_yaml(constants.INF_RULES_YAML)
        rules_yaml_file["spec"]["ingress"] = rules
        rules_yaml = OCS(**rules_yaml_file)
        rules_yaml.create()
        logger.info("IngressNodeFirewall Rules created successfully")
