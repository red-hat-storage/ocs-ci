import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating
from ocs_ci.utility.operators import IngressNodeFirewallOperator


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
    # Create and deploy the Ingress Node Firewall Operator
    inf_operator = IngressNodeFirewallOperator(create_catalog=True)
    inf_operator.deploy()

    # Create firewall configuration and rules
    create_config()
    create_rules(rules=rules)


def create_config():
    """
    Creates configuration for IngressNodeFirewall

    """
    logger.info("Creating IngressNodeFirewallConfig")
    config_yaml_file = templating.load_yaml(constants.INF_CONFIG_YAML)
    config_yaml = OCS(**config_yaml_file)
    config_yaml.create()
    logger.info("IngressNodeFirewallConfig created successfully")


def create_rules(rules):
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
