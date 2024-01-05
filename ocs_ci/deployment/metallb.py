import logging
import tempfile

from ocs_ci.deployment.vmware import assign_ips
from ocs_ci.ocs.constants import (
    METALLB_CATALOG_SOURCE_YAML,
    MARKETPLACE_NAMESPACE,
    METALLB_DEFAULT_NAMESPACE,
    METALLB_OPERATOR_GROUP_YAML,
    METALLB_SUBSCRIPTION_YAML,
    METALLB_CONTROLLER_MANAGER_PREFIX,
    METALLB_WEBHOOK_PREFIX,
    METALLB_IPADDRESSPOOL_PATH,
    METALLB_L2_ADVERTISEMENT_PATH,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd


logger = logging.getLogger(__name__)

# TODO: adjust func for ipaddresspool
# TODO: check created l2advertisement
# TODO: release ipaddresses
# TODO: undeploy Metal LB instance


class MetalLBInstaller:
    def __init__(self, version: str, ip_list: list, namespace: str = "metallb-system"):
        self.addresses_reserved = None
        self.version = version
        self.ip_range = ip_list
        self.namespace = namespace

    def create_metallb_namespace(self):
        logger.info(f"Creating namespace {self.namespace} for MetalLB")
        exec_cmd(f"oc create namespace {self.namespace}")

    def create_catalog_source(self):
        logger.info("Creating catalog source for MetalLB")
        # replace latest version with specific version
        catalog_source_data = templating.load_yaml(METALLB_CATALOG_SOURCE_YAML)
        image = catalog_source_data.get("spec").get("image")
        image = image.replace("latest", f"v{self.version}")
        catalog_source_data.get("spec").update({"image": image})
        catalog_source_name = catalog_source_data.get("metadata").get("name")

        # install catalog source
        metallb_catalog_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="metallb_catalogsource", delete=False
        )
        templating.dump_data_to_temp_yaml(
            catalog_source_data, metallb_catalog_file.name
        )
        exec_cmd(f"oc apply -f {metallb_catalog_file.name}", timeout=2400)

        # wait for catalog source is ready
        metallb_catalog_source = CatalogSource(
            resource_name=catalog_source_name,
            namespace=MARKETPLACE_NAMESPACE,
        )
        metallb_catalog_source.wait_for_state("READY")

    def create_metallb_operator_group(self):
        """
        Create MetalLB operator group
        """
        logger.info("Creating MetalLB operator group")
        operator_group_data = templating.load_yaml(METALLB_OPERATOR_GROUP_YAML)

        # update namespace and target namespace
        if self.namespace != METALLB_DEFAULT_NAMESPACE:
            operator_group_data.get("metadata").update({"namespace": self.namespace})
            operator_group_data.get("spec").get("targetNamespaces").append(
                self.namespace
            )

        metallb_operatorgroup_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="metallb_operatorgroup", delete=False
        )
        templating.dump_data_to_temp_yaml(
            operator_group_data, metallb_operatorgroup_file.name
        )

        try:
            exec_cmd(f"oc apply -f {metallb_operatorgroup_file.name}", timeout=2400)
            logger.info("MetalLB OperatorGroup created successfully")
        except CommandFailed as ef:
            if "already exists" in str(ef):
                logger.info("MetalLB OperatorGroup already exists")

    def create_metallb_subscription(self):
        """
        Create MetalLB subscription
        """
        logger.info("Creating MetalLB subscription")
        subscription_data = templating.load_yaml(METALLB_SUBSCRIPTION_YAML)
        if self.namespace != METALLB_DEFAULT_NAMESPACE:
            subscription_data.get("metadata").update({"namespace": self.namespace})

        metallb_subscription_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="metallb_subscription", delete=False
        )
        templating.dump_data_to_temp_yaml(
            subscription_data, metallb_subscription_file.name
        )

        try:
            exec_cmd(f"oc apply -f {metallb_subscription_file.name}", timeout=2400)
            logger.info("MetalLB Subscription created successfully")
        except CommandFailed as ef:
            if "already exists" in str(ef):
                logger.info("MetalLB Subscription already exists")

        metallb_pods = get_pod_name_by_pattern(METALLB_CONTROLLER_MANAGER_PREFIX)
        metallb_pods.extend(get_pod_name_by_pattern(METALLB_WEBHOOK_PREFIX))

        wait_for_pods_to_be_running(
            namespace=self.namespace, pod_names=metallb_pods, timeout=300
        )

    def create_ip_address_pool(self, num_of_ips: int = 2):
        """
        Create IP address pool for MetalLB
        """
        assert (
            num_of_ips >= 2
        ), "Minimum 2 IP addresses are required for Provider/Client mode"

        logger.info("Reserving IP addresses from IPAM and Creating IP address pool")
        # Reserve IP addresses for cluster assuming we need minimum 2 for each Hosted cluster
        self.addresses_reserved = assign_ips(num_of_ips)
        # TODO - if IP addresses are not in format address/subnet mask - convert them
        logger.info(f"Reserved IP addresses are {self.addresses_reserved}")

        ipaddresspool_data = templating.load_yaml(METALLB_IPADDRESSPOOL_PATH)
        if self.namespace != METALLB_DEFAULT_NAMESPACE:
            ipaddresspool_data.get("metadata").update({"namespace": self.namespace})

        ipaddresspool_data.get("spec").update({"addresses": self.addresses_reserved})

        ipaddresspool_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ipaddresspool_file", delete=False
        )
        templating.dump_data_to_temp_yaml(ipaddresspool_data, ipaddresspool_file.name)

        exec_cmd(f"oc apply -f {ipaddresspool_file.name}", timeout=2400)
        logger.info("IP address pool created successfully")

    def create_l2advertisement(self):
        """
        Create L2 advertisement for IP address pool
        """
        logger.info("Creating L2 advertisement for IP address pool")
        # METALLB_L2_ADVERTISEMENT_PATH
        l2_advertisement_data = templating.load_yaml(METALLB_L2_ADVERTISEMENT_PATH)
        if self.namespace != METALLB_DEFAULT_NAMESPACE:
            l2_advertisement_data.get("metadata").update({"namespace": self.namespace})

        l2_advertisement_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="l2_advertisement_file", delete=False
        )

        templating.dump_data_to_temp_yaml(
            l2_advertisement_data, l2_advertisement_file.name
        )

        exec_cmd(f"oc apply -f {l2_advertisement_file.name}", timeout=2400)
        logger.info("L2 advertisement created")

    def deploy(self):
        """
        Deploy MetalLB
        """
        pass

    def undeploy(self):
        """
        Undeploy MetalLB
        """
        # Delete MetalLB
        # Delete namespace
        pass
