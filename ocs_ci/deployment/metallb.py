import ipaddress
import json
import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants
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
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd, get_ocp_version

logger = logging.getLogger(__name__)


class MetalLBInstaller:
    def __init__(
        self,
        version: str = None,
        namespace: str = "metallb-system",
    ):
        self.addresses_reserved = None
        if not version:
            self.version_lb = get_ocp_version()
        else:
            self.version_lb = version
        self.namespace_lb = namespace
        self.l2Advertisement_name = None
        self.ip_address_pool_name = None
        self.subscription_name = None
        self.operatorgroup_name = None
        self.catalog_source_name = None
        self.hostnames = []
        self.timeout_check_resources_existence = 6

    def create_metallb_namespace(self):
        """
        Create MetalLB namespace
        :return: True if namespace is created, False otherwise
        """
        logger.info(f"Creating namespace {self.namespace_lb} for MetalLB")

        ocp = OCP(kind="namespace", resource_name=self.namespace_lb)
        if ocp.check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            resource_name=self.namespace_lb,
            should_exist=True,
        ):
            logger.info(f"Namespace {self.namespace_lb} already exists")
            return

        exec_cmd(f"oc create namespace {self.namespace_lb}")

        return ocp.check_resource_existence(
            resource_name=self.namespace_lb,
            timeout=120,
            should_exist=True,
        )

    def catalog_source_created(self):
        """
        Check if catalog source is created
        :return: True if catalog source is created, False otherwise
        """
        return CatalogSource(
            resource_name=self.catalog_source_name,
            namespace=MARKETPLACE_NAMESPACE,
        ).check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            should_exist=True,
            resource_name=self.catalog_source_name,
        )

    def create_catalog_source(self):
        """
        Create catalog source for MetalLB
        :return: True if catalog source is created, False otherwise, error if not get Ready state
        """
        logger.info("Creating catalog source for MetalLB")
        # replace latest version with specific version
        catalog_source_data = templating.load_yaml(METALLB_CATALOG_SOURCE_YAML)

        image_placeholder = catalog_source_data.get("spec").get("image")
        catalog_source_data.get("spec").update(
            {"image": image_placeholder.format("4.14")}
        )

        self.catalog_source_name = catalog_source_data.get("metadata").get("name")

        if self.catalog_source_created():
            logger.info(f"Catalog Source {self.catalog_source_name} already exists")
            return

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
            resource_name=self.catalog_source_name,
            namespace=MARKETPLACE_NAMESPACE,
        )

        return metallb_catalog_source.wait_for_state("READY")

    def metallb_operator_group_created(self):
        """
        Check if MetalLB operator group is created
        :return: True if operator group is created, False otherwise
        """
        if not self.operatorgroup_name:
            return False

        return OCP(
            kind=constants.OPERATOR_GROUP,
            namespace=self.namespace_lb,
            resource_name=self.operatorgroup_name,
        ).check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            should_exist=True,
            resource_name=self.operatorgroup_name,
        )

    def create_metallb_operator_group(self):
        """
        Create MetalLB operator group
        :return: True if operator group is created, False otherwise
        """
        logger.info("Creating MetalLB operator group")
        operator_group_data = templating.load_yaml(METALLB_OPERATOR_GROUP_YAML)

        self.operatorgroup_name = operator_group_data.get("metadata").get("name")

        # check if OperatorGroup already exists
        if self.metallb_operator_group_created():
            logger.info(f"OperatorGroup {self.operatorgroup_name} already exists")
            return

        # update namespace and target namespace
        if self.namespace_lb != METALLB_DEFAULT_NAMESPACE:
            operator_group_data.get("metadata").update({"namespace": self.namespace_lb})

        metallb_operatorgroup_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="metallb_operatorgroup", delete=False
        )
        templating.dump_data_to_temp_yaml(
            operator_group_data, metallb_operatorgroup_file.name
        )

        exec_cmd(f"oc apply -f {metallb_operatorgroup_file.name}", timeout=2400)

        return self.metallb_operator_group_created()

    def subscription_created(self):
        """
        Check if subscription already exists
        :return: bool True if subscription already exists, False otherwise
        """
        return OCP(
            kind=constants.SUBSCRIPTION,
            namespace=self.namespace_lb,
            resource_name=self.subscription_name,
        ).check_resource_existence(
            should_exist=True, resource_name=self.subscription_name
        )

    def create_metallb_subscription(self):
        """
        Create MetalLB subscription
        :return: True if subscription is created, and metallb pods are Ready, False otherwise
        """
        logger.info("Creating MetalLB subscription")
        subscription_data = templating.load_yaml(METALLB_SUBSCRIPTION_YAML)
        if self.namespace_lb != METALLB_DEFAULT_NAMESPACE:
            subscription_data.get("metadata").update({"namespace": self.namespace_lb})

        self.subscription_name = subscription_data.get("metadata").get("name")

        if self.subscription_created():
            logger.info(f"Subscription {self.subscription_name} already exists")
            return

        metallb_subscription_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="metallb_subscription", delete=False
        )
        templating.dump_data_to_temp_yaml(
            subscription_data, metallb_subscription_file.name
        )

        exec_cmd(f"oc apply -f {metallb_subscription_file.name}", timeout=2400)

        metallb_pods = get_pod_name_by_pattern(
            METALLB_CONTROLLER_MANAGER_PREFIX, self.namespace_lb
        )
        metallb_pods.extend(
            get_pod_name_by_pattern(METALLB_WEBHOOK_PREFIX, self.namespace_lb)
        )

        return self.subscription_created() and wait_for_pods_to_be_running(
            namespace=self.namespace_lb, pod_names=metallb_pods, timeout=300
        )

    def create_ip_address_pool(self):
        """
        Create IP address pool for MetalLB
        :return: True if IP address pool is created, False if creation failed and None if IP address pool already exists
        """
        reserved_ips_num = config.ENV_DATA.get("ips_to_reserve")
        if not reserved_ips_num:
            raise ValueError(
                "Number of reserved IP addresses for MetalLB is not specified"
            )
        else:
            for num in range(1, reserved_ips_num + 1):
                self.hostnames.append(
                    f"clustername-{config.ENV_DATA['cluster_name']}-{num}"
                )

        # common part for both platforms
        ipaddresspool_data = templating.load_yaml(METALLB_IPADDRESSPOOL_PATH)
        if self.namespace_lb != METALLB_DEFAULT_NAMESPACE:
            ipaddresspool_data.get("metadata").update({"namespace": self.namespace_lb})

        self.ip_address_pool_name = ipaddresspool_data.get("metadata").get("name")

        if self.ip_address_pool_created():
            logger.info(
                f"IPAddressPool {self.ip_address_pool_name} already exists in the namespace {self.namespace_lb}"
            )
            return

        if config.ENV_DATA["platform"] == constants.HCI_VSPHERE:

            # due to circular import error, import is here
            from ocs_ci.deployment.vmware import assign_ips

            logger.info("Reserving IP addresses from IPAM and Creating IP address pool")
            self.addresses_reserved = assign_ips(hosts=self.hostnames)

            ip_addresses_with_mask = [ip + "/32" for ip in self.addresses_reserved]
            ipaddresspool_data.get("spec").update({"addresses": ip_addresses_with_mask})

        elif config.ENV_DATA["platform"] == constants.HCI_BAREMETAL:
            cidr = config.ENV_DATA["machine_cidr"]
            network = ipaddress.ip_network(cidr)
            ip_list_by_cidr = list(network)
            ip_list_for_hosted_clusters = list()

            # remove ip addresses reserved for machines, Network, Gateway, and Broadcast
            for i, ip in enumerate(ip_list_by_cidr):
                if i < 10 or i == len(ip_list_by_cidr) - 1:
                    continue
                ip_list_for_hosted_clusters.append(f"{ip}/{network.prefixlen}")
            ipaddresspool_data.get("spec").update(
                {"addresses": ip_list_for_hosted_clusters}
            )
        else:
            raise NotImplementedError(
                f"Platform {config.ENV_DATA['platform']} is not supported yet"
            )

        # create IP address pool file
        ipaddresspool_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ipaddresspool_file", delete=False
        )
        templating.dump_data_to_temp_yaml(ipaddresspool_data, ipaddresspool_file.name)

        exec_cmd(
            f"oc apply -f {ipaddresspool_file.name}",
            timeout=240,
        )

        return self.ip_address_pool_created()

    def ip_address_pool_created(self):
        """
        Check if IP address pool is created
        :return: True if IP address pool is created, False otherwise
        """
        return OCP(
            kind=constants.IP_ADDRESS_POOL,
            namespace=self.namespace_lb,
            resource_name=self.ip_address_pool_name,
        ).check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            should_exist=True,
            resource_name=self.ip_address_pool_name,
        )

    def update_ip_address_pool_cr(self, ipaddresspool_data):
        """
        Update IP address pool custom resource
        :param ipaddresspool_data: IP address pool data. YAML accessible as dict

        """
        ocp = OCP(
            kind=constants.IP_ADDRESS_POOL,
            namespace=self.namespace_lb,
            resource_name=self.ip_address_pool_name,
        )
        if self.ip_address_pool_created():
            logger.info(
                f"IPAddressPool {self.ip_address_pool_name} already exists, adding old IPs to new IPAddressPool"
            )
            ip_addresses_with_mask = ocp.exec_oc_cmd(
                "get ipaddresspool metallb -o=jsonpath='{.spec.addresses}'"
            )

            addresses_list = json.loads(str(ip_addresses_with_mask))
            ip_addresses_with_mask = addresses_list.extend(ip_addresses_with_mask)
            ipaddresspool_data.get("spec").update({"addresses": ip_addresses_with_mask})

    def l2advertisement_created(self):
        """
        Check if L2 advertisement is created
        :return: True if L2 advertisement is created, False otherwise
        """
        return OCP(
            kind=constants.L2_ADVERTISEMENT,
            namespace=self.namespace_lb,
            resource_name=self.l2Advertisement_name,
        ).check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            should_exist=True,
            resource_name=self.l2Advertisement_name,
        )

    def create_l2advertisement(self):
        """
        Create L2 advertisement for IP address pool
        :return: True if L2 advertisement is created, False if failed, None if L2 advertisement already exists
        """

        logger.info("Creating L2 advertisement for IP address pool")
        # METALLB_L2_ADVERTISEMENT_PATH
        l2_advertisement_data = templating.load_yaml(METALLB_L2_ADVERTISEMENT_PATH)
        if self.namespace_lb != METALLB_DEFAULT_NAMESPACE:
            l2_advertisement_data.get("metadata").update(
                {"namespace": self.namespace_lb}
            )

        self.l2Advertisement_name = l2_advertisement_data.get("metadata").get("name")

        if self.l2advertisement_created():
            logger.info(
                f"L2 advertisement {self.l2Advertisement_name} already exists in the namespace {self.namespace_lb}"
            )
            return

        l2_advertisement_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="l2_advertisement_file", delete=False
        )

        templating.dump_data_to_temp_yaml(
            l2_advertisement_data, l2_advertisement_file.name
        )

        exec_cmd(f"oc apply -f {l2_advertisement_file.name}", timeout=2400)

        return self.l2advertisement_created()

    def deploy_lb(self):
        """
        Deploy MetalLB
        If resources are already created, method will not create them again

        """

        if not config.DEPLOYMENT.get("metallb_operator"):
            logger.info("MetalLB operator deployment is not requested")
            return

        logger.info(
            f"Deploying MetalLB and dependant resources to namespace: '{self.namespace_lb}'"
        )
        if self.create_metallb_namespace():
            logger.info(f"Namespace {self.namespace_lb} created successfully")
        if self.create_catalog_source():
            logger.info("MetalLB catalog source created successfully")
        if self.create_metallb_operator_group():
            logger.info("MetalLB operator group created successfully")
        if self.create_metallb_subscription():
            logger.info("MetalLB subscription created successfully")
        if self.create_ip_address_pool():
            logger.info("IP address pool created successfully")
        if self.create_l2advertisement():
            logger.info("L2 advertisement created successfully")

    def undeploy(self):
        """
        Undeploy MetalLB
        """

        self.delete_l2advertisement()

        # due to circular import error, import is here
        from ocs_ci.deployment.vmware import release_ips

        release_ips(hosts=self.hostnames)

        self.delete_ipaddresspool()

        self.delete_subscription()

        self.delete_operatorgroup()

        self.delete_catalogsource()

        self.delete_metallb_namespace()

    def delete_l2advertisement(self):
        """
        Delete l2advertisement
        :return: True if l2advertisement is deleted, False otherwise
        """
        ocp = OCP(
            kind=constants.L2_ADVERTISEMENT,
            namespace=self.namespace_lb,
            resource_name=self.l2Advertisement_name,
        )
        ocp.delete(resource_name=self.l2Advertisement_name)
        return ocp.check_resource_existence(
            resource_name=self.l2Advertisement_name,
            timeout=120,
            should_exist=False,
        )

    def delete_operatorgroup(self):
        """
        Delete operator group
        :returns True if operator group is deleted, False otherwise
        """
        ocp = OCP(
            kind=constants.OPERATOR_GROUP,
            namespace=self.namespace_lb,
            resource_name=self.operatorgroup_name,
        )
        ocp.delete(resource_name=self.operatorgroup_name)
        return ocp.check_resource_existence(
            resource_name=self.operatorgroup_name,
            timeout=120,
            should_exist=False,
        )

    def delete_subscription(self):
        """
        Delete subscription
        :returns True if subscription is deleted, False otherwise
        """
        ocp = OCP(
            kind=constants.SUBSCRIPTION,
            namespace=self.namespace_lb,
            resource_name=self.subscription_name,
        )
        ocp.delete(resource_name=self.subscription_name)
        return ocp.check_resource_existence(
            resource_name=self.subscription_name,
            timeout=120,
            should_exist=False,
        )

    def delete_ipaddresspool(self):
        """
        Delete ipaddresspool
        :returns True if ipaddresspool is deleted, False otherwise
        """
        ocp = OCP(
            kind=constants.IP_ADDRESS_POOL,
            namespace=self.namespace_lb,
            resource_name=self.ip_address_pool_name,
        )
        ocp.delete(resource_name=self.ip_address_pool_name)
        return ocp.check_resource_existence(
            resource_name=self.ip_address_pool_name,
            timeout=120,
            should_exist=False,
        )

    def delete_catalogsource(self):
        """
        Delete catalog source
        :return: True if catalog source is deleted, False otherwise
        """
        ocp = OCP(
            kind=constants.CATSRC,
            namespace=MARKETPLACE_NAMESPACE,
            resource_name=self.catalog_source_name,
        )
        ocp.delete(resource_name=self.catalog_source_name)
        return ocp.check_resource_existence(
            resource_name=self.catalog_source_name,
            timeout=120,
            should_exist=False,
        )

    def delete_metallb_namespace(self):
        """
        Delete MetalLB namespace
        :return: True if namespace is deleted, False otherwise
        """
        ocp = OCP(kind="namespace", resource_name=self.namespace_lb)
        ocp.delete(resource_name=self.namespace_lb)
        return ocp.check_resource_existence(
            resource_name=self.namespace_lb,
            timeout=120,
            should_exist=False,
        )
