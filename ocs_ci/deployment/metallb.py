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
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
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
    def __init__(
        self,
        version: str = None,
        namespace: str = "metallb-system",
    ):
        self.addresses_reserved = None
        if not version:
            self.version = config.ENV_DATA.get("metallb_version")
        else:
            self.version = version
        self.namespace = namespace
        self.l2Advertisement_name = None
        self.ip_address_pool_name = None
        self.subscription_name = None
        self.operatorgroup_name = None
        self.catalog_source_name = None
        self.hostnames = []

    def create_metallb_namespace(self):
        """
        Create MetalLB namespace
        :return: True if namespace is created, False otherwise
        """
        logger.info(f"Creating namespace {self.namespace} for MetalLB")

        ocp = OCP(kind="namespace", resource_name=self.namespace)
        try:
            exec_cmd(f"oc create namespace {self.namespace}")
        except CommandFailed as ef:
            if "already exists" in str(ef):
                logger.info(f"Namespace {self.namespace} already exists")

        return ocp.check_resource_existence(
            resource_name=self.namespace,
            timeout=120,
            should_exist=True,
        )

    def create_catalog_source(self):
        """
        Create catalog source for MetalLB
        :return: True if catalog source is created, False otherwise
        """
        logger.info("Creating catalog source for MetalLB")
        # replace latest version with specific version
        catalog_source_data = templating.load_yaml(METALLB_CATALOG_SOURCE_YAML)
        image = catalog_source_data.get("spec").get("image")
        image = image.replace("latest", f"v{self.version}")
        catalog_source_data.get("spec").update({"image": image})
        self.catalog_source_name = catalog_source_data.get("metadata").get("name")

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
        metallb_catalog_source.wait_for_state("READY")
        return metallb_catalog_source.check_resource_existence(
            should_exist=True,
            resource_name=self.catalog_source_name,
        )

    def create_metallb_operator_group(self):
        """
        Create MetalLB operator group
        :return: True if operator group is created, False otherwise
        """
        logger.info("Creating MetalLB operator group")
        operator_group_data = templating.load_yaml(METALLB_OPERATOR_GROUP_YAML)

        # update namespace and target namespace
        if self.namespace != METALLB_DEFAULT_NAMESPACE:
            operator_group_data.get("metadata").update({"namespace": self.namespace})
            operator_group_data.get("spec").get("targetNamespaces").append(
                self.namespace
            )
        self.operatorgroup_name = operator_group_data.get("metadata").get("name")

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
        return OCP(
            kind="OperatorGroup",
            namespace=self.namespace,
            resource_name=self.operatorgroup_name,
        ).check_resource_existence(
            should_exist=True,
            resource_name=self.operatorgroup_name,
        )

    def create_metallb_subscription(self):
        """
        Create MetalLB subscription
        :return: True if subscription is created, and metallb pods are Ready, False otherwise
        """
        logger.info("Creating MetalLB subscription")
        subscription_data = templating.load_yaml(METALLB_SUBSCRIPTION_YAML)
        if self.namespace != METALLB_DEFAULT_NAMESPACE:
            subscription_data.get("metadata").update({"namespace": self.namespace})

        self.subscription_name = subscription_data.get("metadata").get("name")
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

        metallb_pods = get_pod_name_by_pattern(
            METALLB_CONTROLLER_MANAGER_PREFIX, self.namespace
        )
        metallb_pods.extend(
            get_pod_name_by_pattern(METALLB_WEBHOOK_PREFIX, self.namespace)
        )

        subscription_created = OCP(
            kind="subscription",
            namespace=self.namespace,
            resource_name=self.subscription_name,
        ).check_resource_existence(
            should_exist=True,
            resource_name=self.subscription_name,
        )
        return subscription_created and wait_for_pods_to_be_running(
            namespace=self.namespace, pod_names=metallb_pods, timeout=300
        )

    def create_ip_address_pool(self):
        """
        Create IP address pool for MetalLB
        :return: True if IP address pool is created, False otherwise
        """
        reserved_ips_num = config.ENV_DATA.get("reserved_ips_num")
        if not reserved_ips_num:
            raise ValueError(
                "Number of reserved IP addresses for MetalLB is not specified"
            )
        else:
            for num in range(1, reserved_ips_num + 1):
                self.hostnames.append(
                    f"clustername-{config.ENV_DATA['cluster_name']}-{num}"
                )

        logger.info("Reserving IP addresses from IPAM and Creating IP address pool")
        # Reserve IP addresses for cluster assuming we need minimum 2 for each Hosted cluster

        if config.ENV_DATA["platform"] == constants.VSPHERE_PLATFORM:

            # TODO - if IP addresses are not in format address/subnet mask - convert them
            # due to circular import error, import is here
            from ocs_ci.deployment.vmware import assign_ips

            self.addresses_reserved = assign_ips(hosts=self.hostnames)
            logger.info(f"Reserved IP addresses are {self.addresses_reserved}")

        else:
            raise NotImplementedError(
                f"Platform {config.ENV_DATA['platform']} is not supported yet"
            )

        ipaddresspool_data = templating.load_yaml(METALLB_IPADDRESSPOOL_PATH)
        if self.namespace != METALLB_DEFAULT_NAMESPACE:
            ipaddresspool_data.get("metadata").update({"namespace": self.namespace})

        ip_addresses_with_mask = [ip + "/32" for ip in self.addresses_reserved]
        ipaddresspool_data.get("spec").update({"addresses": ip_addresses_with_mask})
        self.ip_address_pool_name = ipaddresspool_data.get("metadata").get("name")

        ipaddresspool_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ipaddresspool_file", delete=False
        )
        templating.dump_data_to_temp_yaml(ipaddresspool_data, ipaddresspool_file.name)

        exec_cmd(f"oc apply -f {ipaddresspool_file.name}", timeout=2400)
        logger.info("IP address pool created successfully")
        return OCP(
            kind="IPAddressPool",
            namespace=self.namespace,
            resource_name=self.ip_address_pool_name,
        ).check_resource_existence(
            should_exist=True, resource_name=self.ip_address_pool_name
        )

    def create_l2advertisement(self):
        """
        Create L2 advertisement for IP address pool
        :return: True if L2 advertisement is created, False otherwise
        """
        logger.info("Creating L2 advertisement for IP address pool")
        # METALLB_L2_ADVERTISEMENT_PATH
        l2_advertisement_data = templating.load_yaml(METALLB_L2_ADVERTISEMENT_PATH)
        if self.namespace != METALLB_DEFAULT_NAMESPACE:
            l2_advertisement_data.get("metadata").update({"namespace": self.namespace})

        self.l2Advertisement_name = l2_advertisement_data.get("metadata").get("name")
        l2_advertisement_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="l2_advertisement_file", delete=False
        )

        templating.dump_data_to_temp_yaml(
            l2_advertisement_data, l2_advertisement_file.name
        )

        exec_cmd(f"oc apply -f {l2_advertisement_file.name}", timeout=2400)
        logger.info("L2 advertisement created")
        return OCP(
            kind="L2Advertisement",
            namespace=self.namespace,
            resource_name=self.l2Advertisement_name,
        ).check_resource_existence(
            should_exist=True, resource_name=self.l2Advertisement_name
        )

    def deploy(self):
        """
        Deploy MetalLB
        """
        self.create_metallb_namespace()

        self.create_catalog_source()

        self.create_metallb_operator_group()

        self.create_metallb_subscription()

        self.create_ip_address_pool()

        self.create_l2advertisement()

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
            kind="L2Advertisement",
            namespace=self.namespace,
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
            kind="OperatorGroup",
            namespace=self.namespace,
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
            kind="subscription",
            namespace=self.namespace,
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
            kind="IPAddressPool",
            namespace=self.namespace,
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
            kind="CatalogSource",
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
        ocp = OCP(kind="namespace", resource_name=self.namespace)
        ocp.delete(resource_name=self.namespace)
        return ocp.check_resource_existence(
            resource_name=self.namespace,
            timeout=120,
            should_exist=False,
        )
