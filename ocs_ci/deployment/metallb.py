import json
import logging
import tempfile
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import (
    QE_APP_REGISTRY_SOURCE,
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
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.csv import check_all_csvs_are_succeeded
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    exec_cmd,
    get_ocp_version,
    TimeoutSampler,
    wait_for_machineconfigpool_status,
)

logger = logging.getLogger(__name__)


class MetalLBInstaller:
    def __init__(
        self,
        namespace: str = "metallb-system",
    ):
        self.addresses_reserved = None
        self.namespace_lb = namespace
        self.l2Advertisement_name = None
        self.ip_address_pool_name = None
        self.subscription_name = None
        self.operatorgroup_name = None
        self.catalog_source_name = None
        self.hostnames = []
        self.timeout_check_resources_existence = 6
        self.timeout_wait_csvs_minutes = 20

    def create_metallb_namespace(self):
        """
        Create MetalLB namespace
        Returns:
            bool: True if namespace is created, False otherwise
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

        Returns:
            bool: True if catalog source is created, False otherwise
        """
        return CatalogSource(
            resource_name=self.catalog_source_name,
            namespace=MARKETPLACE_NAMESPACE,
        ).check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            should_exist=True,
            resource_name=self.catalog_source_name,
        )

    @retry(CommandFailed, tries=3, delay=15)
    def create_catalog_source(self):
        """
        Create catalog source for MetalLB

        Returns:
            bool: True if catalog source is created, False otherwise, error if not get Ready state
        """
        logger.info("Creating catalog source for MetalLB")
        # replace latest version with specific version
        catalog_source_data = templating.load_yaml(QE_APP_REGISTRY_SOURCE)

        metallb_version = config.default_cluster_ctx.ENV_DATA.get("metallb_version")
        if not metallb_version:
            metallb_version = get_ocp_version()

        image_placeholder = catalog_source_data.get("spec").get("image")
        catalog_source_data.get("spec").update(
            {"image": image_placeholder.format(metallb_version)}
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

        Returns:
            bool: True if operator group is created, False otherwise
        """
        # operatorgroup name installed with UI has a dynamic suffix. So, we need to check not using the name of resource
        cmd = "oc get operatorgroup -n metallb-system | awk 'NR>1 {print \"true\"; exit}' "
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode != 0:
            logger.error(f"Failed to get operatorgroup crs \n{cmd_res.stderr}")
            return False

        return cmd_res.stdout.decode("utf-8").strip() == "true"

    @retry(CommandFailed, tries=3, delay=15)
    def create_metallb_operator_group(self):
        """
        Create MetalLB operator group

        Returns:
            bool: True if operator group is created, False otherwise
        """
        logger.info("Creating MetalLB operator group")
        operator_group_data = templating.load_yaml(METALLB_OPERATOR_GROUP_YAML)

        self.operatorgroup_name = operator_group_data.get("metadata").get("name")

        # check if OperatorGroup already exists
        if self.metallb_operator_group_created():
            logger.info("OperatorGroup already exists")
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
        Returns:
            bool: True if subscription already exists, False otherwise
        """
        return OCP(
            kind=constants.SUBSCRIPTION_COREOS,
            namespace=self.namespace_lb,
            resource_name=self.subscription_name,
        ).check_resource_existence(
            should_exist=True, resource_name=self.subscription_name
        )

    @retry(CommandFailed, tries=3, delay=15)
    def create_metallb_subscription(self):
        """
        Create MetalLB subscription

        Returns:
            bool: True if subscription is created, and metallb pods are Ready, False otherwise
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

        try:
            self.wait_csv_installed()

        except Exception as e:
            logger.error(f"Error during MetalLb installation: {e}, sleep 30 sec")
            # trying to wait for MetalLB csvs ready in case of exception
            time.sleep(30)
            return False

        metallb_pods = get_pod_name_by_pattern(
            METALLB_CONTROLLER_MANAGER_PREFIX, self.namespace_lb
        )
        metallb_pods.extend(
            get_pod_name_by_pattern(METALLB_WEBHOOK_PREFIX, self.namespace_lb)
        )

        return self.subscription_created() and wait_for_pods_to_be_running(
            namespace=self.namespace_lb, pod_names=metallb_pods, timeout=300
        )

    @retry(CommandFailed, tries=8, delay=3)
    def metallb_instance_created(self):
        """
        Check if MetalLB instance is created

        Returns:
            bool: True if MetalLB instance is created, False otherwise
        """
        return OCP(
            kind=constants.METALLB_INSTANCE,
            namespace=self.namespace_lb,
            resource_name="metallb",
        ).check_resource_existence(
            timeout=self.timeout_check_resources_existence,
            should_exist=True,
            resource_name="metallb",
        )

    def metallb_kind_available(self):
        """
        Check if MetalLB Kind is available
        This method is a hack to avoid 'Error is error: the server doesn't have a resource type "MetalLB"' or time.sleep

        Returns:
            bool: True if MetalLB Kind is available, False otherwise
        """
        return bool(
            len(
                (
                    exec_cmd("oc api-resources | grep MetalLB", shell=True)
                    .stdout.decode("utf-8")
                    .strip()
                )
            )
        )

    def create_metallb_instance(self):
        """
        Create MetalLB instance
        Returns:
            bool: True if MetalLB instance is created, False/None otherwise
        """

        if self.metallb_instance_created():
            logger.info("MetalLB instance already exists")
            return

        # hack to avoid Error is error: the server doesn't have a resource type "MetalLB"
        # that appears even after csv is installed and operator pods are running
        for sample in TimeoutSampler(
            timeout=10 * 60,
            sleep=15,
            func=self.metallb_kind_available,
        ):
            if sample:
                logger.info("MetalLB api is available")
                break

        logger.info("Creating MetalLB instance")
        metallb_inst_data = templating.load_yaml(constants.METALLB_INSTANCE_YAML)
        metallb_inst_data.get("metadata").update({"namespace": self.namespace_lb})

        metallb_inst_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="metallb_instance_", delete=False
        )
        templating.dump_data_to_temp_yaml(metallb_inst_data, metallb_inst_file.name)

        retry(CommandFailed, tries=3, delay=15)(exec_cmd)(
            f"oc apply -f {metallb_inst_file.name}", timeout=240
        )

        return self.metallb_instance_created()

    def create_ip_address_pool(self):
        """
        Create IP address pool for MetalLB

        Returns:
            bool: True if IP address pool is created, False if creation failed

        Raises:
            NotImplementedError: if platform is not supported
            ValueError: if number of reserved IP addresses for MetalLB is not specified
        """

        # common part for both platforms
        ipaddresspool_data = templating.load_yaml(METALLB_IPADDRESSPOOL_PATH)
        if self.namespace_lb != METALLB_DEFAULT_NAMESPACE:
            ipaddresspool_data.get("metadata").update({"namespace": self.namespace_lb})

        self.ip_address_pool_name = ipaddresspool_data.get("metadata").get("name")

        if self.ip_address_pool_created():
            logger.info(
                f"IPAddressPool {self.ip_address_pool_name} already exists in the namespace {self.namespace_lb}"
            )
            return True

        # if IP addresses are specified, create IPAddressPool with the list of IPs
        if config.ENV_DATA.get("ip_address_pool"):
            ipaddresspool_data.get("spec").update(
                {"addresses": config.ENV_DATA["ip_address_pool"]}
            )
        # if IP addresses are not specified, follow the logic specified for each platform
        elif config.ENV_DATA["platform"] == constants.HCI_VSPHERE:
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

            # due to circular import error, import is here
            from ocs_ci.deployment.vmware import assign_ips

            logger.info("Reserving IP addresses from IPAM and Creating IP address pool")
            self.addresses_reserved = assign_ips(hosts=self.hostnames)

            ip_addresses_with_mask = [ip + "/32" for ip in self.addresses_reserved]
            ipaddresspool_data.get("spec").update({"addresses": ip_addresses_with_mask})
        else:
            logger.info(
                "config.ENV_DATA['ip_address_pool'] is not specified and selected platform doesn't support "
                "dynamic allocation (or it is not implemented)"
            )

        ipaddresspool_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ipaddresspool_file", delete=False
        )
        templating.dump_data_to_temp_yaml(ipaddresspool_data, ipaddresspool_file.name)

        retry(CommandFailed, tries=3, delay=15)(exec_cmd)(
            f"oc apply -f {ipaddresspool_file.name}", timeout=240
        )

        return self.ip_address_pool_created()

    def ip_address_pool_created(self):
        """
        Check if IP address pool is created

        Returns:
            bool: True if IP address pool is created, False otherwise
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

        Args:
            ipaddresspool_data (dict): IP address pool data. YAML accessible as dict

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
        Returns:
            bool: True if L2 advertisement is created, False otherwise
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

    @retry(CommandFailed, tries=3, delay=15)
    def create_l2advertisement(self):
        """
        Create L2 advertisement for IP address pool

        Returns:
            bool: True if L2 advertisement is created, False if failed, None if L2 advertisement already exists
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

        Returns:
            bool: True if MetalLB is deployed, False otherwise
        """

        if not config.DEPLOYMENT.get("metallb_operator"):
            logger.info("MetalLB operator deployment is not requested")
            return True

        logger.info(
            f"Deploying MetalLB and dependant resources to namespace: '{self.namespace_lb}'"
        )

        if self.apply_icsp():
            logger.info("ICSP brew-registry applied successfully")
        if self.create_metallb_namespace():
            logger.info(f"Namespace {self.namespace_lb} created successfully")
        if self.create_catalog_source():
            logger.info("MetalLB catalog source created successfully")
        if self.create_metallb_operator_group():
            logger.info("MetalLB operator group created successfully")
        if self.create_metallb_subscription():
            logger.info("MetalLB subscription created successfully")
        if self.create_metallb_instance():
            logger.info("MetalLB instance created successfully")
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

        if config.ENV_DATA["platform"] == constants.HCI_VSPHERE:
            release_ips(hosts=self.hostnames)

        self.delete_ipaddresspool()

        self.delete_subscription()

        self.delete_operatorgroup()

        self.delete_catalogsource()

        self.delete_metallb_namespace()

    def delete_l2advertisement(self):
        """
        Delete l2advertisement

        Returns:
            bool: True if l2advertisement is deleted, False otherwise
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

        Returns:
            bool: True if operator group is deleted, False otherwise
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

        Returns:
            bool: True if subscription is deleted, False otherwise
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

        Returns:
             bool: True if ipaddresspool is deleted, False otherwise
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

        Returns:
             bool: True if catalog source is deleted, False otherwise
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

        Returns:
            True if namespace is deleted, False otherwise
        """
        ocp = OCP(kind="namespace", resource_name=self.namespace_lb)
        ocp.delete(resource_name=self.namespace_lb)
        return ocp.check_resource_existence(
            resource_name=self.namespace_lb,
            timeout=120,
            should_exist=False,
        )

    def wait_csv_installed(self):
        """
        Verify if MetalLB CSV is installed

        Returns:
            bool: True if MetalLB CSV is installed, False otherwise
        """
        for sample in TimeoutSampler(
            timeout=self.timeout_wait_csvs_minutes * 60,
            sleep=15,
            func=check_all_csvs_are_succeeded,
            namespace=self.namespace_lb,
        ):
            if sample:
                logger.info("MetalLB CSV installed successfully")
                break
        return True

    def icsp_brew_registry_exists(self):
        """
        Check if the ICSP Brew registry exists

        Returns:
            bool: True if the ICSP Brew registry exists, False otherwise
        """
        return OCP(
            kind="ImageContentSourcePolicy", resource_name="brew-registry"
        ).check_resource_existence(
            timeout=self.timeout_check_resources_existence, should_exist=True
        )

    def apply_icsp(self):
        """
        Apply the ICSP to the cluster
        """
        if self.icsp_brew_registry_exists():
            logger.info("ICSP Brew registry already exists")
            return
        icsp_data = templating.load_yaml(constants.SUBMARINER_DOWNSTREAM_BREW_ICSP)
        icsp_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="acm_icsp", delete=False
        )
        templating.dump_data_to_temp_yaml(icsp_data, icsp_data_yaml.name)
        exec_cmd(f"oc create -f {icsp_data_yaml.name}", timeout=300)
        wait_for_machineconfigpool_status(node_type="all")
        logger.info("ICSP applied successfully")
        return self.icsp_brew_registry_exists()
