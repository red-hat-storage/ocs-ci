import logging
import tempfile

from ocs_ci.ocs.constants import (
    METALLB_CATALOG_SOURCE_YAML,
    MARKETPLACE_NAMESPACE,
    METALLB_DEFAULT_NAMESPACE,
    METALLB_OPERATOR_GROUP_YAML,
    METALLB_SUBSCRIPTION_YAML,
    METALLB_CONTROLLER_MANAGER_PREFIX,
    METALLB_WEBHOOK_PREFIX,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)

# TODO: create Metal LB instance
# TODO: Reserve ipaddresses
# TODO: create ipaddresspool
# TODO: adjust func for ipaddresspool
# TODO: create l2advertisement for ipaddresspool


class MetalLBInstaller:
    def __init__(self, version: str, ip_list: list, namespace: str = "metallb-system"):
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
