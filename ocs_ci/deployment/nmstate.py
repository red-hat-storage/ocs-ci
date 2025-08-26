import logging

logger = logging.getLogger(__name__)

from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import TimeoutExpiredError, ResourceNotFoundError
from ocs_ci.deployment.helpers.lso_helpers import (
    create_optional_operators_catalogsource_non_ga,
)


class NMStateInstaller(object):
    """
    NMState Installer class for NMState deployment

    """

    def __init__(self):
        self.namespace = constants.NMSTATE_NAMESPACE

    def create_nmstate_operator_namespace(self):
        """
        Creates the namespace for NMState resources

        Raises:
            CommandFailed: If the 'oc create' command fails.

        """
        try:
            logger.info(f"Creating namespace {self.namespace} for NMState resources")
            namespace_yaml_file = templating.load_yaml(constants.NMSTATE_NAMESPACE_YAML)
            namespace_yaml = OCS(**namespace_yaml_file)
            namespace_yaml.create()
            logger.info(f"NMState namespace {self.namespace} was created successfully")
        except exceptions.CommandFailed as ef:
            if (
                f'project.project.openshift.io "{self.namespace}" already exists'
                in str(ef)
            ):
                logger.info(f"Namespace {self.namespace} already present")
                raise ef

    def create_nmstate_operatorgroup(self):
        """
        Creates an OperatorGroup for NMState

        """
        logger.info("Creating OperatorGroup for NMState")
        operatorgroup_yaml_file = templating.load_yaml(
            constants.NMSTATE_OPERATORGROUP_YAML
        )
        operatorgroup_yaml = OCS(**operatorgroup_yaml_file)
        operatorgroup_yaml.create()
        logger.info("NMState OperatorGroup created successfully")

    def create_nmstate_subscription(self):
        """
        Creates subscription for NMState operator

        """
        logger.info("Creating Subscription for NMState")
        catalog_name = constants.OPERATOR_CATALOG_SOURCE_NAME
        package_manifest = PackageManifest(
            resource_name=constants.NMSTATE_CSV_NAME,
            selector=f"catalog={catalog_name}",
        )
        try:
            package_manifest.get()
        except ResourceNotFoundError:
            create_optional_operators_catalogsource_non_ga()
            catalog_name = constants.OPTIONAL_OPERATORS
            package_manifest = PackageManifest(
                resource_name=constants.NMSTATE_CSV_NAME,
                selector=f"catalog={catalog_name}",
            )
        subscription_yaml_file = templating.load_yaml(
            constants.NMSTATE_SUBSCRIPTION_YAML
        )
        subscription_yaml_file["spec"]["source"] = catalog_name
        subscription_yaml = OCS(**subscription_yaml_file)
        subscription_yaml.create(do_reload=False)
        logger.info("NMState Subscription created successfully")

    def verify_nmstate_csv_status(self):
        """
        Verify the CSV status for the nmstate Operator deployment equals Succeeded

        """
        for csv in TimeoutSampler(
            timeout=900,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=constants.NMSTATE_CSV_NAME,
            namespace=self.namespace,
        ):
            if csv:
                break
        csv_name = csv[0]["metadata"]["name"]
        csv_obj = CSV(resource_name=csv_name, namespace=self.namespace)
        csv_obj.wait_for_phase(phase="Succeeded", timeout=720)

    def create_nmstate_instance(self):
        """
        Create an instance of the nmstate Operator

        """
        logger.info("Creating NMState Instance")
        subscription_yaml_file = templating.load_yaml(constants.NMSTATE_INSTANCE_YAML)
        subscription_yaml = OCS(**subscription_yaml_file)
        subscription_yaml.create()
        logger.info("NMState Instance created successfully")

    def verify_nmstate_pods_running(self):
        """
        Verify the pods for NMState Operator are running

        """
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=self.count_nmstate_pods_running,
            count=10,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError(
                "Not all nmstate pods in Running state after 300 seconds"
            )

    def count_nmstate_pods_running(self, count):
        """
        Count the pods for NMState Operator are running

        Returns:
            bool:

        """
        count_running_nmstate_pods = 0
        ocp_pod = OCP(kind=constants.POD, namespace=self.namespace)
        pod_items = ocp_pod.get().get("items")
        # Check if nmstate pods are in running state
        for nmstate_pod in pod_items:
            nmstate_pod_name = nmstate_pod.get("metadata").get("name")
            status = ocp_pod.get_resource_status(nmstate_pod_name)
            if status == constants.STATUS_RUNNING:
                logger.info(f"NMState pod {nmstate_pod_name} in running state")
                count_running_nmstate_pods += 1
        return count_running_nmstate_pods >= count

    def running_nmstate(self):
        """
        Install NMState operator and create an instance

        """
        self.create_nmstate_operator_namespace()
        self.create_nmstate_operatorgroup()
        self.create_nmstate_subscription()
        self.verify_nmstate_csv_status()
        self.create_nmstate_instance()
        self.verify_nmstate_pods_running()
