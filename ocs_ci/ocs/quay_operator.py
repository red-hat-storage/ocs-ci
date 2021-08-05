import logging
from time import sleep

from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import storagecluster_independent_check
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, exec_cmd, run_cmd
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError

logger = logging.getLogger(__name__)


class QuayOperator(object):
    """
    Quay operator class

    """

    def __init__(self):
        """
        Initializer function

        """
        self.namespace = constants.OPENSHIFT_OPERATORS
        self.quay_operator = None
        self.quay_registry = None
        self.quay_pod_obj = OCP(kind=constants.POD, namespace=self.namespace)
        self.quay_registry_name = ""
        self.quay_operator_csv = ""
        self.sc_default = False
        self.sc_name = (
            constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            if storagecluster_independent_check()
            else constants.DEFAULT_STORAGECLASS_RBD
        )

    def setup_quay_operator(self):
        """
        Deploys Quay operator

        """
        quay_operator_data = templating.load_yaml(file=constants.QUAY_SUB)
        self.quay_operator_csv = quay_operator_data["spec"]["startingCSV"]
        self.quay_operator = OCS(**quay_operator_data)
        logger.info(f"Installing Quay operator: {self.quay_operator.name}")
        self.quay_operator.create()
        for quay_pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, constants.QUAY_OPERATOR, self.namespace
        ):
            if quay_pod:
                self.quay_pod_obj.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    resource_name=quay_pod[0],
                    sleep=30,
                    timeout=600,
                )
                break

    def create_quay_registry(self):
        """
        Creates Quay registry

        """
        if not helpers.get_default_storage_class():
            patch = ' \'{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}\' '
            run_cmd(
                f"oc patch storageclass {self.sc_name} "
                f"-p {patch} "
                f"--request-timeout=120s"
            )
            self.sc_default = True
        quay_registry_data = templating.load_yaml(file=constants.QUAY_REGISTRY)
        self.quay_registry_name = quay_registry_data["metadata"]["name"]
        self.quay_registry = OCS(**quay_registry_data)
        logger.info(f"Creating Quay registry: {self.quay_registry.name}")
        self.quay_registry.create()
        logger.info("Waiting for 15s for registry to get initialized")
        sleep(15)
        self.wait_for_quay_endpoint()

    def wait_for_quay_endpoint(self):
        """
        Waits for quay registry endpoint

        """
        logger.info("Waiting for quay registry endpoint to be up")
        sample = TimeoutSampler(
            timeout=300,
            sleep=15,
            func=self.check_quay_registry_endpoint,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error("Quay registry endpoint did not get created.")
            raise TimeoutExpiredError
        else:
            logger.info("Quay registry endpoint is up")

    def check_quay_registry_endpoint(self):
        """
        Checks if quay registry endpoint is up

        Returns:
            bool: True if quay endpoint is up else False

        """
        return (
            True
            if self.quay_registry.get().get("status").get("registryEndpoint")
            else False
        )

    def teardown(self):
        """
        Quay operator teardown

        """
        if self.sc_default:
            patch = ' \'{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}\' '
            run_cmd(
                f"oc patch storageclass {self.sc_name} "
                f"-p {patch} "
                f"--request-timeout=120s"
            )
        if self.quay_registry:
            self.quay_registry.delete()
        if self.quay_operator:
            self.quay_operator.delete()
        exec_cmd(
            f"oc delete {constants.CLUSTER_SERVICE_VERSION} {self.quay_operator_csv} -n {self.namespace}"
        )
