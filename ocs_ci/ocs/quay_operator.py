import logging
from time import sleep

from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import (
    storagecluster_independent_check,
    create_unique_resource_name,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError

logger = logging.getLogger(__name__)


class QuayOperator(object):
    """
    Quay operator class

    """

    def __init__(self):
        """
        Quay operator initializer function

        """
        self.namespace = constants.OPENSHIFT_OPERATORS
        self.ocp_obj = ocp.OCP(namespace=self.namespace)
        self.quay_operator = None
        self.quay_registry = None
        self.quay_registry_secret = None
        self.quay_pod_obj = OCP(kind=constants.POD, namespace=self.namespace)
        self.quay_registry_name = ""
        self.quay_operator_csv = ""
        self.quay_registry_secret_name = ""
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
        self.quay_operator_csv = get_csvs_start_with_prefix(
            csv_prefix=constants.QUAY_OPERATOR,
            namespace=self.namespace,
        )[0]["metadata"]["name"]

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
        self.quay_registry_secret_name = create_unique_resource_name(
            "quay-user", "secret"
        )
        logger.info(
            f"Creating Quay registry config for super-user access: {self.quay_registry_secret_name}"
        )
        self.quay_registry_secret = self.ocp_obj.exec_oc_cmd(
            command=f"create secret generic --from-file config.yaml={constants.QUAY_SUPER_USER} "
            f"{self.quay_registry_secret_name}"
        )
        quay_registry_data = templating.load_yaml(file=constants.QUAY_REGISTRY)
        self.quay_registry_name = quay_registry_data["metadata"]["name"]
        quay_registry_data["spec"][
            "configBundleSecret"
        ] = self.quay_registry_secret_name
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
        if self.quay_registry_secret:
            self.ocp_obj.exec_oc_cmd(f"delete secret {self.quay_registry_secret_name}")
        if self.quay_registry:
            self.quay_registry.delete()
        if self.quay_operator:
            self.quay_operator.delete()
        if self.quay_operator_csv:
            self.ocp_obj.exec_oc_cmd(
                f"delete {constants.CLUSTER_SERVICE_VERSION} "
                f"{self.quay_operator_csv}"
            )
