import logging
import tempfile


from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.utility.utils import exec_cmd, run_cmd
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry


log = logging.getLogger(__name__)


def do_deploy_ols():
    """

    Handle OpenshiftLightspeed operator installation

    Returns:
        bool: True if OLS operator is installed, False otherwise

    """
    log.info("Creating OpenshiftLightspeed Operator")

    # check if OLS is already installed
    if validate_ols_operator_installed(timeout=10):
        log.info("OLS Operator already installed")
        return True

    try:
        exec_cmd(f"oc create -f {constants.OLS_OPERATOR_YAML}")
        validate_ols_operator_installed()
        wait_for_pods_to_be_running(namespace=constants.OLS_OPERATOR_NAMESPACE)
        return True
    except Exception as ex:
        log.error(f"Failed to install OLS Operator. Exception is: {ex}")
        return False


def validate_ols_operator_installed(
    namespace=constants.OLS_OPERATOR_NAMESPACE,
    operator_name=constants.OLS_OPERATOR_NAME,
    timeout=600,
):
    """

    Validate whether the OLS operator is installed.

    The method checks for the presence of a clusterServiceVersion (CSV) and operator.

    Args:
        namespace (str): Namespace
        operator_name (str): Name of the operator
        timeout (int): Time to wait OLS CSV reached in succeeded state

    Returns:
        bool : True if operator installation succeeaded

    Raises:
        ResourceWrongStatusException: In case the resource is not in expected phase.
        NotSupportedFunctionError: If resource doesn't have phase!
        ResourceNameNotSpecifiedException: in case the name is not specified.

    """
    log.info(f"Validating installation of OLS operator {operator_name}")
    csv_obj = CSV(
        resource_name="lightspeed-operator-controller-manager", namespace=namespace
    )
    return csv_obj.wait_for_phase(phase=constants.SUCCEEDED, timeout=timeout)


def create_ols_secret():
    """

    Create credential secret for LLM provider (i.e IBM watsonx)

    Returns:
        bool: True if secret created, False otherwise

    """
    log.info("Create credential secret for LLM provider")
    try:
        ols_secret_obj = templating.load_yaml(constants.OLS_SECRET_YAML)
        api_token = ocsci_config.AUTH["ibm_watsonx_llm_for_ols"]["api_token"]
        ols_secret_obj["stringData"]["apitoken"] = api_token
        ols_secret_obj = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ols_secret_obj", delete=False
        )
        templating.dump_data_to_temp_yaml(ols_secret_obj, ols_secret_obj.name)
        run_cmd(f"oc create -f {ols_secret_obj.name}")
        return True
    except Exception as ex:
        log.error(
            f"Failed to create credential secret for LLM provider, Exception is: {ex}"
        )
        return False


def create_ols_config():
    """

    Create custom resource "ols-config" file that contains
    the yaml content for the LLM provider

    Returns:
        bool: True is ols-config is created, False otherwise

    """
    log.info(
        "Create custom resource ols-config file that contains the yaml content for the LLM provider"
    )
    try:
        # ToDo: when we get konflux build the code need to be modified to get lightspeed-image
        openshift_lightspeed_image = get_openshift_lightspeed_image()
        project_id = ocsci_config.AUTH["ibm_watsonx_llm_for_ols"]["projectID"]
        url = ocsci_config.AUTH["ibm_watsonx_llm_for_ols"]["url"]
        model_name = ocsci_config.AUTH["ibm_watsonx_llm_for_ols"]["model"]
        ols_config_obj = templating.load_yaml(constants.OLS_CONFIG_YAML)
        ols_config_obj["spec"]["ols"]["rag"][0]["image"] = openshift_lightspeed_image
        ols_config_obj["spec"]["llm"]["providers"][0]["projectID"] = project_id
        ols_config_obj["spec"]["llm"]["providers"][0]["url"] = url
        ols_config_obj["spec"]["llm"]["providers"][0]["models"][0]["name"] = model_name
        ols_config_obj["spec"]["ols"]["defaultModel"] = model_name
        ols_config_obj = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ols_config_obj", delete=False
        )
        templating.dump_data_to_temp_yaml(ols_config_obj, ols_config_obj.name)
        run_cmd(f"oc create -f {ols_config_obj.name}")
        return True
    except Exception as ex:
        log.error(f"Failed to create ols-config. Exception is: {ex}")
        return False


@retry(ResourceWrongStatusException, tries=20, delay=5, backoff=3)
def verify_ols_connects_to_llm():
    """

    Verifies ols pods are up and running, and successfully connected to LLM provider

    """

    # verify all the pods are running
    if not wait_for_pods_to_be_running(namespace=constants.OLS_OPERATOR_NAMESPACE):
        return False

    # Verify the OLS connected to LLM
    ols_config_obj = OCP(
        kind=constants.OLS_CONFIG_KIND, namespace=constants.OLS_OPERATOR_NAMESPACE
    )
    command = f"get {constants.OLS_CONFIG_KIND} -oyaml"
    ols_yaml_output = ols_config_obj.exec_oc_cmd(command=command)
    ols_status = ols_yaml_output["items"][0]["status"]["conditions"]
    for status in ols_status:
        if status["status"] and status["reason"] == "Available":
            log.info(f"Type {status['type']} is in expected state")
        else:
            log.error(f"Type {status['type']} is in not expected state")
            raise ResourceWrongStatusException(
                f"Resource type: {status['type']} is not in expected state: {status}. OLS is not configured correctly"
            )


def get_openshift_lightspeed_image():
    """

    Get openshift lightspeed rag image

    Returns:
        image (str) : openshift lightspeed rag image

    """
    csv_obj = CSV(namespace=ocsci_config.ENV_DATA["cluster_namespace"])
    for item in csv_obj.get()["items"]:
        if item["metadata"]["name"].startswith(defaults.OCS_OPERATOR_NAME) or item[
            "metadata"
        ]["name"].startswith(constants.OCS_CLIENT_OPERATOR):
            for i in item["spec"]["relatedImages"]:
                if i["name"].startswith("odf-lightspeed-rag-content"):
                    return i["image"]
