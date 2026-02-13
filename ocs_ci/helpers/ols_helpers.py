import logging
import tempfile
import time


from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    get_pod_logs,
    wait_for_pods_to_be_running,
)
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


def create_ols_secret(api_token=None):
    """

    Create credential secret for LLM provider (i.e IBM watsonx)

    Args:
        api_token (str, optional): Override API token. If None, uses value from
            config. Use e.g. "invalid-token" for negative (misconfiguration) tests.

    Returns:
        bool: True if secret created, False otherwise

    """
    log.info("Create credential secret for LLM provider")
    try:
        secret_data = templating.load_yaml(constants.OLS_SECRET_YAML)
        token = (
            api_token
            if api_token is not None
            else ocsci_config.AUTH["ibm_watsonx_llm_for_ols"]["api_token"]
        )
        secret_data["stringData"]["apitoken"] = token
        secret_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ols_secret_", suffix=".yaml", delete=False
        )
        templating.dump_data_to_temp_yaml(secret_data, secret_file.name)
        run_cmd(f"oc create -f {secret_file.name}")
        return True
    except Exception as ex:
        log.error(
            f"Failed to create credential secret for LLM provider, Exception is: {ex}"
        )
        return False


def create_ols_config(overrides=None):
    """

    Create custom resource "ols-config" file that contains
    the yaml content for the LLM provider.

    Args:
        overrides (dict, optional): Override LLM provider settings. Keys can be
            "url", "projectID", "model". Used for negative (misconfiguration) tests,
            e.g. overrides={"url": "https://invalid.example.com", "projectID": "invalid"}

    Returns:
        bool: True is ols-config is created, False otherwise

    """
    log.info(
        "Create custom resource ols-config file that contains the yaml content for the LLM provider"
    )
    try:
        # ToDo: when we get konflux build the code need to be modified to get lightspeed-image
       
        auth = ocsci_config.AUTH["ibm_watsonx_llm_for_ols"]
        project_id = (overrides or {}).get("projectID", auth["projectID"])
        url = (overrides or {}).get("url", auth["url"])
        model_name = (overrides or {}).get("model", auth["model"])
        ols_config_data = templating.load_yaml(constants.OLS_CONFIG_YAML)
        ols_config_data["spec"]["llm"]["providers"][0]["projectID"] = project_id
        ols_config_data["spec"]["llm"]["providers"][0]["url"] = url
        ols_config_data["spec"]["llm"]["providers"][0]["models"][0]["name"] = model_name
        ols_config_data["spec"]["ols"]["defaultModel"] = model_name
        config_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ols_config_", suffix=".yaml", delete=False
        )
        templating.dump_data_to_temp_yaml(ols_config_data, config_file.name)
        run_cmd(f"oc create -f {config_file.name}")
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


def verify_ols_connection_fails(timeout=300, interval=15):
    """

    Verify that OLS does NOT reach Available state within the given timeout.
    Used for negative tests (intentionally misconfigured OLS BYOK).

    Args:
        timeout (int): Maximum time in seconds to wait. If OLS becomes Available
            within this time, the verification fails (returns False).
        interval (int): Polling interval in seconds for checking OLSConfig status.

    Returns:
        bool: True if OLS never became Available (expected for misconfigured setup).
            False if OLS became Available (unexpected in negative scenario).

    """
    ols_config_obj = OCP(
        kind=constants.OLS_CONFIG_KIND, namespace=constants.OLS_OPERATOR_NAMESPACE
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            out = ols_config_obj.exec_oc_cmd(
                command=f"get {constants.OLS_CONFIG_KIND} -o yaml"
            )
            items = out.get("items") or []
            if not items:
                time.sleep(interval)
                continue
            conditions = items[0].get("status", {}).get("conditions") or []
            for cond in conditions:
                if cond.get("type") == "Available" and cond.get("status") == "True":
                    log.error(
                        "OLS connection succeeded (Available=True); expected failure for misconfigured setup"
                    )
                    return False
        except Exception as ex:
            log.debug("Could not get OLSConfig status: %s", ex)
        time.sleep(interval)
    log.info(
        "OLS did not reach Available state within timeout (misconfiguration behaved as expected)"
    )
    return True


def delete_ols_config_and_secret():
    """

    Delete the cluster OLSConfig and the watsonx API secret so that a new
    (e.g. misconfigured) config can be applied. Used before negative tests.

    """
    run_cmd(
        f"oc delete {constants.OLS_CONFIG_KIND} cluster -n {constants.OLS_OPERATOR_NAMESPACE} --ignore-not-found=true"
    )
    run_cmd(
        f"oc delete secret watsonx-api-keys -n {constants.OLS_OPERATOR_NAMESPACE} --ignore-not-found=true"
    )
    time.sleep(10)


def verify_ols_pod_logs_contain_expected_errors(
    expected_patterns=None,
    namespace=None,
    log_tail_lines=1000,
    require_all=False,
):
    """

    Verify that OLS pod logs contain the expected error pattern(s).
    Used in negative (misconfigured) tests to ensure the failure is visible in logs.

    Args:
        expected_patterns (list): List of strings to search for (case-insensitive).
            If None, uses default patterns typical for invalid LLM URL/credentials.
        namespace (str): Namespace where OLS pods run. Defaults to OLS_OPERATOR_NAMESPACE.
        log_tail_lines (int): Number of tail lines to fetch per pod. Default 1000.
        require_all (bool): If True, all patterns must be found. If False, at least
            one pattern must be found. Default False.

    Returns:
        tuple: (success: bool, message: str). success is True if the pattern
            condition is satisfied; message describes what was found or missing.

    """
    ns = namespace or constants.OLS_OPERATOR_NAMESPACE
    patterns = expected_patterns or [
        "error",
        "failed",
        "unauthorized",
        "401",
        "403",
        "connection refused",
        "invalid",
        "unable to connect",
        "timeout",
        "no such host",
        "connection reset",
        "dial tcp",
    ]

    pods = get_all_pods(namespace=ns)
    if not pods:
        return False, f"No pods found in namespace {ns} to check logs"

    all_logs = ""
    for pod in pods:
        try:
            logs = get_pod_logs(
                pod_name=pod.name,
                namespace=ns,
                all_containers=True,
                tail=log_tail_lines,
            )
            all_logs += f"\n--- pod: {pod.name} ---\n{logs or ''}"
        except Exception as ex:
            log.debug("Could not get logs for pod %s: %s", pod.name, ex)
            continue

    all_logs_lower = all_logs.lower()
    found = [p for p in patterns if p.lower() in all_logs_lower]
    missing = [p for p in patterns if p.lower() not in all_logs_lower]

    if require_all:
        success = len(missing) == 0
        if success:
            log.info(
                "OLS pod logs contain all expected error pattern(s): %s",
                patterns,
            )
            return True, f"Found all expected error pattern(s) in OLS pod logs: {patterns}"
        return (
            False,
            f"Not all expected error patterns found in OLS pod logs. "
            f"Missing: {missing}. Log snippet (last 500 chars): {all_logs[-500:]!r}",
        )
    # at least one
    if found:
        log.info(
            "OLS pod logs contain expected error pattern(s): %s",
            found,
        )
        return True, f"Found expected error pattern(s) in OLS pod logs: {found}"
    return (
        False,
        f"None of the expected error patterns found in OLS pod logs. "
        f"Patterns checked: {patterns}. Log snippet (last 500 chars): {all_logs[-500:]!r}",
    )

