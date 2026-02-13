import logging
import tempfile
import time


from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs import constants
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

# OLSConfig status condition type used to determine connection ready state.
# Only ApiReady is checked; ready when ApiReady has reason=Available, status=True.
OLS_READY_CONDITION_TYPE = "ApiReady"


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


def _get_ols_config_conditions():
    """Fetch OLSConfig status conditions from the cluster."""
    ols_config_obj = OCP(
        kind=constants.OLS_CONFIG_KIND, namespace=constants.OLS_OPERATOR_NAMESPACE
    )
    out = ols_config_obj.exec_oc_cmd(command=f"get {constants.OLS_CONFIG_KIND} -o yaml")
    items = out.get("items") or []
    if not items:
        return []
    return items[0].get("status", {}).get("conditions") or []


def _is_ols_connection_ready(conditions):
    """

    Return True if OLS connection is ready based on ApiReady status condition only.

    Ready when ApiReady has reason=Available and status=True.
    Other condition types (e.g. ConsolePluginReady, CacheReady) are not checked.

    """
    by_type = {c.get("type"): c for c in conditions if c.get("type")}
    cond = by_type.get(OLS_READY_CONDITION_TYPE)
    if not cond:
        return False
    return cond.get("reason") == "Available" and cond.get("status") == "True"


def wait_for_ols_connection_state(expect_ready=True, timeout=300, interval=15):
    """

    Wait until OLS connection state matches the expected state (single shared logic).

    Uses OLSConfig ApiReady condition only: ready when ApiReady has
    reason=Available, status=True.

    Args:
        expect_ready (bool): If True, wait until connection is ready (returns True
            when ready, False on timeout). If False, wait until timeout without
            ever seeing ready (returns True when timeout without ready, False if
            connection became ready).
        timeout (int): Maximum time in seconds to wait.
        interval (int): Polling interval in seconds.

    Returns:
        bool: True when the desired state was observed (ready and expect_ready,
            or not ready after timeout when expect_ready=False). False when
            expect_ready=True and timeout, or when expect_ready=False and
            connection became ready.

    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conditions = _get_ols_config_conditions()
            ready = _is_ols_connection_ready(conditions)
            if expect_ready:
                if ready:
                    log.info("OLS connection is ready (ApiReady Available).")
                    return True
            else:
                if ready:
                    log.error(
                        "OLS connection became ready (expected failure for misconfigured setup)"
                    )
                    return False
        except Exception as ex:
            log.debug("Could not get OLSConfig status: %s", ex)
        time.sleep(interval)

    if expect_ready:
        log.warning("OLS connection did not become ready within timeout.")
        return False
    log.info(
        "OLS did not reach ready state within timeout (misconfiguration behaved as expected)."
    )
    return True


@retry(ResourceWrongStatusException, tries=20, delay=5, backoff=3)
def verify_ols_connects_to_llm(timeout=600, interval=15):
    """

    Verifies OLS pods are up and running and OLSConfig reaches connection-ready state.

    Ready is defined as ApiReady having reason=Available, status=True.

    """
    if not wait_for_pods_to_be_running(namespace=constants.OLS_OPERATOR_NAMESPACE):
        raise ResourceWrongStatusException("OLS pods did not reach Running state")

    if not wait_for_ols_connection_state(
        expect_ready=True, timeout=timeout, interval=interval
    ):
        conditions = _get_ols_config_conditions()
        by_type = {c.get("type"): c for c in conditions if c.get("type")}
        raise ResourceWrongStatusException(
            "OLS connection did not become ready within timeout. "
            f"ApiReady must be Available/True. Conditions: {by_type}"
        )


def verify_ols_connection_fails(timeout=300, interval=15):
    """

    Verify that OLS does NOT reach connection-ready state within the given timeout.
    Used for negative tests (intentionally misconfigured OLS BYOK).

    Ready is defined as ApiReady Available/True. Returns True if OLS never became
    ready (expected); False if it became ready.

    """
    return wait_for_ols_connection_state(
        expect_ready=False, timeout=timeout, interval=interval
    )


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
            return (
                True,
                f"Found all expected error pattern(s) in OLS pod logs: {patterns}",
            )
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
