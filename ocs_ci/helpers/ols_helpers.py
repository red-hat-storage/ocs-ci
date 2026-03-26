import logging
import os
import tempfile
import time


from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_all_pods,
    get_pod_logs,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.utility.utils import exec_cmd, run_cmd, TimeoutSampler
from ocs_ci.utility import templating
from ocs_ci.utility import version as version_util
from ocs_ci.utility.retry import retry


log = logging.getLogger(__name__)

# OLSConfig status condition type used to determine connection ready state.
# Only ApiReady is checked; ready when ApiReady has reason=Available, status=True.
OLS_READY_CONDITION_TYPE = "ApiReady"


def get_ols_rag_content_image():
    """

    Build the OLS RAG content image reference for the cluster under test.

    Uses ``quay.io/rhceph-dev/odf4-odf-lightspeed-rag-content-rhel9`` with tag
    ``v{major}.{minor}`` derived from ``ENV_DATA["ocs_version"]`` (same ODF version
    as the rest of ocs-ci). Override with full image ref in
    ``AUTH["ibm_watsonx_llm_for_ols"]["rag_content_image"]`` when needed.

    Returns:
        str: Container image pull spec (repo:tag).

    """
    auth_ols = ocsci_config.AUTH.get("ibm_watsonx_llm_for_ols") or {}
    override = auth_ols.get("rag_content_image")
    if override:
        return override
    sv = version_util.get_semantic_ocs_version_from_config()
    tag = f"v{sv.major}.{sv.minor}"
    return f"{constants.OLS_RAG_CONTENT_IMAGE_REPO}:{tag}"


def get_ols_operator_csv_name(namespace=constants.OLS_OPERATOR_NAMESPACE):
    """

    Resolve the installed OLS ClusterServiceVersion name (e.g. ``lightspeed-operator.v1.0.10``).

    Args:
        namespace (str): Namespace where the OLS subscription installs the CSV.

    Returns:
        str or None: CSV ``metadata.name`` if a CSV with the expected prefix exists.

    """
    matches = get_csvs_start_with_prefix(
        constants.OLS_OPERATOR_CSV_NAME_PREFIX, namespace
    )
    if not matches:
        return None
    return matches[0]["metadata"]["name"]


def do_deploy_ols():
    """

    Handle OpenshiftLightspeed operator installation

    Returns:
        bool: True if OLS operator is installed, False otherwise

    """
    log.info("Creating OpenshiftLightspeed Operator")

    # check if OLS is already installed (short timeout / fast poll)
    if validate_ols_operator_installed(timeout=10, interval=1):
        log.info("OLS Operator already installed")
        return True

    try:
        exec_cmd(f"oc create -f {constants.OLS_OPERATOR_YAML}")
        validate_ols_operator_installed()
        wait_for_pods_to_be_running(namespace=constants.OLS_OPERATOR_NAMESPACE)
        return True
    except (CommandFailed, ResourceWrongStatusException) as ex:
        log.error("Failed to install OLS Operator: %s", ex)
        return False
    except Exception as ex:
        log.error("Unexpected error installing OLS Operator: %s", ex)
        return False


def validate_ols_operator_installed(
    namespace=constants.OLS_OPERATOR_NAMESPACE,
    operator_name=constants.OLS_OPERATOR_NAME,
    timeout=600,
    interval=5,
):
    """

    Validate whether the OLS operator is installed.
    Args:
        namespace (str): Namespace
        operator_name (str): Subscription/package name (for logging only)
        timeout (int): Time to wait for the CSV to exist and reach ``Succeeded``
        interval (int): Seconds between polls when waiting for CSV / phase

    Returns:
        bool: True if operator installation succeeded.

    Raises:
        ResourceWrongStatusException: If CSV not found or not in expected phase in time.
        NotSupportedFunctionError: If resource doesn't have phase!
        ResourceNameNotSpecifiedException: in case the name is not specified.

    """
    log.info(
        "Validating OLS operator installation (package ref: %s, CSV prefix: %s)",
        operator_name,
        constants.OLS_OPERATOR_CSV_NAME_PREFIX,
    )

    def _ols_csv_succeeded():
        csv_name = get_ols_operator_csv_name(namespace=namespace)
        if not csv_name:
            return False
        log.debug("Resolved OLS CSV name: %s", csv_name)
        csv_obj = CSV(resource_name=csv_name, namespace=namespace)
        return csv_obj.check_phase(phase=constants.SUCCEEDED)

    sampler = TimeoutSampler(timeout, interval, func=_ols_csv_succeeded)
    if not sampler.wait_for_func_status(True):
        raise ResourceWrongStatusException(
            f"OLS operator CSV with prefix {constants.OLS_OPERATOR_CSV_NAME_PREFIX!r} "
            f"not found or not {constants.SUCCEEDED} in namespace {namespace} within {timeout}s"
        )
    return True


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
    secret_file = tempfile.NamedTemporaryFile(
        mode="w+", prefix="ols_secret_", suffix=".yaml", delete=False
    )
    secret_path = secret_file.name
    secret_file.close()
    try:
        secret_data = templating.load_yaml(constants.OLS_SECRET_YAML)
        token = (
            api_token
            if api_token is not None
            else ocsci_config.AUTH["ibm_watsonx_llm_for_ols"]["api_token"]
        )
        secret_data["stringData"]["apitoken"] = token
        templating.dump_data_to_temp_yaml(secret_data, secret_path)
        run_cmd(f"oc create -f {secret_path}")
        return True
    except (CommandFailed, KeyError, OSError) as ex:
        log.error(
            "Failed to create credential secret for LLM provider: %s",
            ex,
        )
        return False
    finally:
        try:
            os.unlink(secret_path)
        except OSError:
            pass


def create_ols_config(overrides=None):
    """

    Create custom resource "ols-config" file that contains
    the yaml content for the LLM provider.

    Args:
        overrides (dict, optional): Override LLM provider settings. Keys can be
            "url", "projectID", "model". Used for negative (misconfiguration) tests,
            e.g. overrides={"url": "https://invalid.example.com", "projectID": "invalid"}.
            RAG image is ``get_ols_rag_content_image()`` (Quay dev repo, tag ``v{major}.{minor}``
            from ``ENV_DATA["ocs_version"]``) unless ``AUTH[...]["rag_content_image"]`` is set.

    Returns:
        bool: True is ols-config is created, False otherwise

    """
    log.info(
        "Create custom resource ols-config file that contains the yaml content for the LLM provider"
    )
    config_file = tempfile.NamedTemporaryFile(
        mode="w+", prefix="ols_config_", suffix=".yaml", delete=False
    )
    config_path = config_file.name
    config_file.close()
    try:
        auth = ocsci_config.AUTH["ibm_watsonx_llm_for_ols"]
        project_id = (overrides or {}).get("projectID", auth["projectID"])
        url = (overrides or {}).get("url", auth["url"])
        model_name = (overrides or {}).get("model", auth["model"])
        ols_config_data = templating.load_yaml(constants.OLS_CONFIG_YAML)
        ols_config_data["spec"]["llm"]["providers"][0]["projectID"] = project_id
        ols_config_data["spec"]["llm"]["providers"][0]["url"] = url
        ols_config_data["spec"]["llm"]["providers"][0]["models"][0]["name"] = model_name
        ols_config_data["spec"]["ols"]["defaultModel"] = model_name
        rag = ols_config_data.get("spec", {}).get("ols", {}).get("rag") or []
        if rag:
            rag[0]["image"] = get_ols_rag_content_image()
            log.info("OLS RAG content image: %s", rag[0]["image"])
        templating.dump_data_to_temp_yaml(ols_config_data, config_path)
        run_cmd(f"oc create -f {config_path}")
        return True
    except (CommandFailed, KeyError, OSError) as ex:
        log.error("Failed to create ols-config: %s", ex)
        return False
    finally:
        try:
            os.unlink(config_path)
        except OSError:
            pass


def _get_ols_config_conditions():
    """

    Fetch OLSConfig status conditions from the cluster.

    Returns:
        list: ``status.conditions`` for the cluster OLSConfig, or empty list if missing.

    """
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

    Args:
        conditions (list): OLSConfig ``status.conditions`` list from the cluster.

    Returns:
        bool: True if ApiReady indicates Available/True.

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

    Args:
        timeout (int): Seconds to wait for ApiReady (connection ready).
        interval (int): Seconds between polls of OLSConfig status.

    Raises:
        ResourceWrongStatusException: If pods are not Running or connection
            does not become ready within ``timeout``.

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

    Args:
        timeout (int): Maximum seconds to wait without seeing a ready state.
        interval (int): Seconds between polls of OLSConfig status.

    Returns:
        bool: True if misconfiguration prevented reaching ready within ``timeout``.

    """
    return wait_for_ols_connection_state(
        expect_ready=False, timeout=timeout, interval=interval
    )


def wait_for_ols_config_status_after_apply(
    timeout=None,
    interval=None,
):
    """

    Poll until OLSConfig has non-empty ``status.conditions`` (operator reconciled).

    Used after applying a misconfigured OLSConfig so subsequent checks run against
    real status instead of a fixed sleep.

    Args:
        timeout (int, optional): Max seconds to poll. Defaults to
            ``constants.OLS_POST_MISCONFIG_APPLY_WAIT_SEC``.
        interval (int, optional): Seconds between polls. Defaults to
            ``constants.OLS_POST_MISCONFIG_POLL_INTERVAL_SEC``.

    Returns:
        bool: True if conditions appeared within ``timeout``, False otherwise.

    """
    timeout = timeout or constants.OLS_POST_MISCONFIG_APPLY_WAIT_SEC
    interval = interval or constants.OLS_POST_MISCONFIG_POLL_INTERVAL_SEC
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conditions = _get_ols_config_conditions()
            if conditions:
                log.info("OLSConfig status conditions present after apply.")
                return True
        except Exception as ex:
            log.debug("Waiting for OLSConfig status: %s", ex)
        time.sleep(interval)
    log.warning(
        "OLSConfig status conditions not populated within %s s; continuing checks.",
        timeout,
    )
    return False


def delete_ols_config_and_secret():
    """

    Delete the cluster OLSConfig and the watsonx API secret so that a new
    (e.g. misconfigured) config can be applied. Used before negative tests.

    Waits briefly after delete so subsequent ``oc create`` does not race the API server.

    """
    run_cmd(
        f"oc delete {constants.OLS_CONFIG_KIND} cluster -n {constants.OLS_OPERATOR_NAMESPACE} --ignore-not-found=true"
    )
    run_cmd(
        f"oc delete secret watsonx-api-keys -n {constants.OLS_OPERATOR_NAMESPACE} --ignore-not-found=true"
    )
    time.sleep(constants.OLS_CONFIG_DELETE_WAIT_SEC)


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
