import pytest

from logging import getLogger

from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_noobaa_core_pod,
    get_noobaa_endpoint_pods,
    get_noobaa_operator_pod,
    get_pod_logs,
)
from ocs_ci.framework.testlib import tier2
from ocs_ci.ocs.constants import (
    CONFIGMAP,
    NOOBAA_CONFIGMAP,
    POD,
    NOOBAA_APP_LABEL,
    STATUS_RUNNING,
    OPENSHIFT_STORAGE_NAMESPACE,
)

log = getLogger(__name__)
NAMESPACE = config.ENV_DATA.get("cluster_namespace", OPENSHIFT_STORAGE_NAMESPACE)
LOG_LEVEL_DEFAULT = "default_level"
LOG_LEVEL_WARN = "warn"
LOG_LEVEL_ALL = "all"


def get_noobaa_cfg_log_level(cfgmap: OCP) -> str:
    """
    Gets noobaa current log level from its configmap

    Args:
        cfgmap (OCP): OCP object of kind "configmap"

    Returns:
        str: value of NOOBAA_LOG_LEVEL

    """
    log_level = cfgmap.get(NOOBAA_CONFIGMAP).get("data").get("NOOBAA_LOG_LEVEL")
    log.info(f"Noobaa current log level from configmap: {log_level}")
    return log_level


def set_noobaa_cfg_log_level(cfgmap: OCP, log_level: str) -> None:
    """
    Patch noobaa configmap, to set log level

    Args:
        cfgmap (OCP): OCP object of kind "configmap"
        log_level (str): value of NOOBAA_LOG_LEVEL to set

    """
    cmd = f'{{"data": {{"NOOBAA_LOG_LEVEL": "{log_level}"}}}}'
    log.info(f"Setting noobaa's log level to {log_level} at {NOOBAA_CONFIGMAP}:")
    cfgmap.patch(resource_name=NOOBAA_CONFIGMAP, params=cmd, format_type="merge")


def check_noobaa_logs(pod_name: str, text_to_search: str) -> bool:
    """
    Check if given text can be found in noobaa pod log

    Args:
        pod_name (str): pod name to check its logs
        text_to_search (str): text to search for in given pod log

    Returns:
        bool: True if text is found in logs, False otherwise

    """
    logs = get_pod_logs(pod_name)
    if text_to_search in logs:
        return True
    return False


@tier2
@pytest.mark.bugzilla("1932846")
@pytest.mark.polarion_id("OCS-4863")
class TestNoobaaLogLevel:
    """
    Test optional Noobaa (MCG) log level reduction, as validation of BZ-1932846

        1. Check logs on default level, make sure that logs has reduced
        2. Change log level to "All"
        3. Make sure that logs now contains addional information

    """

    cfgmap = OCP(namespace=NAMESPACE, kind=CONFIGMAP, resource_name=NOOBAA_CONFIGMAP)
    pod_obj = OCP(namespace=NAMESPACE, kind=POD, selector=NOOBAA_APP_LABEL)

    @pytest.fixture(scope="class")
    def verify_log_default_level(self) -> None:
        log_level = get_noobaa_cfg_log_level(self.cfgmap)
        if log_level != LOG_LEVEL_DEFAULT:
            set_noobaa_cfg_log_level(cfgmap=self.cfgmap, log_level=LOG_LEVEL_DEFAULT)
        self.pod_obj.wait_for_resource(condition=STATUS_RUNNING)

    def test_mcg_core_log_default_level(self, verify_log_default_level) -> None:
        noobaa_core_pod = get_noobaa_core_pod()
        noobaa_core_name = noobaa_core_pod.data.get("metadata").get("name")

        # When log level is reduced, we should only observe '[0]' level lines
        assert not check_noobaa_logs(noobaa_core_name, "[1]")

    def test_noobaa_operator_log_default_level(self, verify_log_default_level) -> None:
        noobaa_operator_pod = get_noobaa_operator_pod()
        noobaa_operator_name = noobaa_operator_pod.data.get("metadata").get("name")
        assert not check_noobaa_logs(noobaa_operator_name, "level=info")

    def test_noobaa_endpoint_log_default_level(self, verify_log_default_level) -> None:
        noobaa_endpoint_pod = get_noobaa_endpoint_pods()[0]
        noobaa_endpoint_name = noobaa_endpoint_pod.data.get("metadata").get("name")
        assert not check_noobaa_logs(noobaa_endpoint_name, "[1]")

    def test_mcg_core_log_level_all(self, change_the_noobaa_log_level) -> None:
        noobaa_core_pod = get_noobaa_core_pod()
        noobaa_core_name = noobaa_core_pod.data.get("metadata").get("name")
        assert check_noobaa_logs(noobaa_core_name, "[1]")

    def test_noobaa_operator_log_level_all(self, change_the_noobaa_log_level) -> None:
        noobaa_operator_pod = get_noobaa_operator_pod()
        noobaa_operator_name = noobaa_operator_pod.data.get("metadata").get("name")
        assert check_noobaa_logs(noobaa_operator_name, "level=info")

    def test_noobaa_endpoint_log_level_all(self, change_the_noobaa_log_level) -> None:
        noobaa_endpoint_pod = get_noobaa_endpoint_pods()[0]
        noobaa_endpoint_name = noobaa_endpoint_pod.data.get("metadata").get("name")
        assert check_noobaa_logs(noobaa_endpoint_name, "[1]")
