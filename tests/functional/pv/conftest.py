import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.utility import version

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items):
    """
    Skip tests in a directory based on conditions

    Args:
        items: list of collected tests

    """
    ocs_version = version.get_semantic_ocs_version_from_config()

    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if "manage/pv_services/pvc_snapshot" in str(item.fspath) and (
                ocs_version < version.VERSION_4_11
            ):
                log.debug(
                    f"Test {item} is removed from the collected items. PVC snapshot is not supported on"
                    f" {config.ENV_DATA['platform'].lower()} with ODF < 4.11 due to the bug 2069367"
                )

                items.remove(item)

                pod_obj.pvc = pvc_obj
                pods_dc.append(pod_obj) if deployment_config else pods.append(pod_obj)

        # Get pod objects if deployment_config is True
        # pods_dc will be an empty list if deployment_config is False
        for pod_dc in pods_dc:
            pod_objs = pod.get_all_pods(
                namespace=pvcs[0].project.namespace,
                selector=[pod_dc.name],
                selector_label="name",
            )
            for pod_obj in pod_objs:
                pod_obj.pvc = pod_dc.pvc
            pods.extend(pod_objs)

        log.info(
            f"Created {len(pvcs_cephfs)} cephfs PVCs and {len(pvcs_rbd)} rbd "
            f"PVCs. Created {len(pods)} pods. "
        )
        return pvcs, pods

    return factory


def pytest_collection_modifyitems(items):
    """
    A pytest hook to skip certain tests when running on
    openshift dedicated platform
    Args:
        items: list of collected tests
    """
    # Skip the below test till node implementation completed for ODF-MS platform
    skip_till_node_implement = [
        "test_rwo_pvc_fencing_node_short_network_failure",
        "test_rwo_pvc_fencing_node_prolonged_network_failure",
        "test_worker_node_restart_during_pvc_clone",
        "test_rwo_pvc_fencing_node_prolonged_and_short_network_failure",
    ]
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            for testname in skip_till_node_implement:
                if testname in str(item):
                    log.info(
                        f"Test {item} is removed from the collected items"
                        f" till node implementation is in place"
                    )
                    items.remove(item)
                    break

