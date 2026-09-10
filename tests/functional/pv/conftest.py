import logging

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.utility import version

log = logging.getLogger(__name__)


@pytest.fixture
def test_resources_cleanup(request):
    """
    Track dynamically created pods, PVCs, and snapshots and delete them during
    teardown. If any resource fails to delete (leaving leftovers on the cluster),
    the teardown fails loudly instead of silently passing.

    Returns:
        dict: buckets ``{"pods": [], "pvcs": [], "snapshots": []}`` for the test
            to append the resource objects it creates.

    """
    resources = {"pods": [], "pvcs": [], "snapshots": []}

    def resource_teardown():
        log.info("Cleaning up test pods, PVCs, and snapshots")
        cleanup_errors = (CommandFailed, TimeoutError, TimeoutExpiredError)
        leftovers = []

        # Delete in dependency order: pods first (they hold the PVCs), then PVCs,
        # then snapshots.
        for kind in ("pods", "pvcs", "snapshots"):
            for obj in resources[kind]:
                name = getattr(obj, "name", "unknown")
                try:
                    obj.delete()
                except cleanup_errors as ex:
                    log.error(f"Failed to delete {kind[:-1]} {name}: {ex}")
                    leftovers.append(f"{kind[:-1]}/{name}")
                except Exception as ex:
                    log.exception(f"Unexpected error deleting {kind[:-1]} {name}: {ex}")
                    leftovers.append(f"{kind[:-1]}/{name}")

        assert not leftovers, (
            "Teardown failed to delete resources; cluster may have leftovers: "
            f"{', '.join(leftovers)}"
        )

    request.addfinalizer(resource_teardown)
    return resources


def pytest_collection_modifyitems(items):
    """
    Skip tests in a directory based on conditions

    Args:
        items: list of collected tests

    """
    ocs_version = version.get_semantic_ocs_version_from_config()

    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            if "functional/pv/pvc_snapshot" in str(item.fspath) and (
                ocs_version < version.VERSION_4_11
            ):
                log.debug(
                    f"Test {item} is removed from the collected items. PVC snapshot is not supported on"
                    f" {config.ENV_DATA['platform'].lower()} with ODF < 4.11 due to the bug 2069367"
                )
                items.remove(item)
