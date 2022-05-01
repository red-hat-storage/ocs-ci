import pytest
import logging
import threading
from datetime import datetime, timedelta

from ocs_ci.framework.testlib import (
    E2ETest,
)
import ocs_ci.helpers.omadops_helpers as omadops_helpers
import tests.OMADOps.conftest as omadops_config
from ocs_ci.ocs.longevity import start_app_workload

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def set_factories(
    pvc_factory, pvc_clone_factory, snapshot_factory, snapshot_restore_factory
):
    omadops_config.CONFIG_VARS["pvc_factory"] = pvc_factory
    omadops_config.CONFIG_VARS["pvc_clone_factory"] = pvc_clone_factory
    omadops_config.CONFIG_VARS["snapshot_factory"] = snapshot_factory
    omadops_config.CONFIG_VARS["snapshot_restore_factory"] = snapshot_restore_factory


@pytest.fixture(autouse=True)
def set_end_time():
    """
    Setting up the end time of the test to be in the future.
    Configured in tests.OMADOps.config.EXECUTION_TIME_HOURS
    """
    omadops_config.END_TIME = datetime.now() + timedelta(
        hours=omadops_config.EXECUTION_TIME_HOURS
    )
    # For debug:
    omadops_config.END_TIME = datetime.now() + timedelta(minutes=60)
    log.info(f"Ending test at {omadops_config.END_TIME}")


@pytest.fixture(autouse=True)
def start_app_workload_fixture(request):
    # bg IOs
    start_app_workload(
        request=request,
        workloads_list=omadops_config.USER_OPS,
        run_time=omadops_config.EXECUTION_TIME_HOURS * 60,
        delay=60,
    )


@pytest.mark.skipif(
    not omadops_config.ADMIN_OPS,
    reason="Admin Ops list is empty",
)
class TestOMADOps(E2ETest):
    def test_omadops(self, project_factory):
        # USER OPS
        thread1 = threading.Thread(
            target=omadops_helpers.run_user_ops,
            name="run_user_ops",
            args=(project_factory,),
        )

        # ADMIN OPS
        thread2 = threading.Thread(
            target=omadops_helpers.run_admin_ops, name="run_admin_ops", args=(self,)
        )

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()
