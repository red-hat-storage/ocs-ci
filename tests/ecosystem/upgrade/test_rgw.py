import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    pre_upgrade,
    post_upgrade,
)
from ocs_ci.ocs.mcg_workload import wait_for_active_pods
from ocs_ci.ocs.resources import pod

log = logging.getLogger(__name__)


@pre_upgrade
@pytest.mark.bugzilla("1904171")
def test_start_upgrade_rgw_io(rgw_deployments, rgw_workload_job):
    """
    Confirm that there is RGW workload job running before upgrade.

    """
    # wait a few seconds for fio job to start
    assert wait_for_active_pods(
        rgw_workload_job, 1, timeout=20
    ), f"Job {rgw_workload_job.name} doesn't have any running pod"


@post_upgrade
@pytest.mark.bugzilla("1904171")
def test_upgrade_rgw_io(rgw_deployments, rgw_workload_job):
    """
    Confirm that there is RGW workload job running after upgrade.

    """
    assert wait_for_active_pods(
        rgw_workload_job, 1
    ), f"Job {rgw_workload_job.name} doesn't have any running pod"
    job_pods = pod.get_pods_having_label(
        f"job-name={rgw_workload_job.name}", rgw_workload_job.namespace
    )
    for job_pod in job_pods:
        pod_logs = pod.get_pod_logs(
            job_pod["metadata"]["name"], namespace=job_pod["metadata"]["namespace"]
        )
        log.debug(f"Logs from job pod {job_pod['metadata']['name']}: {pod_logs}")
        assert "Unavailable" not in pod_logs
