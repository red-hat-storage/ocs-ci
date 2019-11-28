import logging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers, order_pre_upgrade, order_post_upgrade
)

log = logging.getLogger(__name__)


@order_pre_upgrade
@ignore_leftovers
def test_pre_upgrade_pods(pre_upgrade_block_pods, pre_upgrade_filesystem_pods):
    """
    Confirm that there are pods created before upgrade.
    """
    assert pre_upgrade_block_pods
    assert pre_upgrade_filesystem_pods


@order_post_upgrade
@pytest.mark.polarion_id("OCS-1862")
def test_pod_io(
    pre_upgrade_filesystem_pods,
    post_upgrade_filesystem_pods
):
    """
    Test IO on multiple pods at the same time.
    """
    log.info(f"Pods created before upgrade: {pre_upgrade_filesystem_pods}")
    log.info(f"Pods created after upgrade: {post_upgrade_filesystem_pods}")
    pods = pre_upgrade_filesystem_pods + post_upgrade_filesystem_pods

    # Run IOs on all pods
    with ThreadPoolExecutor() as executor:
        for pod in pods:
            log.info(f"Running fio on {pod.name}")
            executor.submit(
                pod.run_io(
                    storage_type='fs',
                    size='1GB',
                    runtime=30,
                )
            )
    for pod in pods:
        log.info(f"Waiting for results from {pod.name}")
        fio_result = pod.get_fio_results()
        reads = fio_result.get('jobs')[0].get('read').get('iops')
        writes = fio_result.get('jobs')[0].get('write').get('iops')
        assert reads, f"There are no reads from pod {pod.name}"
        assert writes, f"There are no writes from pod {pod.name}"
