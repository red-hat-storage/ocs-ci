import logging
import pytest

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import (
    ignore_leftovers, order_pre_upgrade, order_post_upgrade
)

log = logging.getLogger(__name__)


@order_pre_upgrade
@ignore_leftovers
def test_start_pre_upgrade_pod_io(pre_upgrade_pods_running_io):
    """
    Confirm that there are pods created before upgrade.
    """
    assert pre_upgrade_pods_running_io


@order_post_upgrade
@pytest.mark.polarion_id("OCS-1862")
def test_pod_io(
    pre_upgrade_filesystem_pods,
    post_upgrade_filesystem_pods,
    pre_upgrade_block_pods,
    post_upgrade_block_pods,
    upgrade_fio_file
):
    """
    Test IO on multiple pods at the same time and finish IO on pods that were
    created before upgrade.
    """
    log.info(
        f"Pods using filesystem created before upgrade: "
        f"{pre_upgrade_filesystem_pods}"
    )
    log.info(
        f"Pods using filesystem created after upgrade: "
        f"{post_upgrade_filesystem_pods}"
    )
    log.info(
        f"Pods using block device created before upgrade: "
        f"{pre_upgrade_block_pods}"
    )
    log.info(
        f"Pods using block device created after upgrade: "
        f"{post_upgrade_block_pods}"
    )

    log.warning('Stopping fio for pre upgrade pods')
    upgrade_fio_file.write('stop')

    # Run IOs on filesystem pods
    log.info('Starting fio on post upgrade fs pods')
    pods = pre_upgrade_filesystem_pods + post_upgrade_filesystem_pods
    with ThreadPoolExecutor() as executor:
        for pod in pods:
            log.info(f"Running fio on {pod.name}")
            executor.submit(
                pod.run_io(
                    storage_type='fs',
                    size='1GB',
                    runtime=20,
                )
            )
    for pod in pods:
        log.info(f"Waiting for results from {pod.name}")
        fio_result = pod.get_fio_results()
        reads = fio_result.get('jobs')[0].get('read').get('iops')
        writes = fio_result.get('jobs')[0].get('write').get('iops')
        assert reads, f"There are no reads from pod {pod.name}"
        assert writes, f"There are no writes from pod {pod.name}"

    # Run IOs on block device pods
    log.info('Starting fio on post upgrade block pods')
    pods = pre_upgrade_block_pods + post_upgrade_block_pods
    with ThreadPoolExecutor() as executor:
        for pod in pods:
            log.info(f"Running fio on {pod.name}")
            executor.submit(
                pod.run_io(
                    storage_type='block',
                    size='1024MB',
                    runtime=20,
                )
            )
    for pod in pods:
        log.info(f"Waiting for results from {pod.name}")
        fio_result = pod.get_fio_results()
        reads = fio_result.get('jobs')[0].get('read').get('iops')
        writes = fio_result.get('jobs')[0].get('write').get('iops')
        assert reads, f"There are no reads from pod {pod.name}"
        assert writes, f"There are no writes from pod {pod.name}"
