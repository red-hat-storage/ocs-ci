import logging
from time import sleep

from ocs_ci.ocs.cluster import ceph_health_check
from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    tier1,
    ManageTest,
    ignore_leftovers,
)
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    ipi_deployment_required,
    managed_service_required,
)

log = logging.getLogger(__name__)


@tier1
@ignore_leftovers
class TestCreateScalePodsAndPvcsUsingKubeJob(ManageTest):
    """
    Test create scale pods and PVCs using a kube job
    """

    @skipif_external_mode
    @ipi_deployment_required
    def test_create_scale_pods_and_pvcs_using_kube_job(
        self, create_scale_pods_and_pvcs_using_kube_job
    ):
        """
        Test create scale pods and PVCs using a kube job
        """
        log.info("Start creating resources using kube job...")
        create_scale_pods_and_pvcs_using_kube_job()
        time_to_wait_for_io_running = 120
        log.info(
            f"Wait {time_to_wait_for_io_running} seconds for checking "
            f"that the IO running as expected"
        )
        sleep(time_to_wait_for_io_running)
        ceph_health_check()
        log.info("The resources created successfully using the kube job")

    @managed_service_required
    def test_create_scale_pods_and_pvcs_using_kube_job_ms(
        self, create_scale_pods_and_pvcs_using_kube_job
    ):
        """
        Test create scale pods and PVCs using a kube job with managed service
        """
        config.switch_to_consumer()
        log.info("Start creating resources using kube job...")
        create_scale_pods_and_pvcs_using_kube_job()
        ceph_health_check()

        log.info("Switch to the provider")
        config.switch_to_provider()
        time_to_wait_for_io_running = 120
        log.info(
            f"Wait {time_to_wait_for_io_running} seconds for checking "
            f"that the IO running as expected"
        )
        sleep(time_to_wait_for_io_running)
        ceph_health_check()

        log.info("Switch back to the consumer")
        config.switch_to_consumer()
        log.info("The resources created successfully using the kube job")
