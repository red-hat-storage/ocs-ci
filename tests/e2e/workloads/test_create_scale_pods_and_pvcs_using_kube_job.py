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


@tier1
@ignore_leftovers
@managed_service_required
class TestCreateScalePodsAndPvcsUsingKubeJobWithMSConsumers(ManageTest):
    """
    Test create scale pods and PVCs using a kube job with MS consumers
    """

    def test_create_scale_pods_and_pvcs_with_ms_consumers(
        self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
    ):
        """
        Test create scale pods and PVCs using a kube job with MS consumers
        """
        config.switch_to_provider()
        create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers()
        assert (
            config.cur_index == config.get_provider_index()
        ), "The current index has changed"

        time_to_wait_for_io_running = 120
        log.info(
            f"Wait {time_to_wait_for_io_running} seconds for checking "
            f"that the IO running as expected"
        )
        sleep(time_to_wait_for_io_running)
        ceph_health_check()

        log.info("Checking the Ceph Health on the consumers")
        consumer_indexes = config.get_consumer_indexes_list()
        for i in consumer_indexes:
            config.switch_to_consumer(i)
            ceph_health_check()

        log.info(
            "The scale pods and PVCs using a kube job with MS consumers created successfully"
        )

    def test_create_and_delete_scale_pods_and_pvcs_with_ms_consumers(
        self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
    ):
        """
        Test create and delete scale pods and PVCs using a kube job with MS consumers
        """
        config.switch_to_provider()
        consumer_i_per_fio_scale = (
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers()
        )
        assert (
            config.cur_index == config.get_provider_index()
        ), "The current index has changed"

        time_to_wait_for_io_running = 120
        log.info(
            f"Wait {time_to_wait_for_io_running} seconds for checking "
            f"that the IO running as expected"
        )
        sleep(time_to_wait_for_io_running)
        ceph_health_check()

        log.info("Clean up the pods and PVCs from all consumers")
        for consumer_i, fio_scale in consumer_i_per_fio_scale.items():
            config.switch_to_consumer(consumer_i)
            fio_scale.cleanup()

        log.info("Checking the Ceph Health on the consumers")
        consumer_indexes = config.get_consumer_indexes_list()
        for i in consumer_indexes:
            config.switch_to_consumer(i)
            ceph_health_check()

        log.info(
            "The scale pods and PVCs using a kube job with MS consumers "
            "created and deleted successfully"
        )
