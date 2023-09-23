import logging
from time import sleep
import pytest

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
    ms_provider_and_consumer_required,
    skipif_ms_provider_and_consumer,
    ms_consumer_required,
    skipif_bm,
    magenta_squad,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_all_pvcs
from ocs_ci.ocs.resources.pod import get_all_pods


log = logging.getLogger(__name__)


@magenta_squad
@tier1
@ignore_leftovers
class TestCreateScalePodsAndPvcsUsingKubeJob(ManageTest):
    """
    Test create scale pods and PVCs using a kube job
    """

    @pytest.fixture(autouse=True)
    def setup(self, request):
        self.orig_index = None

        def finalizer():
            if self.orig_index is not None:
                log.info("Switch back to the original context")
                config.switch_ctx(self.orig_index)

        request.addfinalizer(finalizer)

    @skipif_bm
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

    @ms_provider_and_consumer_required
    def test_create_scale_pods_and_pvcs_using_kube_job_ms(
        self, create_scale_pods_and_pvcs_using_kube_job
    ):
        """
        Test create scale pods and PVCs using a kube job with managed service
        """
        self.orig_index = config.cur_index

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

    @skipif_ms_provider_and_consumer
    @ms_consumer_required
    def test_create_scale_pods_and_pvcs_with_ms_consumer(
        self, create_scale_pods_and_pvcs_using_kube_job
    ):
        """
        Test create scale pods and PVCs using a kube job with MS consumer
        when we don't have a provider in the run
        """
        log.info("Start creating resources using kube job with MS consumer...")
        create_scale_pods_and_pvcs_using_kube_job()
        time_to_wait_for_io_running = 60
        log.info(
            f"Wait {time_to_wait_for_io_running} seconds for checking "
            f"that the IO running as expected"
        )
        sleep(time_to_wait_for_io_running)
        ceph_health_check()
        log.info(
            "The resources created successfully using the kube job with MS consumer"
        )


@magenta_squad
@tier1
@ignore_leftovers
@ms_provider_and_consumer_required
class TestCreateScalePodsAndPvcsUsingKubeJobWithMSConsumers(ManageTest):
    """
    Test create scale pods and PVCs using a kube job with MS consumers
    """

    def setup(self):
        self.orig_index = None
        self.scale_count = min(constants.SCALE_PVC_ROUND_UP_VALUE)
        self.pvc_per_pod_count = 5
        self.expected_pod_num = int(self.scale_count / self.pvc_per_pod_count)
        self.consumer_i_per_fio_scale = {}

    def check_scale_pods_and_pvcs_created_on_consumers(self):
        for consumer_i, fio_scale in self.consumer_i_per_fio_scale.items():
            config.switch_ctx(consumer_i)
            c_name = config.ENV_DATA.get("cluster_name")
            ocp_pvc = OCP(kind=constants.PVC, namespace=fio_scale.namespace)
            ocp_pvc.wait_for_resource(
                timeout=30,
                condition=constants.STATUS_BOUND,
                resource_count=self.scale_count,
            )
            log.info(f"All the PVCs were created successfully on the consumer {c_name}")

            ocp_pod = OCP(kind=constants.POD, namespace=fio_scale.namespace)
            ocp_pod.wait_for_resource(
                timeout=30,
                condition=constants.STATUS_COMPLETED,
                resource_count=self.expected_pod_num,
            )
            log.info(f"All the pods were created successfully on the consumer {c_name}")

        log.info("All the pods and PVCs were created successfully on the consumers")

    def check_pods_and_pvcs_deleted_on_consumers(self):
        for consumer_i, fio_scale in self.consumer_i_per_fio_scale.items():
            config.switch_ctx(consumer_i)
            c_name = config.ENV_DATA.get("cluster_name")

            pvc_objs = get_all_pvcs(fio_scale.namespace)["items"]
            assert not pvc_objs, "There are still remaining PVCs"
            log.info(f"All the PVCs deleted successfully on the consumer {c_name}")

            pod_objs = get_all_pods(fio_scale.namespace)
            assert not pod_objs, "There are still remaining pods"
            log.info(f"All the pods deleted successfully on the consumer {c_name}")

        log.info("All the pods and PVCs were deleted successfully on the consumers")

    def test_create_scale_pods_and_pvcs_with_ms_consumers(
        self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
    ):
        """
        Test create scale pods and PVCs using a kube job with MS consumers
        """
        self.orig_index = config.cur_index
        self.consumer_i_per_fio_scale = (
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers(
                scale_count=self.scale_count,
                pvc_per_pod_count=self.pvc_per_pod_count,
            )
        )
        assert config.cur_index == self.orig_index, "The current index has changed"

        config.switch_to_provider()
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
            config.switch_ctx(i)
            ceph_health_check()

        self.check_scale_pods_and_pvcs_created_on_consumers()
        log.info(
            "The scale pods and PVCs using a kube job with MS consumers created successfully"
        )

    def test_create_and_delete_scale_pods_and_pvcs_with_ms_consumers(
        self, create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
    ):
        """
        Test create and delete scale pods and PVCs using a kube job with MS consumers
        """
        self.orig_index = config.cur_index
        self.consumer_i_per_fio_scale = (
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers(
                scale_count=self.scale_count,
                pvc_per_pod_count=self.pvc_per_pod_count,
            )
        )
        assert config.cur_index == self.orig_index, "The current index has changed"

        config.switch_to_provider()
        time_to_wait_for_io_running = 120
        log.info(
            f"Wait {time_to_wait_for_io_running} seconds for checking "
            f"that the IO running as expected"
        )
        sleep(time_to_wait_for_io_running)
        ceph_health_check()

        self.check_scale_pods_and_pvcs_created_on_consumers()

        log.info("Clean up the pods and PVCs from all consumers")
        for consumer_i, fio_scale in self.consumer_i_per_fio_scale.items():
            config.switch_ctx(consumer_i)
            fio_scale.cleanup()

        self.check_pods_and_pvcs_deleted_on_consumers()

        log.info("Checking the Ceph Health on the consumers")
        consumer_indexes = config.get_consumer_indexes_list()
        for i in consumer_indexes:
            config.switch_ctx(i)
            ceph_health_check()

        log.info(
            "The scale pods and PVCs using a kube job with MS consumers "
            "created and deleted successfully"
        )
