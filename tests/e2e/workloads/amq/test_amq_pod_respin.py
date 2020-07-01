import logging
import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.framework.testlib import (
    E2ETest, workloads, ignore_leftovers
)
from tests.helpers import default_storage_class
from tests.disruption_helpers import Disruptions
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.pod import get_all_pods


log = logging.getLogger(__name__)


def respin_amq_app_pod(kafka_namespace):
    """
    Respin amq pod

    Args:
        kafka_namespace (str): Namespace for kafka

    """
    pod = ocp.OCP(kind=constants.POD, namespace=kafka_namespace)
    pod_obj_list = get_all_pods(namespace=kafka_namespace)
    for pod_obj in pod_obj_list:
        pod_obj.delete()
        assert pod.wait_for_resource(
            condition='Running', resource_count=len(pod_obj_list), timeout=300
        )


@ignore_leftovers
@workloads
class TestAMQPodRespin(E2ETest):
    """
    Test running open messages on amq cluster when backed by rbd
    and with Ceph pods respin,  amq pod respin

    """

    @pytest.fixture()
    def amq_setup(self, amq_factory_fixture):
        """
        Creates amq cluster and run benchmarks

        """
        sc_name = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)
        self.amq_workload_dict = templating.load_yaml(constants.AMQ_WORKLOAD_YAML)
        self.amq, self.thread, self.queue = amq_factory_fixture(
            sc_name=sc_name.name, tiller_namespace="tiller",
            amq_workload_yaml=self.amq_workload_dict, run_in_bg=True
        )

    @pytest.mark.parametrize(
        argnames=[
            "pod_name"
        ],
        argvalues=[
            pytest.param(
                *['osd'], marks=pytest.mark.polarion_id("OCS-1276")
            ),
            pytest.param(
                *['mon'], marks=pytest.mark.polarion_id("OCS-1275")
            ),
            pytest.param(
                *['mgr'], marks=pytest.mark.polarion_id("OCS-2222")
            ),
            pytest.param(
                *['rbdplugin'], marks=pytest.mark.polarion_id("OCS-1277")
            ),
            pytest.param(
                *['rbdplugin_provisioner'], marks=pytest.mark.polarion_id("OCS-1283")
            ),
            pytest.param(
                *['operator'], marks=pytest.mark.polarion_id("OCS-2223")
            ),
            pytest.param(
                *['amq'], marks=pytest.mark.polarion_id("OCS-1280")
            )
        ]
    )
    @pytest.mark.usefixtures(amq_setup.__name__)
    def test_run_amq_respin_pod(self, pod_name):
        """
        Test amq workload when spinning ceph pods
        and restarting amq pods

        """
        # Respin relevant pod
        if pod_name == 'amq':
            respin_amq_app_pod(kafka_namespace=constants.AMQ_NAMESPACE)
        else:
            log.info(f"Respin Ceph pod {pod_name}")
            disruption = Disruptions()
            disruption.set_resource(resource=f'{pod_name}')
            disruption.delete_resource()

        # Validate and collect the results
        log.info("Wait till amq benchmark run complete")
        self.thread.join()
        result = self.queue.get()
        log.info("Validate amq benchmark is run completely")
        assert self.amq.validate_amq_benchmark(
            result=result, amq_workload_yaml=self.amq_workload_dict
        ) is not None, (
            "Benchmark did not completely run or might failed in between"
        )
