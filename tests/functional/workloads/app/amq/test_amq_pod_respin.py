import logging
import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.framework.pytest_customization.marks import magenta_squad
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


def respin_amq_app_pod(kafka_namespace, pod_pattern):
    """
    Respin amq pod

    Args:
        kafka_namespace (str): Namespace for kafka
        pod_pattern (str): The pattern for the pod

    """
    pod_obj = ocp.OCP(kind=constants.POD, namespace=kafka_namespace)
    pod_obj_list = get_all_pods(namespace=kafka_namespace)
    for pod in TimeoutSampler(
        300, 10, get_pod_name_by_pattern, pod_pattern, kafka_namespace
    ):
        try:
            if pod is not None:
                pod_obj.delete(resource_name=pod[0])
                assert pod_obj.wait_for_resource(
                    condition="Running", resource_count=len(pod_obj_list), timeout=300
                )
                break
        except IndexError as ie:
            log.error(" pod doesn't exist")
            raise ie


@magenta_squad
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
        self.amq, self.threads = amq_factory_fixture(sc_name=sc_name.name)

        # Initialize Sanity instance
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["pod_name"],
        argvalues=[
            pytest.param(*["osd"], marks=pytest.mark.polarion_id("OCS-1276")),
            pytest.param(*["mon"], marks=pytest.mark.polarion_id("OCS-1275")),
            pytest.param(*["mgr"], marks=pytest.mark.polarion_id("OCS-2222")),
            pytest.param(*["rbdplugin"], marks=pytest.mark.polarion_id("OCS-1277")),
            pytest.param(
                *["rbdplugin_provisioner"], marks=pytest.mark.polarion_id("OCS-1283")
            ),
            pytest.param(*["operator"], marks=pytest.mark.polarion_id("OCS-2223")),
            pytest.param(*["amq"], marks=pytest.mark.polarion_id("OCS-1280")),
        ],
    )
    @pytest.mark.usefixtures(amq_setup.__name__)
    def test_run_amq_respin_pod(self, pod_name):
        """
        Test amq workload when spinning ceph pods
        and restarting amq pods

        """
        # Respin relevant pod
        if pod_name == "amq":
            pod_pattern_list = [
                "cluster-operator",
                "my-cluster-kafka",
                "my-cluster-zookeeper",
                "my-connect-cluster-connect",
                "my-bridge-bridge",
            ]
            for pod_pattern in pod_pattern_list:
                respin_amq_app_pod(
                    kafka_namespace=constants.AMQ_NAMESPACE, pod_pattern=pod_pattern
                )
        else:
            log.info(f"Respin Ceph pod {pod_name}")
            disruption = Disruptions()
            disruption.set_resource(resource=f"{pod_name}")
            disruption.delete_resource()

            # Validate the results
            log.info("Validate message run completely")
            for thread in self.threads:
                thread.result(timeout=1800)

        # Perform cluster and Ceph health checks
        self.sanity_helpers.health_check(tries=40)
