import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import red_squad, mcg
from ocs_ci.framework.testlib import (
    bugzilla,
    ignore_leftovers,
    ManageTest,
    skipif_ocs_version,
    skipif_vsphere_ipi,
    tier4b,
)
from ocs_ci.helpers.sanity_helpers import Sanity, SanityExternalCluster
from ocs_ci.helpers.helpers import (
    storagecluster_independent_check,
)
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.exceptions import TimeoutExpiredError, CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_pod_node,
    get_noobaa_pods,
    wait_for_pods_to_be_running,
    wait_for_pods_to_be_in_statuses,
)
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@red_squad
@mcg
@tier4b
@bugzilla("1853638")
@ignore_leftovers
@skipif_vsphere_ipi
@skipif_ocs_version("<4.9")
class TestNoobaaSTSHostNodeFailure(ManageTest):
    """
    Test to verify NooBaa Statefulset pods recovers in case of a node failure

    """

    labels_map = {
        constants.NOOBAA_CORE_STATEFULSET: constants.NOOBAA_CORE_POD_LABEL,
        constants.NOOBAA_DB_STATEFULSET: constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
        constants.NOOBAA_OPERATOR_DEPLOYMENT: constants.NOOBAA_OPERATOR_POD_LABEL,
    }

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        if storagecluster_independent_check():
            self.sanity_helpers = SanityExternalCluster()
        else:
            self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames=["noobaa_sts", "respin_noobaa_operator"],
        argvalues=[
            pytest.param(
                *[constants.NOOBAA_CORE_STATEFULSET, False],
                marks=pytest.mark.polarion_id("OCS-2672"),
            ),
            pytest.param(
                *[constants.NOOBAA_DB_STATEFULSET, False],
                marks=pytest.mark.polarion_id("OCS-2668"),
            ),
            pytest.param(
                *[constants.NOOBAA_CORE_STATEFULSET, True],
                marks=pytest.mark.polarion_id("OCS-2669"),
            ),
            pytest.param(
                *[constants.NOOBAA_DB_STATEFULSET, True],
                marks=pytest.mark.polarion_id("OCS-2670"),
            ),
        ],
    )
    def test_noobaa_sts_host_node_failure(
        self,
        noobaa_sts,
        respin_noobaa_operator,
        mcg_obj,
        bucket_factory,
        nodes,
        node_restart_teardown,
    ):
        """
        Test case to fail node where NooBaa Statefulset pod (noobaa-core, noobaa-db)
        is hosted and verify the pod is rescheduled on a healthy node

        """
        executor = ThreadPoolExecutor(max_workers=1)
        pod_obj = OCP(
            kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
        )

        # Get noobaa statefulset pod and node where it is hosted
        noobaa_sts_pod = get_noobaa_pods(noobaa_label=self.labels_map[noobaa_sts])[0]
        noobaa_sts_pod_node = get_pod_node(noobaa_sts_pod)
        log.info(f"{noobaa_sts_pod.name} is running on {noobaa_sts_pod_node.name}")

        # Get the NooBaa operator pod and node where it is hosted
        # Check if NooBaa operator and statefulset pod are hosted on same node
        noobaa_operator_pod = get_noobaa_pods(
            noobaa_label=self.labels_map[constants.NOOBAA_OPERATOR_DEPLOYMENT]
        )[0]
        noobaa_operator_pod_node = get_pod_node(noobaa_operator_pod)
        log.info(
            f"{noobaa_operator_pod.name} is running on {noobaa_operator_pod_node.name}"
        )
        if noobaa_sts_pod_node.name == noobaa_operator_pod_node.name:
            operator_on_same_node = True
            log.info(
                f"{noobaa_sts_pod.name} and {noobaa_operator_pod.name} are running on same node."
            )
        else:
            operator_on_same_node = False
            log.info(
                f"{noobaa_sts_pod.name} and {noobaa_operator_pod.name} are running on different node."
            )

        # Stop the node
        log.info(
            f"Stopping {noobaa_sts_pod_node.name} where {noobaa_sts_pod.name} is hosted"
        )
        stop_thread = executor.submit(nodes.stop_nodes, nodes=[noobaa_sts_pod_node])
        node.wait_for_nodes_status(
            node_names=[noobaa_sts_pod_node.name], status=constants.NODE_NOT_READY
        )

        # Disrupt NooBaa operator
        if respin_noobaa_operator:
            try:
                noobaa_operator_pod.delete(force=True)
            except CommandFailed as e:
                log.warning(
                    f"Failed to delete the noobaa operator pod due to the exception: {str(e)}"
                )

        # Check result of 'stop_thread'
        stop_thread.result()

        # Wait for NooBaa operator pod to reach terminating state or to be deleted
        # if on same node or respun
        if operator_on_same_node or respin_noobaa_operator:
            assert wait_for_pods_to_be_in_statuses(
                [constants.STATUS_TERMINATING],
                pod_names=[noobaa_operator_pod.name],
                raise_pod_not_found_error=False,
                timeout=420,
                sleep=20,
            )

        # Wait for NooBaa operator pod to reach running state
        pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=self.labels_map[constants.NOOBAA_OPERATOR_DEPLOYMENT],
            resource_count=1,
            timeout=420,
            sleep=20,
        )

        # Verify NooBaa statefulset pod reschedules on another node
        try:
            for pod_list in TimeoutSampler(
                60,
                3,
                get_noobaa_pods,
                noobaa_label=self.labels_map[noobaa_sts],
            ):
                if len(pod_list) == 1:
                    pod_node = get_pod_node(pod_list[0])
                    if pod_node.name != noobaa_sts_pod_node.name:
                        log.info(
                            f"{pod_list[0].name} has been rescheduled on {pod_node.name}"
                        )
                        break
                    log.info(f"Waiting for {noobaa_sts_pod.name} pod to be rescheduled")
        except TimeoutExpiredError:
            raise TimeoutExpiredError(
                f"{noobaa_sts_pod.name} pod not rescheduled within 60 seconds"
            )

        # Wait for rescheduled pod to reach Running state.
        # For noobaa-db pod which is attached to a PV it may take more time (~8 minutes)
        # until the new pod can attach to the PV
        pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=self.labels_map[noobaa_sts],
            resource_count=1,
            timeout=800 if noobaa_sts == constants.NOOBAA_DB_STATEFULSET else 60,
            sleep=30 if noobaa_sts == constants.NOOBAA_DB_STATEFULSET else 3,
        )

        # Start the node
        log.info(
            f"Starting {noobaa_sts_pod_node.name} where {noobaa_sts_pod.name} was hosted"
        )
        nodes.start_nodes(nodes=[noobaa_sts_pod_node])
        node.wait_for_nodes_status(
            node_names=[noobaa_sts_pod_node.name], status=constants.NODE_READY
        )

        log.info("Wait for all pods to be in running state")
        wait_for_pods_to_be_running(timeout=300)

        # Check cluster health
        self.sanity_helpers.health_check(tries=40)

        # Creates bucket then writes, reads and deletes objects
        # TODO: Reduce timeout in future versions once 2028559 is fixed
        self.sanity_helpers.obc_put_obj_create_delete(
            mcg_obj, bucket_factory, timeout=900
        )
