import json
import logging
import pytest
import time
from semantic_version import Version

from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier4c,
    skipif_ocs_version,
    ignore_leftovers,
)
from ocs_ci.helpers.helpers import mon_pods_running_on_same_node
from ocs_ci.ocs.constants import (
    ROOK_CEPH_MON_ENDPOINTS,
    CONFIGMAP,
    OPENSHIFT_STORAGE_NAMESPACE,
    STATUS_RUNNING,
    STATUS_PENDING,
    MON_APP_LABEL,
    DEPLOYMENT,
    POD,
    OPERATOR_LABEL,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError, ResourceWrongStatusException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_operator_pods,
    get_pod_node,
    delete_pods,
)
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.cluster import is_lso_cluster
from ocs_ci.framework import config

log = logging.getLogger(__name__)
POD_OBJ = OCP(kind=POD, namespace=OPENSHIFT_STORAGE_NAMESPACE)


@brown_squad
@tier4c
@ignore_leftovers
@skipif_ocs_version("<4.8")
@pytest.mark.polarion_id("OCS-2593")
@pytest.mark.bugzilla("1974204")
class TestMultipleMonPodsStaysOnSameNode(ManageTest):
    """
    Verify multiple mon pods stays on same node
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Verifies cluster is healthy
        """
        mon_pod = get_mon_pods()

        def finalizer():

            try:

                # Validate all mon pods are running
                log.info("Validate all mons are up and running")
                POD_OBJ.wait_for_resource(
                    condition=STATUS_RUNNING,
                    selector=MON_APP_LABEL,
                    resource_count=len(mon_pod),
                )
                log.info("All mons are up and running")

            except (TimeoutExpiredError, ResourceWrongStatusException) as ex:
                log.error(f"{ex}")
                # Restart operator
                operator_pod_obj = get_operator_pods()
                delete_pods(pod_objs=operator_pod_obj)

                # Wait untill mon pod recovery
                POD_OBJ.wait_for_resource(
                    condition=STATUS_RUNNING,
                    selector=MON_APP_LABEL,
                    resource_count=len(mon_pod),
                    timeout=3600,
                    sleep=5,
                )
                log.info("All mons are up and running")

                # Check the ceph health OK
                ceph_health_check(tries=90, delay=15)

        request.addfinalizer(finalizer)

    def test_multiple_mon_pod_stays_on_same_node(self):
        """
        A testcase to verify multiple mon pods stays on same node

        1. Edit the rook-ceph-mon-endpoints configmap
           say, assign mon-a to another node that would be on
           the same node as another mon (compute-1 instead of compute-0)
        2. Delete the mon-a deployment
        3. Edit the mon-b deployment to remove the required mon anti-affinity
        4. Restart the operator
        5. Edit the mon-a deployment to remove the required mon anti-affinity
        6. See mon-a start on compute-1 with mon-b
        7. Soon after, see the operator failover one of these mons onto the
        node that doesn't currently have a mon (compute-0) and start mon-d

        """
        ocs_version = config.ENV_DATA["ocs_version"]
        # Check that we have LSO cluster and OCS version is 4.8 and below
        # This is a workaround due to issue https://github.com/red-hat-storage/ocs-ci/issues/4937
        if not (
            is_lso_cluster() and Version.coerce(ocs_version) <= Version.coerce("4.8")
        ):
            pytest.skip(
                "Skip the test because mons are not node assignment from Rook, if cluster is not "
                "LSO based. And also currently, we want to run the test only with OCS 4.8 and "
                "below. This is a workaround due to issue "
                "https://github.com/red-hat-storage/ocs-ci/issues/4937"
            )
        # Initialize
        rook_ceph_mon = "rook-ceph-mon"

        # Get mons running on pod
        mon_pods = get_mon_pods()
        mon_name_to_del = mon_pods[0].get().get("metadata").get("labels").get("mon")
        mon_name_to_edit = mon_pods[1].get().get("metadata").get("labels").get("mon")
        mon_node = get_pod_node(mon_pods[1])

        # Edit the rook-ceph-mon-endpoints
        log.info(f"Edit the configmap {ROOK_CEPH_MON_ENDPOINTS}")
        configmap_obj = OCP(kind=CONFIGMAP, namespace=OPENSHIFT_STORAGE_NAMESPACE)
        rook_ceph_mon_configmap = configmap_obj.get(
            resource_name=ROOK_CEPH_MON_ENDPOINTS
        )
        json_val = json.loads(rook_ceph_mon_configmap["data"]["mapping"])
        json_val["node"][mon_name_to_del].update(json_val["node"][mon_name_to_edit])
        rook_ceph_mon_configmap["data"]["mapping"] = json.dumps(json_val)
        new_data = rook_ceph_mon_configmap["data"]
        params = f'{{"data": {json.dumps(new_data)}}}'
        configmap_obj.patch(
            resource_name=ROOK_CEPH_MON_ENDPOINTS,
            params=params,
            format_type="strategic",
        )
        log.info(f"Configmap {ROOK_CEPH_MON_ENDPOINTS} edited successfully")
        log.info(
            f"Rook-ceph-mon-endpoints updated configmap: {rook_ceph_mon_configmap}"
        )

        # Delete one mon deployment which had been edited
        dep_obj = OCP(kind=DEPLOYMENT, namespace=OPENSHIFT_STORAGE_NAMESPACE)
        mon_deployment_name_to_del = f"{rook_ceph_mon}-{mon_name_to_del}"
        log.info(f"Deleting mon {mon_deployment_name_to_del} deployments")
        dep_obj.delete(resource_name=mon_deployment_name_to_del)

        # Edit other mon deployment to remove mon anti-affinity
        mon_deployment_name_to_edit = f"{rook_ceph_mon}-{mon_name_to_edit}"
        log.info(
            f"Edit mon {mon_deployment_name_to_edit} deployment "
            "to remove the required mon anti-affinity"
        )
        params = '[{"op": "remove", "path": "/spec/template/spec/affinity"}]'
        dep_obj.patch(
            resource_name=mon_deployment_name_to_edit, params=params, format_type="json"
        )
        log.info(
            f"Successfully removed defined mon anti-affinity {mon_deployment_name_to_edit}"
        )

        # Restart operator
        operator_pod_obj = get_operator_pods()
        delete_pods(pod_objs=operator_pod_obj)
        POD_OBJ.wait_for_resource(condition=STATUS_RUNNING, selector=OPERATOR_LABEL)

        # Validate deleted deployment mon came up and in pending state
        # Initially mon stucks in pending state, remove defined anti-affinity
        POD_OBJ.wait_for_resource(
            condition=STATUS_PENDING,
            resource_count=1,
            selector=MON_APP_LABEL,
            timeout=1200,
        )
        # Edit mon deployment to remove mon anti-affinity
        log.info(
            f"Edit mon {mon_deployment_name_to_del} deployment "
            "to remove the required mon anti-affinity"
        )
        params = '[{"op": "remove", "path": "/spec/template/spec/affinity"}]'
        dep_obj.patch(
            resource_name=mon_deployment_name_to_del, params=params, format_type="json"
        )
        log.info(
            f"Successfully removed defined mon anti-affinity {mon_deployment_name_to_del}"
        )

        # Validate mon pod moved to another node such that 2 mons are running on same node
        log.info("Waiting for 5 seconds for mon recovery")
        time.sleep(5)
        new_mon_pods = get_mon_pods()
        new_node = [
            get_pod_node(mon)
            for mon in new_mon_pods
            if mon.get().get("metadata").get("labels").get("mon") == mon_name_to_del
        ]
        assert (
            new_node[0].name == mon_node.name
        ), f"Mon moved to node {mon_node} such that 2 mons are running on same node"

        # Verify rook deletes one of the mon and move to another node
        timeout = 60
        log.info(f"Waiting for {timeout} seconds for mon recovery")
        time.sleep(timeout)

        POD_OBJ.wait_for_resource(
            condition=STATUS_RUNNING,
            resource_count=len(mon_pods),
            selector=MON_APP_LABEL,
            timeout=3600,
            sleep=5,
        )
        log.info(
            "Mons are up and running state and validate are running on different nodes"
        )
        mon_pods_running_on_same_node()
