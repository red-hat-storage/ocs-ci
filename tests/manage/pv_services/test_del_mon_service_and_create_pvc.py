import json
import logging
import pytest
import time

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_external_mode,
    E2ETest,
    tier4c,
    ignore_leftovers,
    bugzilla,
    runs_on_provider,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import is_lso_cluster
from ocs_ci.ocs.ocp import OCP, get_services_by_label
from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_operator_pods,
    run_io_in_bg,
    wait_for_storage_pods,
    delete_pods,
)
from ocs_ci.utility.utils import ceph_health_check, TimeoutSampler
from ocs_ci.framework import config

log = logging.getLogger(__name__)

POD_OBJ = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])


@tier4c
@ignore_leftovers
@skipif_external_mode
class TestPvcCreationAfterDelMonService(E2ETest):
    """
    Tests to verify PVC creation after deleting
    mon services manually
    """

    consumer_cluster_index = None

    @pytest.fixture(autouse=True)
    def setup(self):
        if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
            # Get the index of consumer cluster
            self.consumer_cluster_index = config.get_consumer_indexes_list()[0]

    @bugzilla("1858195")
    @runs_on_provider
    @skipif_ocs_version("<4.6")
    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2495")
            ),
            pytest.param(
                constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-2494")
            ),
        ],
    )
    def test_pvc_creation_after_del_mon_services(self, interface, pod_factory):
        """
        1. Delete one mon service
        2. Edit the configmap rook-ceph-endpoints
           remove all the deleted mon services entries
        3. Delete deployment, pvc of deleted mon service
        4. Restart rook-ceph-operator
        5. Make sure all mon pods are running
        6. Make sure ceph health Ok and storage pods are running
        7. Sleep for 300 seconds before deleting another mon
        8. Repeat above steps for all mons and at the
           end each mon should contain different endpoints
        9. Create PVC, should succeeded.

        """
        if self.consumer_cluster_index is not None:
            # Switch to consumer to create PVC, pod and start IO
            config.switch_to_consumer(self.consumer_cluster_index)

        pod_obj = pod_factory(interface=interface)
        run_io_in_bg(pod_obj)

        if self.consumer_cluster_index is not None:
            # Switch to provider
            config.switch_to_provider()

        # Get all mon services
        mon_svc = get_services_by_label(
            label=constants.MON_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        # Get all mon pods
        mon_pods = get_mon_pods()
        mon_count = len(mon_pods)

        list_old_svc = []
        for svc in mon_svc:

            # Get rook-ceph-operator pod obj
            operator_pod_obj = get_operator_pods()
            operator_name = operator_pod_obj[0].name

            # Scale down rook-ceph-operator
            log.info("Scale down rook-ceph-operator")
            assert modify_deployment_replica_count(
                deployment_name="rook-ceph-operator", replica_count=0
            ), "Failed to scale down rook-ceph-operator to 0"
            log.info("Successfully scaled down rook-ceph-operator to 0")

            # Validate rook-ceph-operator pod not running
            POD_OBJ.wait_for_delete(resource_name=operator_name)

            svc_name = svc["metadata"]["name"]
            cluster_ip = svc["spec"]["clusterIP"]
            port = svc["spec"]["ports"][0]["port"]
            mon_endpoint = f"{cluster_ip}:{port}"
            mon_id = svc["spec"]["selector"]["mon"]
            list_old_svc.append(cluster_ip)

            # Delete deployment
            log.info("Delete mon deployments")
            del_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            mon_info = del_obj.get(resource_name=svc_name)
            del_obj.delete(resource_name=svc_name)

            # Delete pvc
            if is_lso_cluster():
                mon_data_path = f"/var/lib/rook/mon-{mon_id}"
                mon_node = mon_info["spec"]["template"]["spec"]["nodeSelector"][
                    "kubernetes.io/hostname"
                ]
                log.info(f"Delete the directory `{mon_data_path}` from {mon_node}")
                cmd = f"rm -rf {mon_data_path}"
                ocp_obj = OCP(namespace=config.ENV_DATA["cluster_namespace"])
                ocp_obj.exec_oc_debug_cmd(node=mon_node, cmd_list=[cmd])
            else:
                log.info("Delete mon PVC")
                pvc_name = svc["metadata"]["labels"]["pvc_name"]
                pvc_obj = OCP(
                    kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"]
                )
                pvc_obj.delete(resource_name=pvc_name)

            # Delete the mon service
            log.info("Delete mon service")
            svc_obj = OCP(
                kind=constants.SERVICE, namespace=config.ENV_DATA["cluster_namespace"]
            )
            svc_obj.delete(resource_name=svc_name)

            # Edit the cm
            log.info(f"Edit the configmap {constants.ROOK_CEPH_MON_ENDPOINTS}")
            configmap_obj = OCP(
                kind=constants.CONFIGMAP,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            output_get = configmap_obj.get(
                resource_name=constants.ROOK_CEPH_MON_ENDPOINTS
            )
            new_data = output_get["data"]
            new_data["csi-cluster-config-json"] = (
                new_data["csi-cluster-config-json"].replace(f'"{mon_endpoint}",', "")
                if new_data["csi-cluster-config-json"].find(f'"{mon_endpoint}",') != 1
                else new_data["csi-cluster-config-json"].replace(
                    f',"{mon_endpoint}"', ""
                )
            )
            new_data["data"] = ",".join(
                [
                    value
                    for value in new_data["data"].split(",")
                    if f"{mon_id}=" not in value
                ]
            )
            new_data["mapping"] = (
                new_data["mapping"].replace(f'"{mon_id}":null,', "")
                if new_data["mapping"].find(f'"{mon_id}":null,') != -1
                else new_data["mapping"].replace(f',"{mon_id}":null', "")
            )
            params = f'{{"data": {json.dumps(new_data)}}}'
            log.info(f"Removing {mon_id} entries from configmap")
            configmap_obj.patch(
                resource_name=constants.ROOK_CEPH_MON_ENDPOINTS,
                params=params,
                format_type="strategic",
            )
            log.info(
                f"Configmap {constants.ROOK_CEPH_MON_ENDPOINTS} edited successfully"
            )

            # Scale up rook-ceph-operator
            log.info("Scale up rook-ceph-operator")
            assert modify_deployment_replica_count(
                deployment_name="rook-ceph-operator", replica_count=1
            ), "Failed to scale up rook-ceph-operator to 1"
            log.info("Successfully scaled up rook-ceph-operator to 1")
            log.info("Validate rook-ceph-operator pod is running")
            POD_OBJ.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=constants.OPERATOR_LABEL,
                resource_count=1,
                timeout=600,
                sleep=5,
            )

            # Validate all mons are running
            log.info("Validate all mons are up and running")
            POD_OBJ.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=constants.MON_APP_LABEL,
                resource_count=mon_count,
                timeout=1200,
                sleep=5,
            )
            log.info("All mons are up and running")

            # Check the ceph health OK
            ceph_health_check(tries=90, delay=15)

            # Validate all storage pods are running
            wait_for_storage_pods()

            # Sleep for some seconds before deleting another mon
            sleep_time = 300
            log.info(f"Waiting for {sleep_time} seconds before deleting another mon")
            time.sleep(sleep_time)

        # Check the endpoints are different
        log.info("Validate the mon endpoints are changed")
        new_mon_svc = get_services_by_label(
            label=constants.MON_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        list_new_svc = []
        for new_svc in new_mon_svc:
            cluster_ip = new_svc["spec"]["clusterIP"]
            list_new_svc.append(cluster_ip)
        diff = set(list_new_svc) ^ set(list_old_svc)
        assert len(diff) == len(list_old_svc + list_new_svc), (
            f"Not all endpoints are changed. Set of old "
            f"endpoints {list_old_svc} and new endpoints {list_new_svc}"
        )
        log.info(f"All new mon endpoints are created {list_new_svc}")

        if self.consumer_cluster_index is not None:
            # Switch to consumer to create PVC, pod and run IO
            config.switch_to_consumer(self.consumer_cluster_index)

        # Create PVC and pods
        log.info(f"Create {interface} PVC")
        pod_obj = pod_factory(interface=interface)
        pod_obj.run_io(storage_type="fs", size="500M")

    @pytest.fixture()
    def validate_all_mon_svc_are_up_at_teardown(self, request):
        """
        Verifies all mon services are running

        """
        # Use provider cluster in managed service platform
        if self.consumer_cluster_index is not None:
            config.switch_to_provider()

        # Get all mon services
        mon_svc_list = get_services_by_label(
            label=constants.MON_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        # Get all mon pods
        mon_pods_list = get_mon_pods()

        def finalizer():
            # Use provider cluster in managed service platform
            if self.consumer_cluster_index is not None:
                config.switch_to_provider()

            # Validate all mon services are running
            if len(mon_svc_list) != len(
                get_services_by_label(
                    label=constants.MON_APP_LABEL,
                    namespace=config.ENV_DATA["cluster_namespace"],
                )
            ):

                # Restart the rook-operator pod
                operator_pod_obj = get_operator_pods()
                delete_pods(pod_objs=operator_pod_obj)
                POD_OBJ.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=constants.OPERATOR_LABEL,
                )

                # Wait till all mon services are up
                for svc_list in TimeoutSampler(
                    1200,
                    len(mon_svc_list),
                    get_services_by_label,
                    constants.MON_APP_LABEL,
                    config.ENV_DATA["cluster_namespace"],
                ):
                    try:
                        if len(svc_list) == len(mon_svc_list):
                            log.info("All expected mon services are up")
                            break
                    except IndexError:
                        log.error(
                            f"All expected mon services are not up only found :{svc_list}. "
                            f"Expected: {mon_svc_list}"
                        )

                # Wait till all mon pods running
                POD_OBJ.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=constants.MON_APP_LABEL,
                    resource_count=len(mon_pods_list),
                    timeout=600,
                    sleep=3,
                )

                # Check the ceph health OK
                ceph_health_check(tries=90, delay=15)

            # Switch the context to consumer cluster if needed
            if self.consumer_cluster_index is not None:
                config.switch_to_consumer(self.consumer_cluster_index)

        request.addfinalizer(finalizer)

    @bugzilla("1969733")
    @skipif_ocs_version("<4.7")
    @runs_on_provider
    @pytest.mark.polarion_id("OCS-2611")
    def test_del_mon_svc(
        self, multi_pvc_factory, validate_all_mon_svc_are_up_at_teardown
    ):
        """
        Test to verify same mon comes up and running
        after deleting mon services manually and joins the quorum

        1. Delete the mon services
        2. Restart the rook operator
        3. Make sure all mon pods are running,
        and same service or endpoints are running
        4. Make sure ceph health Ok and storage pods are running
        5. Create PVC, should succeeded.

        """

        self.sanity_helpers = Sanity()

        # Get all mon services
        mon_svc_before = get_services_by_label(
            label=constants.MON_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        # Get all mon pods
        mon_pods = get_mon_pods()

        # Delete the mon services one by one
        svc_obj = OCP(
            kind=constants.SERVICE, namespace=config.ENV_DATA["cluster_namespace"]
        )
        mon_svc_ip_before = []
        for svc in mon_svc_before:
            svc_name = svc["metadata"]["name"]
            mon_svc_ip_before.append(svc["spec"]["clusterIP"])
            log.info(f"Delete mon service {svc_name}")
            svc_obj.delete(resource_name=svc_name)
            # Verify mon services deleted
            svc_obj.wait_for_delete(resource_name=svc_name)

        # Restart the rook-operator pod
        operator_pod_obj = get_operator_pods()
        delete_pods(pod_objs=operator_pod_obj)
        POD_OBJ.wait_for_resource(
            condition=constants.STATUS_RUNNING, selector=constants.OPERATOR_LABEL
        )

        # Verify same mon services are created again
        for svc in mon_svc_before:
            svc_name = svc["metadata"]["name"]
            svc_obj.check_resource_existence(
                should_exist=True, timeout=300, resource_name=svc_name
            )
        log.info("Same old mon services are recreated")

        # Validate all mons are running
        log.info("Validate all mons are up and running")
        POD_OBJ.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=constants.MON_APP_LABEL,
            resource_count=len(mon_pods),
            timeout=600,
            sleep=3,
        )

        # Validate same mon services are running
        log.info("Validate same mon services are running")
        mon_svc_after = get_services_by_label(
            label=constants.MON_APP_LABEL,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        mon_svc_ip_after = [svc["spec"]["clusterIP"] for svc in mon_svc_after]
        assert len(set(mon_svc_ip_after) ^ set(mon_svc_ip_before)) == 0, (
            "Different mon services are running. "
            f"Before mon services list: {mon_svc_ip_before}, "
            f"After mon services list: {mon_svc_ip_after}"
        )
        log.info("Same old mon services are running and all mons are in running state")

        # Verify everything running fine
        log.info("Verifying All resources are Running and matches expected result")
        self.sanity_helpers.health_check(tries=120)

        # Validate all storage pods are running
        wait_for_storage_pods()

        if self.consumer_cluster_index is not None:
            # Switch to consumer to create PVC
            config.switch_to_consumer(self.consumer_cluster_index)

        # Create and delete resources
        self.sanity_helpers.create_pvc_delete(multi_pvc_factory=multi_pvc_factory)
