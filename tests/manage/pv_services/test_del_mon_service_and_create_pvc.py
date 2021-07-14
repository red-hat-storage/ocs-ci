import json
import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import skipif_rbd_not_deployed, skipif_cephfs_not_deployed
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_external_mode,
    E2ETest,
    tier4a,
    ignore_leftovers,
    bugzilla,
)
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import is_lso_cluster
from ocs_ci.ocs.ocp import OCP, get_services_by_label
from ocs_ci.ocs.resources.pod import (
    get_mon_pods,
    get_operator_pods,
    run_io_in_bg,
    wait_for_storage_pods,
)
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)

POD_OBJ = OCP(kind=constants.POD, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)


@bugzilla("1858195")
@tier4a
@ignore_leftovers
@skipif_external_mode
@skipif_ocs_version("<4.6")
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL,
            marks=[pytest.mark.polarion_id("OCS-2495"), skipif_rbd_not_deployed],
        ),
        pytest.param(
            constants.CEPHFILESYSTEM,
            marks=[pytest.mark.polarion_id("OCS-2494"), skipif_cephfs_not_deployed],
        ),
    ],
)
class TestPvcCreationAfterDelMonService(E2ETest):
    """
    Tests to verify PVC creation after deleting
    mon services manually
    """

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

        pod_obj = pod_factory(interface=interface)
        run_io_in_bg(pod_obj)

        # Get all mon services
        mon_svc = get_services_by_label(
            label=constants.MON_APP_LABEL,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
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
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
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
                ocp_obj = OCP(namespace=constants.OPENSHIFT_STORAGE_NAMESPACE)
                ocp_obj.exec_oc_debug_cmd(node=mon_node, cmd_list=[cmd])
            else:
                log.info("Delete mon PVC")
                pvc_name = svc["metadata"]["labels"]["pvc_name"]
                pvc_obj = OCP(
                    kind=constants.PVC, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
                )
                pvc_obj.delete(resource_name=pvc_name)

            # Delete the mon service
            log.info("Delete mon service")
            svc_obj = OCP(
                kind=constants.SERVICE, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            svc_obj.delete(resource_name=svc_name)

            # Edit the cm
            log.info(f"Edit the configmap {constants.ROOK_CEPH_MON_ENDPOINTS}")
            configmap_obj = OCP(
                kind=constants.CONFIGMAP,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
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
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
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

        # Create PVC and pods
        log.info(f"Create {interface} PVC")
        pod_obj = pod_factory(interface=interface)
        pod_obj.run_io(storage_type="fs", size="500M")
