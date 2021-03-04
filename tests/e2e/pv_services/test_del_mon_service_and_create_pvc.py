import json
import logging
import pytest

from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    E2ETest,
    tier4a,
    ignore_leftovers,
)
from ocs_ci.helpers.disruption_helpers import Disruptions
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP, get_services_by_label
from ocs_ci.ocs.resources.pod import get_mon_pods

log = logging.getLogger(__name__)


@tier4a
@ignore_leftovers
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
class TestPvcCreationAfterDelMonService(E2ETest):
    """
    Tests to verify PVC creation after deleting
    mon services manually
    """

    def create_pvc_and_pod(self, interface, pvc_factory, pod_factory):
        """
        create resources for the test

        Args:
            interface(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod
        """
        self.pvc_obj = pvc_factory(
            interface=interface, size=5, status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

    def test_pvc_creation_after_del_mon_services(
        self, interface, pvc_factory, pod_factory
    ):
        """
        1. Delete one mon service
        2. Edit the configmap rook-ceph-endpoints
           remove all the deleted mon services entries
        3. Delete deployment, pvc of deleted mon service
        4. Restart rook-ceph-operator
        5. Make sure all mon pods are running
        6. Repeat above steps for all mons and at the
           end each mon should contain different endpoints
        7. Create PVC, should succeeded.

        """

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

            name = svc["metadata"]["labels"]["pvc_name"]
            cluster_ip = svc["spec"]["clusterIP"]
            port = svc["spec"]["ports"][0]["port"]
            mon_endpoint = f"{cluster_ip}:{port}"
            mon_id = svc["spec"]["selector"]["mon"]
            list_old_svc.append(cluster_ip)

            # Delete the mon service
            svc_obj = OCP(
                kind=constants.SERVICE, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            svc_obj.delete(resource_name=name)

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

            # Delete deployment
            log.info("Delete mon deployments")
            del_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            del_obj.delete(resource_name=name)

            # Delete pvc
            log.info("Delete mon PVC")
            pvc_obj = OCP(
                kind=constants.PVC, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            pvc_obj.delete(resource_name=name)

            # Restart the operator pod
            pod_name = "operator"
            log.info(f"Respin {pod_name} pod")
            disruption = Disruptions()
            disruption.set_resource(resource=f"{pod_name}")
            disruption.delete_resource()

            # Validate all mons are running
            pod_obj = OCP(
                kind=constants.POD, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
            )
            pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=constants.MON_APP_LABEL,
                resource_count=mon_count,
            )
            log.info("All mons are up and running")

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
        self.create_pvc_and_pod(
            interface=interface, pvc_factory=pvc_factory, pod_factory=pod_factory
        )
