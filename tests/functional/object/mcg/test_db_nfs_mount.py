import pytest
import logging

from ocs_ci.framework import config
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_running,
)
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    bugzilla,
    polarion_id,
    vsphere_platform_required,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@mcg
@runs_on_provider
@red_squad
class TestNoobaaDbNFSMount:
    @pytest.fixture()
    def mount_ngix_pod(self, request):
        # try to mount the reesi004 nfs mount to nginx pod
        nginx_pod_data = templating.load_yaml(constants.NGINX_POD_YAML)
        nginx_pod_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        nginx_pod_data["spec"]["containers"][0]["volumeMounts"][0]["name"] = "nfs-vol"
        nginx_pod_data["spec"]["containers"][0]["volumeMounts"][0][
            "mountPath"
        ] = "/var/nfs"
        nginx_pod_data["spec"]["volumes"][0]["name"] = "nfs-vol"
        nginx_pod_data["spec"]["volumes"][0]["nfs"] = dict()
        nginx_pod_data["spec"]["volumes"][0]["nfs"]["server"] = config.ENV_DATA.get(
            "nb_nfs_server"
        )
        nginx_pod_data["spec"]["volumes"][0]["nfs"]["path"] = config.ENV_DATA.get(
            "nb_nfs_mount"
        )
        nginx_pod_data["spec"]["volumes"][0].pop("persistentVolumeClaim")
        nginx_pod = helpers.create_resource(**nginx_pod_data)

        helpers.wait_for_resource_state(
            nginx_pod,
            constants.STATUS_RUNNING,
        )
        logger.info(f"Pod {nginx_pod.name} is created and running")

        def finalizer():
            nginx_pod.delete()
            logger.info(f"Deleted the nginx pod {nginx_pod.name}")

        request.addfinalizer(finalizer)

    @pytest.fixture()
    def mount_noobaa_db_pod(self, request):
        # scale down noobaa db stateful set
        helpers.modify_statefulset_replica_count(
            constants.NOOBAA_DB_STATEFULSET, replica_count=0
        )
        logger.info("Scaled down noobaa db sts")

        # patch the noobaa db stateful set to mount nfs
        ocp_obj = OCP(
            kind=constants.STATEFULSET,
            resource_name=constants.NOOBAA_DB_STATEFULSET,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        params = (
            '{"spec": {"template": {"spec": {"containers": [{"name": "db", "volumeMounts": '
            '[{"mountPath": "/var/nfs", "name": "nfs-vol"}]}], "volumes":[{"name": "nfs-vol", '
            f'"nfs": {{"server": "{config.ENV_DATA.get("nb_nfs_server")}", '
            f'"path": "{config.ENV_DATA.get("nb_nfs_mount")}"}}}}]}}}}}}}}'
        )
        ocp_obj.patch(params=params, format_type="strategic")
        logger.info("Patched noobaa sts to mount nfs")

        # scale up the noobaa db stateful set
        helpers.modify_statefulset_replica_count(
            constants.NOOBAA_DB_STATEFULSET, replica_count=1
        )
        logger.info("Scaled up the noobaa sts")

        def finalizer():
            params = (
                '[{"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/0"},'
                '{"op": "remove", "path": "/spec/template/spec/volumes/0"}]'
            )
            ocp_obj.patch(params=params, format_type="json")
            logger.info("Patched noobaa-db-pod back to the default")
            assert wait_for_pods_to_be_running(
                pod_names=["noobaa-db-pg-0"]
            ), "Noobaa db pod didnt came up running"

        request.addfinalizer(finalizer)

    @tier2
    @vsphere_platform_required
    @bugzilla("2115616")
    @polarion_id("OCS-4950")
    def test_db_nfs_mount(self, mount_ngix_pod, mount_noobaa_db_pod):
        """
        This test verifies noobaa db pod mounts nfs share successfully
        """
        assert wait_for_pods_to_be_running(
            pod_names=["noobaa-db-pg-0"]
        ), "Noobaa db pod didnt came up running, maybe nfs mount failed"
        logger.info("No issues with mounting nfs to noobaa db pod")
