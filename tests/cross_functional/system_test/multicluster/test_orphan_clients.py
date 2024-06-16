import logging

import pytest

from ocs_ci.deployment.helpers.hypershift_base import (
    get_random_cluster_name,
    HyperShiftBase,
)
from ocs_ci.framework import config as ocsci_config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.framework.pytest_customization.marks import (
    tier4b,
)
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import get_latest_release_version
from ocs_ci.utility.version import get_ocs_version_from_csv

logger = logging.getLogger(__name__)


@pytest.fixture
def return_to_original_context(request):
    """
    Make sure that original context is restored after the test.
    """
    original_cluster = ocsci_config.cluster_ctx.MULTICLUSTER["multicluster_index"]

    def finalizer():
        logger.info(f"Switching back to original cluster with index {original_cluster}")
        ocsci_config.switch_ctx(original_cluster)

    request.addfinalizer(finalizer)
    yield


class TestStorageClientRemoval(object):
    """
    Test storage client removal
    """

    @tier4b
    def test_remove_orphan_clients_resources(
        self,
        create_hypershift_clusters,
        pvc_factory,
        pod_factory,
        return_to_original_context,
    ):
        """
        This test is to remove the orphaned storage client resources

        Steps:
        1. Create hosted client.
        2. Add block and cephfs resources and data on hosted client.
        3. Remove the storage client with `hcp` command.
        4. Verify the storage client and it's resources were removed from Provider.
        """

        log_step("Create hosted client")
        cluster_name = get_random_cluster_name()
        odf_version = str(get_ocs_version_from_csv()).replace(".stable", "")

        ocp_version = get_latest_release_version()
        hosted_clusters_conf_on_provider = {
            "ENV_DATA": {
                "clusters": {
                    cluster_name: {
                        "hosted_cluster_path": f"~/clusters/{cluster_name}/openshift-cluster-dir",
                        "ocp_version": ocp_version,
                        "cpu_cores_per_hosted_cluster": 8,
                        "memory_per_hosted_cluster": "12Gi",
                        "hosted_odf_registry": "quay.io/rhceph-dev/ocs-registry",
                        "hosted_odf_version": odf_version,
                        "setup_storage_client": True,
                        "nodepool_replicas": 2,
                    }
                }
            }
        }

        create_hypershift_clusters(hosted_clusters_conf_on_provider)

        original_cluster_index = ocsci_config.cluster_ctx.MULTICLUSTER[
            "multicluster_index"
        ]

        log_step(
            "Switch to the hosted cluster. Add block and cephfs resources and data"
        )
        ocsci_config.switch_to_cluster_by_name(cluster_name)

        modes = [
            (
                constants.CEPHBLOCKPOOL,
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_FILESYSTEM,
            ),
            (
                constants.CEPHFILESYSTEM,
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_FILESYSTEM,
            ),
            (
                constants.CEPHFILESYSTEM,
                constants.ACCESS_MODE_RWX,
                constants.VOLUME_MODE_FILESYSTEM,
            ),
            (
                constants.CEPHBLOCKPOOL,
                constants.ACCESS_MODE_RWO,
                constants.VOLUME_MODE_BLOCK,
            ),
            (
                constants.CEPHBLOCKPOOL,
                constants.ACCESS_MODE_RWX,
                constants.VOLUME_MODE_BLOCK,
            ),
        ]
        self.pod_objs = list()
        self.pvc_objs = list()
        for mode in modes:
            pvc_obj = pvc_factory(
                interface=mode[0],
                access_mode=mode[1],
                size=2,
                volume_mode=mode[2],
                status=constants.STATUS_BOUND,
            )
            logger.info(
                f"Created new pvc {pvc_obj.name}  sc_name={mode[0]} size=2Gi, "
                f"access_mode={mode[1]}, volume_mode={mode[2]}"
            )
            self.pvc_objs.append(pvc_obj)
            if mode[2] == constants.VOLUME_MODE_BLOCK:
                pod_dict_path = constants.CSI_RBD_RAW_BLOCK_POD_YAML
                storage_type = constants.WORKLOAD_STORAGE_TYPE_BLOCK
                raw_block_pv = True
            else:
                pod_dict_path = constants.NGINX_POD_YAML
                storage_type = constants.WORKLOAD_STORAGE_TYPE_FS
                raw_block_pv = False
            logger.info(
                f"Created new pod sc_name={mode[0]} size=2Gi, access_mode={mode[1]}, volume_mode={mode[2]}"
            )
            pod_obj = pod_factory(
                interface=mode[0],
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
                pod_dict_path=pod_dict_path,
                raw_block_pv=raw_block_pv,
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size="1GB",
                verify=True,
            )
            self.pod_objs.append(pod_obj)

        for pod_obj in self.pod_objs:
            fio_result = pod_obj.get_fio_results()
            logger.info("IOPs after FIO:")
            reads = fio_result.get("jobs")[0].get("read").get("iops")
            writes = fio_result.get("jobs")[0].get("write").get("iops")
            logger.info(f"Read: {reads}")
            logger.info(f"Write: {writes}")

        log_step("Remove the storage client with `hcp` command")
        ocsci_config.switch_ctx(original_cluster_index)
        HyperShiftBase().destroy_kubevirt_cluster(cluster_name)

        log_step(
            "Verify the storage client and it's resources were removed from Provider"
        )
