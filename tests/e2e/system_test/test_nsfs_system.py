import logging

import pytest

from ocs_ci.framework.testlib import MCGTest, system_test
from ocs_ci.framework.pytest_customization.marks import skipif_mcg_only, ignore_leftovers
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.bucket_utils import (
    random_object_round_trip_verification,
    compare_directory, sync_object_directory,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod

from ocs_ci.ocs.resources.mcg_params import NSFS
from ocs_ci.ocs.resources.pod import get_plugin_pods, get_mds_pods
from tests.conftest import snapshot_factory
from tests.e2e.conftest import noobaa_db_backup_and_recovery

logger = logging.getLogger(__name__)


@system_test
@skipif_mcg_only
@ignore_leftovers
class TestNSFSSystem(MCGTest):
    """"""

    @pytest.mark.polarion_id("")
    def test_nsfs(
        self, mcg_obj, nsfs_bucket_factory, awscli_pod_session, test_directory_setup, snapshot_factory,
        noobaa_db_backup_and_recovery,
    ):
        """"""

        nsfs_obj_new = NSFS(
            method="OC",
            pvc_size=20,
        )
        nsfs_bucket_factory(nsfs_obj_new)
        nsfs_obj_existing = NSFS(
            method="OC",
            pvc_size=20,
            mount_existing_dir=True,
        )
        nsfs_bucket_factory(nsfs_obj_existing)
        nsfs_objs = [nsfs_obj_new, nsfs_obj_existing]
        for nsfs_obj in nsfs_objs:
            awscli_pod_session.exec_cmd_on_pod(f"mkdir -p {test_directory_setup.origin_dir}/{nsfs_obj.bucket_name} {test_directory_setup.result_dir}/{nsfs_obj.bucket_name}")
            random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=nsfs_obj.bucket_name,
                upload_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                download_dir=f"{test_directory_setup.result_dir}/{nsfs_obj.bucket_name}",
                amount=5,
                pattern="nsfs-test-obj-",
                s3_creds=nsfs_obj.s3_creds,
                result_pod=nsfs_obj.interface_pod,
                result_pod_path=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
            )

        pods_to_respin = [
            pod.Pod(
                **pod.get_pods_having_label(
                    label=constants.NOOBAA_CORE_POD_LABEL,
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
            ),
            get_mds_pods()[0],
        ]

        for pods in pods_to_respin:
            pods.delete()

        pods_to_respin = [
            pod.Pod(
                **pod.get_pods_having_label(
                    label=constants.NOOBAA_CORE_POD_LABEL,
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
            ),
            get_plugin_pods(constants.CEPHFILESYSTEM)[0],
            get_mds_pods()[0],
        ]
        for pods in pods_to_respin:
            wait_for_resource_state(
                resource=pods, state=constants.STATUS_RUNNING, timeout=300
            )
        for nsfs_obj in nsfs_objs:
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                signed_request_creds=nsfs_obj.s3_creds,
            )
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                amount=5,
                pattern="nsfs-test-obj-",
                result_pod=nsfs_obj.interface_pod,
            )

        dep_ocp = OCP(
            kind=constants.DEPLOYMENT, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-operator --replicas=0"
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-mon-a --replicas=0"
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-osd-0 --replicas=0"
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-mds-ocs-storagecluster-cephfilesystem-a --replicas=0"
        )

        for nsfs_obj in nsfs_objs:
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                signed_request_creds=nsfs_obj.s3_creds,
            )
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                amount=5,
                pattern="nsfs-test-obj-",
                result_pod=nsfs_obj.interface_pod,
            )
        dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-operator --replicas=1"
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-mon-a --replicas=1"
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-osd-0 --replicas=1"
        )
        dep_ocp.exec_oc_cmd(
            f"scale deployment rook-ceph-mds-ocs-storagecluster-cephfilesystem-a --replicas=1"
        )
        noobaa_db_backup_and_recovery(snapshot_factory=snapshot_factory)
        for nsfs_obj in nsfs_objs:
            sync_object_directory(
                podobj=awscli_pod_session,
                src=f"s3://{nsfs_obj.bucket_name}",
                target=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                signed_request_creds=nsfs_obj.s3_creds,
            )
            compare_directory(
                awscli_pod=awscli_pod_session,
                original_dir=f"{test_directory_setup.origin_dir}/{nsfs_obj.bucket_name}",
                result_dir=nsfs_obj.mount_path + "/" + nsfs_obj.bucket_name,
                amount=5,
                pattern="nsfs-test-obj-",
                result_pod=nsfs_obj.interface_pod,
            )
