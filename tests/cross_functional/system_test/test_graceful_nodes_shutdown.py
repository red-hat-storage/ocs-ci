"""
Test to verify that data is accessible and uncorrupted as well as
operational cluster after graceful nodes shutdown
"""

import logging
import pytest
import time

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants, defaults, registry
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.longevity import start_ocp_workload
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs.monitoring import (
    validate_pvc_created_and_bound_on_monitoring_pods,
    validate_pvc_are_mounted_on_monitoring_pods,
)
from ocs_ci.framework import config
from ocs_ci.utility.uninstall_openshift_logging import uninstall_cluster_logging
from ocs_ci.utility.retry import retry
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    polarion_id,
    skipif_no_kms,
    skipif_ocs_version,
    magenta_squad,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import get_nodes, wait_for_nodes_status
from ocs_ci.ocs.resources.fips import check_fips_enabled

logger = logging.getLogger(__name__)


class TestGracefulNodesShutdown(E2ETest):
    """
    Test uncorrupted data and operational cluster after graceful nodes shutdown
    """

    bucket_names_list = []

    @pytest.fixture(autouse=True)
    def checks(self):
        """
        Fixture to verify cluster is with FIPS and hugepages enabled
        """

        try:
            check_fips_enabled()
        except Exception as FipsNotInstalledException:
            logger.info(
                f"Handled prometheuous pod exception {FipsNotInstalledException}"
            )

        nodes = get_nodes()
        for node in nodes:
            assert (
                node.get()["status"]["allocatable"]["hugepages-2Mi"] == "64Mi"
            ), f"Huge pages is not applied on {node.name}"

    @pytest.fixture(autouse=True)
    def setup(
        self,
        request,
        project_factory,
        pv_encryption_kms_setup_factory,
        storageclass_factory,
        pod_factory,
        pvc_factory,
        pvc_clone_factory,
        snapshot_factory,
    ):
        """
        Setting up test requirements
        1. Non encrypted fs PVC (create + clone + snapshot)
        2. encrypted block PVC (create + clone + snapshot)
        """

        def teardown():
            logger.info("cleanup the environment")

            # cleanup logging workload
            sub = OCP(
                kind=constants.SUBSCRIPTION,
                namespace=constants.OPENSHIFT_LOGGING_NAMESPACE,
            )
            logging_sub = sub.get().get("items")
            if logging_sub:
                logger.info("Logging is configured")
                uninstall_cluster_logging()

        request.addfinalizer(teardown)

        logger.info("Starting the test setup")

        # Create a project
        self.proj_obj = project_factory()

        # Non encrypted fs PVC
        (
            self.ne_pvc_obj,
            self.ne_pvc_pod_obj,
            self.ne_file_name,
            self.ne_pvc_orig_md5_sum,
            self.snap_obj,
        ) = self.setup_non_encrypted_pvc(
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            pvc_clone_factory=pvc_clone_factory,
            snapshot_factory=snapshot_factory,
        )

        # Encrypted block PVC
        (
            self.eb_pvc_obj,
            self.eb_pvc_pod_obj,
            self.eb_file_name,
            self.eb_pvc_orig_md5_sum,
            self.eb_snap_obj,
        ) = self.setup_encrypted_pvc(
            pv_encryption_kms_setup_factory=pv_encryption_kms_setup_factory,
            storageclass_factory=storageclass_factory,
            interface=constants.CEPHBLOCKPOOL,
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            pvc_clone_factory=pvc_clone_factory,
            snapshot_factory=snapshot_factory,
        )

    def setup_non_encrypted_pvc(
        self,
        pod_factory,
        pvc_factory,
        pvc_clone_factory,
        snapshot_factory,
    ):
        """
        Creates non encrypted fs pvc,clone of pvc and snapshot
        Args:
            pod_factory : Fixture to create new PODs
            pvc_factory: pod_factory : Fixture to create new PVCs
            pvc_clone_factory :Fixture to create a clone from PVC
            snapshot_factory: Fixture to create a VolumeSnapshot ofPVC

        Returns:
            pvc_object: PVC objects for which snapshots are to be created.
            pod_object: POD objects attached to the PVCs
            file_name(str) : Name of the file on which FIO is performed.
            origin md5sum(hex) : md5sum of data present in file
            snap_obj: The object of snapshot created from pvc.
        """

        logger.info("Adding non encrypted pvc")
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=5,
            status=constants.STATUS_BOUND,
            project=self.proj_obj,
        )
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )

        # Run IO for data integrity
        file_name = pod_obj.name
        logger.info(f"File created during IO {file_name} (non encrypted pvc)")
        pod_obj.run_io(storage_type="fs", size="500M", fio_filename=file_name)

        # Wait for fio to finish
        pod_obj.get_fio_results()
        logger.info(f"Io completed on pod {pod_obj.name} (non encrypted pvc)")

        orig_md5_sum = pod.cal_md5sum(pod_obj, file_name)

        pvc_clone_factory(pvc_obj)
        snap_obj = snapshot_factory(pvc_obj)
        return pvc_obj, pod_obj, file_name, orig_md5_sum, snap_obj

    def setup_encrypted_pvc(
        self,
        pvc_type="block",
        pv_encryption_kms_setup_factory=None,
        storageclass_factory=None,
        interface=constants.CEPHFILESYSTEM,
        pod_factory=None,
        pvc_factory=None,
        pvc_clone_factory=None,
        snapshot_factory=None,
    ):
        """
        This function creates encrypted block/fs pvc, clone of pvc and snapshot

        Args:
            pvc_type: Type of pvc used to create pvc
            pv_encryption_kms_setup_factory: Fixture used to create encrypted pvc
            pod_factory : Fixture to create new PODs
            pvc_factory:  Fixture to create new PVCs
            pvc_clone_factory :Fixture to create a clone from PVC
            snapshot_factory: Fixture to create a VolumeSnapshot ofPVC

        Returns:
            pvc_object: PVC objects for which snapshots are to be created.
            pod_object: POD objects attached to the PVCs
            file_name(str) : Name of the file on which FIO is performed.
            origin md5sum(hex) : md5sum of data present in file
            encrypt_snap_obj: The object of snapshot created from encrypted pvc.

        """

        logger.info(f"Adding encrypted {pvc_type} pvc")
        if interface == constants.CEPHBLOCKPOOL:
            self.vault = pv_encryption_kms_setup_factory("v1", False)
            self.sc_obj = storageclass_factory(
                interface=interface,
                encrypted=True,
                encryption_kms_id=self.vault.kmsid,
            )
            self.vault.vault_path_token = self.vault.generate_vault_token()
            self.vault.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)
        pvc_obj = pvc_factory(
            interface=interface,
            storageclass=self.sc_obj,
            size=5,
            status=constants.STATUS_BOUND,
            project=self.proj_obj,
        )
        pod_obj = pod_factory(
            interface=interface,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        # Run IO for data integrity
        file_name = pod_obj.name
        logger.info(f"File created during IO {file_name} (encrypted {pvc_type} pvc)")
        pod_obj.run_io(storage_type="fs", size="500M", fio_filename=file_name)

        # Wait for fio to finish
        pod_obj.get_fio_results()
        logger.info(f"Io completed on pod {pod_obj.name} (encrypted {pvc_type} pvc)")

        origin_md5sum = pod.cal_md5sum(pod_obj, file_name)

        pvc_clone_factory(pvc_obj)
        encrypt_snap_obj = snapshot_factory(pvc_obj)

        return pvc_obj, pod_obj, file_name, origin_md5sum, encrypt_snap_obj

    def validate_snapshot_restore(
        self, pod_factory, snapshot_restore_factory, teardown_factory
    ):
        """
        Verifies the snapshot restore works fine as well as PVC expansion
        is possible on the restored snapshot
        """
        self.restore_pvc_objs = list()
        logger.info("Creating snapshot restore for non-encrypted pvcs after reboot")
        ne_restored_pvc = snapshot_restore_factory(
            self.snap_obj,
            storageclass=self.ne_pvc_obj.storageclass.name,
            volume_mode=self.snap_obj.parent_volume_mode,
            timeout=180,
        )
        self.restore_pvc_objs.append(ne_restored_pvc)

        logger.info("Creating snapshot restore for encrypted rbd pvcs after reboot")
        eb_restored_pvc = snapshot_restore_factory(
            self.eb_snap_obj,
            storageclass=self.eb_pvc_obj.storageclass.name,
            volume_mode=self.eb_snap_obj.parent_volume_mode,
            timeout=180,
        )
        self.restore_pvc_objs.append(eb_restored_pvc)

        for pvc_obj in self.restore_pvc_objs:
            pod_obj = pod_factory(
                interface=pvc_obj.interface,
                pvc=pvc_obj,
                status=constants.STATUS_RUNNING,
            )
            logger.info(f"Attaching the PVC {pvc_obj.name} to pod " f"{pod_obj.name}")
            teardown_factory(pod_obj)

    def validate_pvc_expansion(self, pvc_size_new):
        """
        expand size of PVC and verify the expansion

        Args:
            pvc_size_new (int): Size of PVC(in Gb) to expand
        """
        for pvc_obj in self.restore_pvc_objs:
            logger.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
            pvc_obj.resize_pvc(pvc_size_new, True)

    def validate_data_integrity(self):
        """
        Verifies the md5sum values of files are OK
        Raises:
            AssertionError: If there is a mismatch in md5sum value or None
        """
        logger.info("Compare bucket object after reboot")
        for idx, bucket_name in enumerate(self.bucket_names_list):
            after_bucket_object_set = {
                obj.key
                for obj in self.mcg_obj.s3_list_all_objects_in_bucket(bucket_name)
            }
            assert (
                self.before_bucket_object_list[idx] == after_bucket_object_set
            ), f"Data integrity failed for S3 bucket {bucket_name}"

        logger.info("Compare PVCs MD5Sum after reboot")
        assert self.ne_pvc_orig_md5_sum == pod.cal_md5sum(
            self.ne_pvc_pod_obj, self.ne_file_name
        ), (
            f"Data integrity failed for file '{self.ne_file_name}' "
            f"on non encrypted pvc {self.ne_pvc_obj.name} on pod {self.ne_pvc_pod_obj.name}"
        )

        assert self.eb_pvc_orig_md5_sum == pod.cal_md5sum(
            self.eb_pvc_pod_obj, self.eb_file_name
        ), (
            f"Data integrity failed for file '{self.eb_file_name}' "
            f"on encrypted block pvc {self.eb_pvc_obj.name} on pod {self.eb_pvc_pod_obj.name}"
        )

    def validate_ocp_workload_exists(self):
        """
        Verify ocp workload continues after reboot
        """
        logger.info("Validate Monitoring stack exists")
        pods_list = pod.get_all_pods(
            namespace=defaults.OCS_MONITORING_NAMESPACE,
            selector=["prometheus", "alertmanager"],
        )
        retry((CommandFailed, ResourceWrongStatusException), tries=3, delay=15)(
            pod.validate_pods_are_respinned_and_running_state
        )(pods_list)

        # Validate the pvc is created on monitoring pods
        validate_pvc_created_and_bound_on_monitoring_pods()

        # Validate the pvc are mounted on pods
        retry((CommandFailed, AssertionError), tries=3, delay=15)(
            validate_pvc_are_mounted_on_monitoring_pods
        )(pods_list)

        logger.info("Validate Registry stack exists after reboot")
        # Validate registry pod status
        retry((CommandFailed, UnexpectedBehaviour), tries=3, delay=15)(
            registry.validate_registry_pod_status
        )()

        # Validate pvc mount in the registry pod
        retry((CommandFailed, UnexpectedBehaviour, AssertionError), tries=3, delay=15)(
            registry.validate_pvc_mount_on_registry_pod
        )()
        sub = OCP(
            kind=constants.SUBSCRIPTION,
            namespace=constants.OPENSHIFT_LOGGING_NAMESPACE,
        )
        logging_sub = sub.get().get("items")
        if not logging_sub:
            assert "Logging is not configured"

    @system_test
    @polarion_id("OCS-3976")
    @skipif_no_kms
    @skipif_ocs_version("<4.11")
    @magenta_squad
    def test_graceful_nodes_shutdown(
        self,
        scale_noobaa_resources_session,
        multi_obc_lifecycle_factory,
        nodes,
        setup_mcg_bg_features,
        validate_mcg_bg_features,
        snapshot_restore_factory,
        pod_factory,
        teardown_factory,
    ):
        """
        Steps:
          1) Have a cluster with FIPS, hugepages, encryption enabled.
          2) Create some resources: Create PVC (encrypt & non encrypt),
            take snapshot and restore it into new PVCs, clone PVC
          3) Configure OCP workloads(monitoring, registry, logging)
          4) Create rgw kafka notifications and objects should be notified
          5) Create normal OBCs and buckets
          6) Perform mcg bucket replication (uni and bidirectional) and see the objects are synced.
          7) Perform noobaa caching
          8) Perform S3 operations on NSFS bucket on files : put, get, delete, list objects
          9) With all the above preconditions met, follow the KCS steps for graceful shutdown of nodes
            https://access.redhat.com/articles/5394611, and bring up the cluster back.
            Once cluster up, there should not be seen any issues or impact on any data(No DU/DL/DC) and also
            normal operations should work fine.
         10) Do the Validation of steps 2 to 8 after cluster up and running.
        """

        # Create normal OBCs and buckets
        multi_obc_lifecycle_factory(num_of_obcs=2, bulk=True, measure=False)

        # OCP Workloads
        logger.info("start_ocp_workload")
        start_ocp_workload(
            workloads_list=["monitoring", "registry", "logging"], run_in_bg=True
        )

        # Setup MCG Features
        logger.info(
            "Setup MCG Features- Bucket replication,"
            " Noobaa caching,Object expiration,"
            "MCG NSFS,RGW kafka notification"
        )
        feature_setup_map = setup_mcg_bg_features(
            skip_any_features=["caching", "nsfs", "rgw kafka"]
        )

        # check OSD status after graceful node shutdown
        worker_nodes = get_nodes(node_type="worker")
        master_nodes = get_nodes(node_type="master")

        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            master_instances = nodes.get_ec2_instances(nodes=master_nodes)
            worker_instances = nodes.get_ec2_instances(nodes=worker_nodes)

        logger.info("Gracefully Shutting down worker & master nodes")
        nodes.stop_nodes(nodes=worker_nodes, force=False)
        nodes.stop_nodes(nodes=master_nodes, force=False)

        logger.info("waiting for 5 min before starting nodes")
        time.sleep(300)

        logger.info("Starting worker & master nodes")

        if config.ENV_DATA["platform"].lower() == constants.AWS_PLATFORM:
            nodes.start_nodes(instances=master_instances, nodes=master_nodes)
            nodes.start_nodes(instances=worker_instances, nodes=worker_nodes)
        else:
            nodes.start_nodes(nodes=master_nodes)
            nodes.start_nodes(nodes=worker_nodes)

        retry(
            (
                CommandFailed,
                TimeoutError,
                AssertionError,
                ResourceWrongStatusException,
            ),
            tries=30,
            delay=15,
        )(wait_for_nodes_status(timeout=1800))
        logger.info("All nodes are now in READY state")

        logger.info("Waiting for 10 min for all pods to come in running state.")
        time.sleep(600)

        # check cluster health
        try:
            logger.info("Making sure ceph health is OK")
            Sanity().health_check(tries=50, cluster_check=False)
        except Exception as ex:
            logger.error("Failed at cluster health check!!")
            raise ex

        self.validate_data_integrity()

        self.validate_snapshot_restore(
            pod_factory, snapshot_restore_factory, teardown_factory
        )
        self.validate_pvc_expansion(pvc_size_new=10)

        validate_mcg_bg_features(
            feature_setup_map, skip_any_features=["caching", "rgw kafka", "nsfs"]
        )
        self.validate_ocp_workload_exists()

        # check osd status
        state = "down"
        ct_pod = pod.get_ceph_tools_pod()
        tree_output = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd tree")
        logger.info("ceph osd tree output:")
        logger.info(tree_output)
        assert not (
            state in str(tree_output)
        ), "OSD are down after graceful node shutdown"
