"""
Test to verify that data is accessible and uncorrupted as well as
operational cluster after graceful nodes shutdown
"""
import re
import boto3
import json
import logging
import pytest
import time
import uuid

from datetime import datetime
from semantic_version import Version

from ocs_ci.ocs.ocp import OCP, wait_for_cluster_connectivity
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.rgw import RGW
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.pod import get_pod_logs, get_rgw_pods, get_pod_obj
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    polarion_id,
    skipif_no_kms,
    skipif_ocs_version,
)
from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs.node import (
    get_nodes,
)
from ocs_ci.utility.utils import exec_cmd, run_cmd, clone_notify
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.helpers.helpers import default_storage_class
from ocs_ci.ocs.bucket_utils import s3_put_object, retrieve_verification_mode

from ocs_ci.ocs.resources.fips import check_fips_enabled
from ocs_ci.ocs.jenkins import Jenkins
from ocs_ci.ocs.cosbench import Cosbench
from ocs_ci.ocs.quay_operator import QuayOperator
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.constants import AWSCLI_TEST_OBJ_DIR
from ocs_ci.ocs.bucket_utils import (
    compare_bucket_object_list,
    sync_object_directory,
    write_random_test_objects_to_bucket,
)

logger = logging.getLogger(__name__)


class TestGracefulNodesShutdown(E2ETest):
    """
    Test uncorrupted data and operational cluster after graceful nodes shutdown
    """

    amq = None
    cosbench = None
    jenkins = None
    quay_operator = None
    bucket_names_list = []
    mcg_obj = None
    mcg_obj_session = None

    @pytest.fixture(autouse=True)
    def checks(self):
        # This test is skipped due to https://issues.redhat.com/browse/ENTMQST-3422
        check_fips_enabled()

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
        pvc_factory,
        pod_factory,
        pvc_clone_factory,
        snapshot_factory,
        snapshot_restore_factory,
        mcg_obj,
        bucket_factory,
        install_logging,
        awscli_pod_session,
        mcg_obj_session,
        test_directory_setup,
    ):
        """
        Setting up test requirements
        """

        self.amq = AMQ()

        self.kafka_topic = (
            self.kafkadrop_pod
        ) = self.kafkadrop_svc = self.kafkadrop_route = None

        def teardown():
            logger.info("cleanup the environment")
            if self.cosbench:
                # Dispose containers and objects
                self.cosbench.run_cleanup_workload(
                    prefix="cosbench-mcg-bucket-",
                    containers=5,
                    objects=100,
                    validate=True,
                    timeout=1200,
                    sleep=30,
                )
                self.cosbench.cleanup()
            if self.jenkins:
                self.jenkins.cleanup()
            if self.quay_operator:
                self.quay_operator.teardown()
            if self.amq:
                if self.kafkadrop_pod:
                    self.kafkadrop_pod.delete()
                if self.kafkadrop_svc:
                    self.kafkadrop_svc.delete()
                if self.kafkadrop_route:
                    self.kafkadrop_route.delete()

                self.amq.cleanup()

        request.addfinalizer(teardown)

        logger.info("Starting the test setup")

        # AMQ Kafka notification
        self.setup_amq_kafka_notification(bucket_factory=bucket_factory)
        # Cosbench
        self.setup_cosbench()

        # Jenkins
        self.setup_jenkins()

        # Quay
        self.quay_operator = QuayOperator()

        # Create a project
        self.proj_obj = project_factory()

        # Non encrypted fs PVC
        (
            self.ne_pvc_obj,
            self.ne_pvc_pod_obj,
            self.ne_file_name,
            self.ne_pvc_orig_md5_sum,
        ) = self.setup_non_encrypted_pvc(
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            pvc_clone_factory=pvc_clone_factory,
            snapshot_factory=snapshot_factory,
            snapshot_restore_factory=snapshot_restore_factory,
        )

        # Encrypted block PVC
        (
            self.eb_pvc_obj,
            self.eb_pvc_pod_obj,
            self.eb_file_name,
            self.eb_pvc_orig_md5_sum,
        ) = self.setup_encrypted_pvc(
            pv_encryption_kms_setup_factory=pv_encryption_kms_setup_factory,
            storageclass_factory=storageclass_factory,
            interface=constants.CEPHBLOCKPOOL,
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            pvc_clone_factory=pvc_clone_factory,
            snapshot_factory=snapshot_factory,
        )

        # Encrypted fs PVC
        (
            self.efs_pvc_obj,
            self.efs_pvc_pod_obj,
            self.efs_file_name,
            self.efs_pvc_orig_md5_sum,
        ) = self.setup_encrypted_pvc(
            pvc_factory=pvc_factory,
            pod_factory=pod_factory,
            pvc_clone_factory=pvc_clone_factory,
            snapshot_factory=snapshot_factory,
        )

        # S3 bucket
        self.setup_s3_bucket(mcg_obj=mcg_obj, bucket_factory=bucket_factory)

        # # MCG bucket replication (unidirectional)
        # self.setup_mcg_bucket_rep_unidirectional(
        #     mcg_obj_session=mcg_obj_session,
        #     bucket_factory=bucket_factory,
        #     awscli_pod_session=awscli_pod_session,
        # )
        #
        # # MCG bucket replication (bidirectional)
        # self.setup_mcg_bucket_rep_bidirectional(
        #     bucket_factory=bucket_factory,
        #     awscli_pod_session=awscli_pod_session,
        #     mcg_obj_session=mcg_obj_session,
        #     test_directory_setup=test_directory_setup,
        # )

    def setup_amq_kafka_notification(self, bucket_factory):
        """
        ##################################### AMQ
        """
        sc = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)
        self.amq.setup_amq_cluster(sc.name)

        """
        ############# Kafka notification
        """
        self.kafka_topic = self.amq.create_kafka_topic()

        # Create Kafkadrop pod
        (
            self.kafkadrop_pod,
            self.kafkadrop_svc,
            self.kafkadrop_route,
        ) = self.amq.create_kafkadrop()

        # Get the kafkadrop route
        kafkadrop_host = self.kafkadrop_route.get().get("spec").get("host")

        # Create bucket
        bucketname = bucket_factory(amount=1, interface="RGW-OC")[0].name

        # Get RGW credentials
        rgw_obj = RGW()
        rgw_endpoint, access_key, secret_key = rgw_obj.get_credentials()

        # Clone notify repo
        notify_path = clone_notify()

        # Initialise to put objects
        data = "A random string data to write on created rgw bucket"
        obc_obj = OBC(bucketname)
        s3_resource = boto3.resource(
            "s3",
            verify=retrieve_verification_mode(),
            endpoint_url=rgw_endpoint,
            aws_access_key_id=obc_obj.access_key_id,
            aws_secret_access_key=obc_obj.access_key,
        )
        s3_client = s3_resource.meta.client

        # Initialize notify command to run
        notify_cmd = (
            f"python {notify_path} -e {rgw_endpoint} -a {obc_obj.access_key_id} "
            f"-s {obc_obj.access_key} -b {bucketname} -ke {constants.KAFKA_ENDPOINT} -t {self.kafka_topic.name}"
        )
        logger.info(f"Running cmd {notify_cmd}")

        # Put objects to bucket
        assert s3_client.put_object(
            Bucket=bucketname, Key="key-1", Body=data
        ), "Failed: Put object: key-1"
        exec_cmd(notify_cmd)

        # Validate rgw logs notification are sent
        pattern = "ERROR: failed to create push endpoint"
        rgw_pod_obj = get_rgw_pods()
        rgw_log = get_pod_logs(pod_name=rgw_pod_obj[0].name, container="rgw")
        assert re.search(pattern=pattern, string=rgw_log) is None, (
            f"Error: {pattern} msg found in the rgw logs."
            f"Validate {pattern} found on rgw logs and also "
            f"rgw bucket notification is working correctly"
        )
        assert s3_client.put_object(
            Bucket=bucketname, Key="key-2", Body=data
        ), "Failed: Put object: key-2"
        exec_cmd(notify_cmd)

        # Validate message are received Kafka side using curl command
        curl_command = (
            f"curl -X GET {kafkadrop_host}/topic/{self.kafka_topic.name} "
            "-H 'content-type: application/vnd.kafka.json.v2+json'"
        )
        json_output = run_cmd(cmd=curl_command)
        new_string = json_output.split()
        messages = new_string[new_string.index("messages</td>") + 1]
        if messages.find("1") == -1:
            raise Exception(
                "Error: Messages are not recieved from Kafka side."
                "RGW bucket notification is not working as expected."
            )

        # Validate the timestamp events
        ocs_version = config.ENV_DATA["ocs_version"]
        if Version.coerce(ocs_version) >= Version.coerce("4.8"):
            cmd = (
                f"bin/kafka-console-consumer.sh --bootstrap-server {constants.KAFKA_ENDPOINT} "
                f"--topic {self.kafka_topic.name} --from-beginning --timeout-ms 20000"
            )
            pod_list = get_pod_name_by_pattern(
                pattern="my-cluster-zookeeper", namespace=constants.AMQ_NAMESPACE
            )
            zookeeper_obj = get_pod_obj(
                name=pod_list[0], namespace=constants.AMQ_NAMESPACE
            )
            event_obj = zookeeper_obj.exec_cmd_on_pod(command=cmd)
            logger.info(f"Event obj: {event_obj}")
            event_time = event_obj.get("Records")[0].get("eventTime")
            format_string = "%Y-%m-%dT%H:%M:%S.%fZ"
            try:
                datetime.strptime(event_time, format_string)
            except ValueError as ef:
                logger.error(
                    f"Timestamp event {event_time} doesnt match the pattern {format_string}"
                )
                raise ef

            logger.info(
                f"Timestamp event {event_time} matches the pattern {format_string}"
            )
        """
        ############# END Kafka notification
        """

        # Run open messages
        self.amq.create_messaging_on_amq(topic_name="test-topic")

        # Wait for some time to generate msg
        waiting_time = 60
        logger.info(f"Waiting for {waiting_time} sec to generate msg")
        time.sleep(60)

        threads = self.amq.run_in_bg()
        for thread in threads:
            thread.result(timeout=1800)

    def setup_cosbench(self):
        self.cosbench = Cosbench()
        self.cosbench.setup_cosbench()

        # Create initial containers and objects
        self.cosbench.run_init_workload(
            prefix="cosbench-mcg-bucket-",
            containers=5,
            objects=100,
            validate=True,
            size=8,
            size_unit="MB",
            timeout=1200,
            sleep=30,
        )

    def setup_jenkins(self):
        self.jenkins = Jenkins()
        self.jenkins.create_ocs_jenkins_template()
        self.jenkins.number_projects = 1
        self.jenkins.create_app_jenkins()
        self.jenkins.create_jenkins_pvc()
        self.jenkins.create_jenkins_build_config()
        self.jenkins.wait_for_jenkins_deploy_status(status=constants.STATUS_COMPLETED)

    def setup_non_encrypted_pvc(
        self,
        pvc_factory,
        pod_factory,
        pvc_clone_factory,
        snapshot_factory,
        snapshot_restore_factory,
    ):
        """
        ##################################### non encrypted pvc (ne_pvc)
        create + clone + restore
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

        snapshot_restore_factory(
            snapshot_obj=snap_obj,
            volume_mode=snap_obj.parent_volume_mode,
        )
        return pvc_obj, pod_obj, file_name, orig_md5_sum

    def setup_encrypted_pvc(
        self,
        pvc_type="block",
        pv_encryption_kms_setup_factory=None,
        storageclass_factory=None,
        interface=constants.CEPHFILESYSTEM,
        pvc_factory=None,
        pod_factory=None,
        pvc_clone_factory=None,
        snapshot_factory=None,
    ):
        """
        encrypted block/fs pvc
        Returns:
            pvc object
            pod object
            file_name
            origin md5sum
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
            size=3,
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
        snapshot_factory(pvc_obj)

        return pvc_obj, pod_obj, file_name, origin_md5sum

    def setup_s3_bucket(self, mcg_obj, bucket_factory):
        """
        ##################################### S3 buckets
        """

        # Create 1 bucket of each type (CLI, OC & S3)
        self.mcg_obj = mcg_obj
        self.bucket_names_list.append(bucket_factory(interface="CLI")[0].name)
        self.bucket_names_list.append(bucket_factory(interface="OC")[0].name)
        obj_data = "A random string data"

        self.before_bucket_object_list = [None] * len(self.bucket_names_list)
        for idx, bucket_name in enumerate(self.bucket_names_list):
            key = "ObjKey-" + str(uuid.uuid4().hex)
            assert s3_put_object(
                mcg_obj, bucket_name, key, obj_data
            ), f"Failed: Put object, {key}, bucket name: {bucket_name}"

            self.before_bucket_object_list[idx] = {
                obj.key for obj in mcg_obj.s3_list_all_objects_in_bucket(bucket_name)
            }

    def setup_mcg_bucket_rep_unidirectional(
        self, mcg_obj_session, bucket_factory, awscli_pod_session
    ):
        """
        ##################################### mcg bucket replication - unidirectional
        """
        self.mcg_obj_session = mcg_obj_session
        self.target_bucket_name = bucket_factory(
            bucketclass={"interface": "OC", "backingstore_dict": {"azure": [(1, None)]}}
        )[0].name
        replication_policy = ("basic-replication-rule", self.target_bucket_name, None)
        self.source_bucket_name = bucket_factory(
            1,
            bucketclass={
                "interface": "OC",
                "backingstore_dict": {"aws": [(1, "eu-central-1")]},
            },
            replication_policy=replication_policy,
        )[0].name
        full_object_path = f"s3://{self.source_bucket_name}"
        standard_test_obj_list = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")
        sync_object_directory(
            awscli_pod_session, AWSCLI_TEST_OBJ_DIR, full_object_path, mcg_obj_session
        )
        written_objects = mcg_obj_session.s3_list_all_objects_in_bucket(
            self.source_bucket_name
        )

        assert set(standard_test_obj_list) == {
            obj.key for obj in written_objects
        }, "Needed uploaded objects could not be found"

        compare_bucket_object_list(
            mcg_obj_session, self.source_bucket_name, self.target_bucket_name
        )

    def setup_mcg_bucket_rep_bidirectional(
        self, bucket_factory, awscli_pod_session, mcg_obj_session, test_directory_setup
    ):
        """
        mcg bucket replication - bidirectional
        """
        self.first_bucket_name = bucket_factory(
            bucketclass={
                "interface": "OC",
                "backingstore_dict": {"aws": [(1, "eu-central-1")]},
            }
        )[0].name
        replication_policy = ("basic-replication-rule", self.first_bucket_name, None)
        self.second_bucket_name = bucket_factory(
            1,
            bucketclass={
                "interface": "OC",
                "backingstore_dict": {"azure": [(1, None)]},
            },
            replication_policy=replication_policy,
        )[0].name

        replication_policy_patch_dict = {
            "spec": {
                "additionalConfig": {
                    "replicationPolicy": json.dumps(
                        [
                            {
                                "rule_id": "basic-replication-rule-2",
                                "destination_bucket": self.second_bucket_name,
                            }
                        ]
                    )
                }
            }
        }
        OCP(
            kind="obc",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=self.first_bucket_name,
        ).patch(params=json.dumps(replication_policy_patch_dict), format_type="merge")

        standard_test_obj_list = awscli_pod_session.exec_cmd_on_pod(
            f"ls -A1 {AWSCLI_TEST_OBJ_DIR}"
        ).split(" ")

        # Write all downloaded objects to the bucket
        sync_object_directory(
            awscli_pod_session,
            AWSCLI_TEST_OBJ_DIR,
            f"s3://{self.first_bucket_name}",
            mcg_obj_session,
        )
        first_bucket_set = set(standard_test_obj_list)
        assert first_bucket_set == {
            obj.key
            for obj in mcg_obj_session.s3_list_all_objects_in_bucket(
                self.first_bucket_name
            )
        }, "Needed uploaded objects could not be found"

        compare_bucket_object_list(
            mcg_obj_session, self.first_bucket_name, self.second_bucket_name
        )
        written_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            self.second_bucket_name,
            test_directory_setup.origin_dir,
            amount=5,
            mcg_obj=mcg_obj_session,
        )
        second_bucket_set = set(written_objects)
        second_bucket_set.update(standard_test_obj_list)
        assert second_bucket_set == {
            obj.key
            for obj in mcg_obj_session.s3_list_all_objects_in_bucket(
                self.second_bucket_name
            )
        }, "Needed uploaded objects could not be found"
        compare_bucket_object_list(
            mcg_obj_session, self.first_bucket_name, self.second_bucket_name
        )

    def validate_data_integrity(self):

        # logger.info("Compare bucket replication after reboot (unidirectional)")
        # assert compare_bucket_object_list(
        #     self.mcg_obj_session, self.source_bucket_name, self.target_bucket_name
        # ), f"Failed on data validation for buckets {self.source_bucket_name} and {self.target_bucket_name}"
        #
        # logger.info("Compare bucket replication after reboot (bidirectional)")
        # compare_bucket_object_list(
        #     self.mcg_obj_session, self.first_bucket_name, self.second_bucket_name
        # )

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

        assert self.efs_pvc_orig_md5_sum == pod.cal_md5sum(
            self.efs_pvc_pod_obj, self.efs_file_name
        ), (
            f"Data integrity failed for file '{self.efs_file_name}' "
            f"on encrypted fs pvc {self.efs_pvc_obj.name} on pod {self.efs_pvc_pod_obj.name}"
        )

    @system_test
    @polarion_id("OCS-3976")
    @skipif_no_kms
    @skipif_ocs_version("<4.11")
    def test_graceful_nodes_shutdown(
        self,
        pgsql_factory_fixture,
        # couchbase_factory_fixture,
        multi_obc_lifecycle_factory,
        nodes,
    ):
        """
        Steps:
          1) Have a cluster with FIPS, hugepages, encryption enabled
          2) Create some resources: Create PVC (encrypt & non encrypt),
            take snapshot and restore it into new PVCs, clone PVC
          3) Configure OCP workloads(monitoring, registry, logging)
          4) Configure PGSQL, Couchbase, AMQ , Cosbench, Jenkin, Quay workloads
          5) Create rgw kafka notifications and objects should be notified
          6) Create normal OBCs and buckets
          7) Perform mcg bucket replication (uni and bidirectional) and see the objects are synced.
          8) Perform noobaa caching
          9) Perform S3 operations on NSFS bucket on files : put, get, delete, list objects
          10) With all the above preconditions met, follow the KCS steps for graceful shutdown of nodes
            https://access.redhat.com/articles/5394611, and bring up the cluster back.
            Once cluster up, there should not be seen any issues or impact on any data(No DU/DL/DC) and also
            normal operations should work fine.
        """
        # TODO
        # noobaa caching

        # couchbase_factory_fixture(replicas=3, run_in_bg=True, skip_analyze=True)
        # Deploy quay operator
        self.quay_operator.setup_quay_operator()

        # Deploy PGSQL workload
        logger.info("Deploying pgsql workloads")
        pgsql_factory_fixture(replicas=1)

        multi_obc_lifecycle_factory(num_of_obcs=20, bulk=True, measure=False)

        # Get list of nodes
        worker_nodes = get_nodes(node_type="worker")
        master_nodes = get_nodes(node_type="master")

        logger.info("Shutting down worker & master nodes")
        # if config.ENV_DATA["platform"].lower() == constants.VSPHERE_PLATFORM:
        nodes.stop_nodes(nodes=worker_nodes)
        nodes.start_nodes(nodes=worker_nodes)
        for m_node in master_nodes:
            nodes.stop_nodes(nodes=[m_node])
            nodes.start_nodes(nodes=[m_node])

        wait_for_cluster_connectivity(tries=400)
        Sanity().health_check(tries=60)

        self.validate_data_integrity()
