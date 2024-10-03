import logging

import random
import copy
import re
import time

from uuid import uuid4
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.bucket_utils import (
    random_object_round_trip_verification,
    write_random_test_objects_to_bucket,
    wait_for_cache,
    sync_object_directory,
    verify_s3_object_integrity,
    s3_put_object,
    expire_objects_in_bucket,
    sample_if_objects_expired,
)
from ocs_ci.ocs.resources.pod import get_rgw_pods, get_pod_logs
from ocs_ci.utility.utils import exec_cmd, run_cmd
from ocs_ci.ocs import constants
from ocs_ci.utility.kms import is_kms_enabled
from ocs_ci.ocs.constants import DEFAULT_NOOBAA_BUCKETCLASS, DEFAULT_NOOBAA_BACKINGSTORE
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_noobaa_pods
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.framework import config
from ocs_ci.helpers import helpers
from ocs_ci.ocs.benchmark_operator_fio import get_file_size
from ocs_ci.ocs.benchmark_operator_fio import BenchmarkOperatorFIO
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.utility.utils import TimeoutSampler, ceph_health_check
from ocs_ci.ocs.resources.pod import cal_md5sum
from ocs_ci.ocs.cluster import (
    change_ceph_full_ratio,
    get_percent_used_capacity,
    get_osd_utilization,
    get_ceph_df_detail,
)

logger = logging.getLogger(__name__)


def create_muliple_types_provider_obcs(
    num_of_buckets, bucket_types, cloud_providers, bucket_factory
):
    """
    This function creates valid OBCs of different cloud providers
    and bucket types

    Args:
        num_of_buckets (int): Number of buckets
        bucket_types (dict): Dict representing mapping between
            bucket type and relevant configuration
        cloud_providers (dict): Dict representing mapping between
            cloud providers and relevant configuration
        bucket_factory (fixture): bucket_factory fixture method

    Returns:
        List: list of created buckets

    """

    def get_all_combinations_map(providers, bucket_types):
        """
        Create valid combination of cloud-providers and bucket-types

        Args:
            providers (dict): dictionary representing cloud
                providers and the respective config
            bucket_types (dict): dictionary representing different
                types of bucket and the respective config
        Returns:
            List: containing all the possible combination of buckets

        """
        all_combinations = dict()

        for provider, provider_config in providers.items():
            for bucket_type, type_config in bucket_types.items():
                if provider == "pv" and bucket_type != "data":
                    available_providers = [
                        key for key in cloud_providers.keys() if key != "pv"
                    ]
                    if available_providers:
                        provider = random.choice(available_providers)
                    else:
                        # If 'pv' is the only available provider, choose between 'aws' and 'azure'
                        provider = random.choice(["aws", "azure"])
                    provider_config = providers[provider]
                bucketclass = copy.deepcopy(type_config)

                if "backingstore_dict" in bucketclass.keys():
                    bucketclass["backingstore_dict"][provider] = [provider_config]
                elif "namespace_policy_dict" in bucketclass.keys():
                    bucketclass["namespace_policy_dict"]["namespacestore_dict"][
                        provider
                    ] = [provider_config]
                all_combinations.update({f"{bucket_type}-{provider}": bucketclass})
        return all_combinations

    all_combination_of_obcs = get_all_combinations_map(cloud_providers, bucket_types)
    buckets = list()
    num_of_buckets_each = num_of_buckets // len(all_combination_of_obcs.keys())
    buckets_left = num_of_buckets % len(all_combination_of_obcs.keys())
    if num_of_buckets_each != 0:
        for combo, combo_config in all_combination_of_obcs.items():
            buckets.extend(
                bucket_factory(
                    interface="OC",
                    amount=num_of_buckets_each,
                    bucketclass=combo_config,
                )
            )

    for index in range(0, buckets_left):
        buckets.extend(
            bucket_factory(
                interface="OC",
                amount=1,
                bucketclass=all_combination_of_obcs[
                    list(all_combination_of_obcs.keys())[index]
                ],
            )
        )

    return buckets


def validate_mcg_bucket_replicaton(
    awscli_pod_session,
    mcg_obj_session,
    source_target_map,
    uploaded_objects_dir,
    downloaded_obejcts_dir,
    event,
    run_in_bg=False,
    object_amount=5,
):
    """
    Validate MCG bucket replication feature

    Args:
        awscli_pod_session (Pod): Pod object representing aws-cli pod
        mcg_obj_session (MCG): MCG object
        source_target_map (Dict): Dictionary consisting of source - target buckets
        uploaded_objects_dir (str): directory where uploaded objects are kept
        downloaded_obejcts_dir (str): directory where downloaded objects are kept
        event (threading.Event()): Event() object
        run_in_bg (bool): If True, validation is run in background
        object_amount (int): Amounts of objects

    """
    bidi_uploaded_objs_dir_1 = uploaded_objects_dir + "/bidi_1"
    bidi_uploaded_objs_dir_2 = uploaded_objects_dir + "/bidi_2"
    bidi_downloaded_objs_dir_1 = downloaded_obejcts_dir + "/bidi_1"
    bidi_downloaded_objs_dir_2 = downloaded_obejcts_dir + "/bidi_2"

    # Verify replication is working as expected by performing a two-way round-trip object verification
    while True:
        for first_bucket, second_bucket in source_target_map.items():
            random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=first_bucket.name,
                upload_dir=bidi_uploaded_objs_dir_1,
                download_dir=bidi_downloaded_objs_dir_1,
                amount=object_amount,
                pattern=f"FirstBiDi-{uuid4().hex}",
                prefix="bidi_1",
                wait_for_replication=True,
                second_bucket_name=second_bucket.name,
                mcg_obj=mcg_obj_session,
                cleanup=True,
                timeout=1200,
            )

            random_object_round_trip_verification(
                io_pod=awscli_pod_session,
                bucket_name=second_bucket.name,
                upload_dir=bidi_uploaded_objs_dir_2,
                download_dir=bidi_downloaded_objs_dir_2,
                amount=object_amount,
                pattern=f"SecondBiDi-{uuid4().hex}",
                prefix="bidi_2",
                wait_for_replication=True,
                second_bucket_name=first_bucket.name,
                mcg_obj=mcg_obj_session,
                cleanup=True,
                timeout=1200,
            )
            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.info("Verified bi-direction replication successfully")
            logger.warning("Stopping bi-direction replication verification")
            break
        time.sleep(30)


def validate_mcg_caching(
    awscli_pod_session,
    mcg_obj_session,
    cld_mgr,
    cache_buckets,
    uploaded_objects_dir,
    downloaded_obejcts_dir,
    event,
    run_in_bg=False,
):
    """
    Validate noobaa caching feature against the cache buckets

    Args:
        awscli_pod_session (Pod): Pod object representing aws-cli pod
        mcg_obj_session (MCG): MCG object
        cld_mgr (cld_mgr): cld_mgr object
        cache_buckets (List): List consisting of cache buckets
        uploaded_objects_dir (str): directory where uploaded objects are kept
        downloaded_obejcts_dir (str): directory where downloaded objects are kept
        event (threading.Event()): Event() object
        run_in_bg (bool): If True, validation is run in background

    """
    while True:
        for bucket in cache_buckets:
            cache_uploaded_objs_dir = uploaded_objects_dir + "/cache"
            cache_uploaded_objs_dir_2 = uploaded_objects_dir + "/cache_2"
            cache_downloaded_objs_dir = downloaded_obejcts_dir + "/cache"
            underlying_bucket_name = bucket.bucketclass.namespacestores[0].uls_name

            # Upload a random object to the bucket
            logger.info(f"Uploading to the cache bucket: {bucket.name}")
            obj_name = f"Cache-{uuid4().hex}"
            objs_written_to_cache_bucket = write_random_test_objects_to_bucket(
                awscli_pod_session,
                bucket.name,
                cache_uploaded_objs_dir,
                pattern=obj_name,
                mcg_obj=mcg_obj_session,
            )
            wait_for_cache(
                mcg_obj_session,
                bucket.name,
                objs_written_to_cache_bucket,
                timeout=300,
            )

            # Write a random, larger object directly to the underlying storage of the bucket
            logger.info(
                f"Uploading to the underlying bucket {underlying_bucket_name} directly"
            )
            write_random_test_objects_to_bucket(
                awscli_pod_session,
                underlying_bucket_name,
                cache_uploaded_objs_dir_2,
                pattern=obj_name,
                s3_creds=cld_mgr.aws_client.nss_creds,
                bs="2M",
            )

            # Download the object from the cache bucket
            awscli_pod_session.exec_cmd_on_pod(f"mkdir -p {cache_downloaded_objs_dir}")
            sync_object_directory(
                awscli_pod_session,
                f"s3://{bucket.name}",
                cache_downloaded_objs_dir,
                mcg_obj_session,
            )

            assert verify_s3_object_integrity(
                original_object_path=f"{cache_uploaded_objs_dir}/{obj_name}0",
                result_object_path=f"{cache_downloaded_objs_dir}/{obj_name}0",
                awscli_pod=awscli_pod_session,
            ), "The uploaded and downloaded cached objects have different checksums"

            assert (
                verify_s3_object_integrity(
                    original_object_path=f"{cache_uploaded_objs_dir_2}/{obj_name}0",
                    result_object_path=f"{cache_downloaded_objs_dir}/{obj_name}0",
                    awscli_pod=awscli_pod_session,
                )
                is False
            ), "The cached object was replaced by the new one before the TTL has expired"
            logger.info(f"Verified caching for bucket: {bucket.name}")

            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.warning("Stopping noobaa caching verification")
            break
        time.sleep(30)


def validate_rgw_kafka_notification(kafka_rgw_dict, event, run_in_bg=False):
    """
    Validate kafka notifications for RGW buckets

    Args:
        kafka_rgw_dict (Dict): Dict consisting of rgw bucket,
        kafka_topic, kafkadrop_host etc
        event (threading.Event()): Event() object
        run_in_bg (Bool): True if you want to run in the background

    """
    s3_client = kafka_rgw_dict["s3client"]
    bucketname = kafka_rgw_dict["kafka_rgw_bucket"]
    notify_cmd = kafka_rgw_dict["notify_cmd"]
    data = kafka_rgw_dict["data"]
    kafkadrop_host = kafka_rgw_dict["kafkadrop_host"]
    kafka_topic = kafka_rgw_dict["kafka_topic"]

    while True:
        data = data + f"{uuid4().hex}"

        def put_object_to_bucket(bucket_name, key, body):
            return s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

        assert put_object_to_bucket(
            bucketname, "key-1", data
        ), "Failed: Put object: key-1"
        exec_cmd(notify_cmd)

        # Validate rgw logs notification are sent
        # No errors are seen
        pattern = "ERROR: failed to create push endpoint"
        rgw_pod_obj = get_rgw_pods()
        rgw_log = get_pod_logs(pod_name=rgw_pod_obj[0].name, container="rgw")
        assert re.search(pattern=pattern, string=rgw_log) is None, (
            f"Error: {pattern} msg found in the rgw logs."
            f"Validate {pattern} found on rgw logs and also "
            f"rgw bucket notification is working correctly"
        )
        assert put_object_to_bucket(
            bucketname, "key-2", data
        ), "Failed: Put object: key-2"
        exec_cmd(notify_cmd)

        # Validate message are received Kafka side using curl command
        # A temporary way to check from Kafka side, need to check from UI
        @retry(Exception, tries=5, delay=5)
        def validate_kafa_for_message():
            curl_command = (
                f"curl -X GET {kafkadrop_host}/topic/{kafka_topic.name} "
                "-H 'content-type: application/vnd.kafka.json.v2+json'"
            )
            json_output = run_cmd(cmd=curl_command)
            # logger.info("Json output:" f"{json_output}")
            new_string = json_output.split()
            messages = new_string[new_string.index("messages</td>") + 1]
            logger.info("Messages:" + str(messages))
            if messages.find("1") == -1:
                raise Exception(
                    "Error: Messages are not recieved from Kafka side."
                    "RGW bucket notification is not working as expected."
                )

        validate_kafa_for_message()

        if event.is_set() or not run_in_bg:
            logger.warning("Stopping kafka rgw notification verification")
            break
        time.sleep(30)


def validate_mcg_object_expiration(
    mcg_obj,
    buckets,
    event,
    run_in_bg=False,
    object_amount=5,
):
    """
    Validates objects expiration for MCG buckets

    Args:
        mcg_obj (MCG): MCG object
        buckets (List): List of MCG buckets
        event (threading.Event()): Event() object
        run_in_bg (Bool): True if wants to run in background
        object_amount (Int): Amount of objects
        prefix (str): Any prefix used for objects

    """
    while True:
        for bucket in buckets:

            for i in range(object_amount):
                s3_put_object(
                    mcg_obj,
                    bucket.name,
                    f"obj-key-{uuid4().hex}",
                    "Some random data",
                )
            expire_objects_in_bucket(bucket.name)
            sample_if_objects_expired(mcg_obj, bucket.name)
            if event.is_set():
                run_in_bg = False
                break

        if not run_in_bg:
            logger.warning("Stopping MCG object expiration verification")
            break
        time.sleep(30)


def validate_mcg_nsfs_feature():
    logger.info("This is not implemented")


def validate_noobaa_rebuild_system(self, bucket_factory_session, mcg_obj_session):
    """
    This function is to verify noobaa rebuild. Verifies KCS: https://access.redhat.com/solutions/5948631

    1. Stop the noobaa-operator by setting the replicas of noobaa-operator deployment to 0.
    2. Delete the noobaa deployments/statefulsets.
    3. Delete the PVC db-noobaa-db-0.
    4. Patch existing backingstores and bucketclasses to remove finalizer
    5. Delete the backingstores/bucketclass.
    6. Delete the noobaa secrets.
    7. Restart noobaa-operator by setting the replicas back to 1.
    8. Monitor the pods in openshift-storage for noobaa pods to be Running.

    """

    dep_ocp = OCP(
        kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
    )
    state_ocp = OCP(
        kind=constants.STATEFULSET, namespace=config.ENV_DATA["cluster_namespace"]
    )
    noobaa_pvc_obj = get_pvc_objs(pvc_names=["db-noobaa-db-pg-0"])

    # Scale down noobaa operator
    logger.info(
        f"Scaling down {constants.NOOBAA_OPERATOR_DEPLOYMENT} deployment to replica: 0"
    )
    dep_ocp.exec_oc_cmd(
        f"scale deployment {constants.NOOBAA_OPERATOR_DEPLOYMENT} --replicas=0"
    )

    # Delete noobaa deployments and statefulsets
    logger.info("Deleting noobaa deployments and statefulsets")
    dep_ocp.delete(resource_name=constants.NOOBAA_ENDPOINT_DEPLOYMENT)
    state_ocp.delete(resource_name=constants.NOOBAA_DB_STATEFULSET)
    state_ocp.delete(resource_name=constants.NOOBAA_CORE_STATEFULSET)

    # Delete noobaa-db pvc
    pvc_obj = OCP(kind=constants.PVC, namespace=config.ENV_DATA["cluster_namespace"])
    logger.info("Deleting noobaa-db pvc")
    pvc_obj.delete(resource_name=noobaa_pvc_obj[0].name, wait=True)
    pvc_obj.wait_for_delete(resource_name=noobaa_pvc_obj[0].name, timeout=300)

    # Patch and delete existing backingstores
    params = '{"metadata": {"finalizers":null}}'
    bs_obj = OCP(
        kind=constants.BACKINGSTORE, namespace=config.ENV_DATA["cluster_namespace"]
    )
    for bs in bs_obj.get()["items"]:
        assert bs_obj.patch(
            resource_name=bs["metadata"]["name"],
            params=params,
            format_type="merge",
        ), "Failed to change the parameter in backingstore"
        logger.info(f"Deleting backingstore: {bs['metadata']['name']}")
        bs_obj.delete(resource_name=bs["metadata"]["name"])

    # Patch and delete existing bucketclass
    bc_obj = OCP(
        kind=constants.BUCKETCLASS, namespace=config.ENV_DATA["cluster_namespace"]
    )
    for bc in bc_obj.get()["items"]:
        assert bc_obj.patch(
            resource_name=bc["metadata"]["name"],
            params=params,
            format_type="merge",
        ), "Failed to change the parameter in bucketclass"
        logger.info(f"Deleting bucketclass: {bc['metadata']['name']}")
        bc_obj.delete(resource_name=bc["metadata"]["name"])

    # Delete noobaa secrets
    logger.info("Deleting noobaa related secrets")
    if is_kms_enabled():
        dep_ocp.exec_oc_cmd(
            "delete secrets noobaa-admin noobaa-endpoints noobaa-operator noobaa-server"
        )
    else:
        dep_ocp.exec_oc_cmd(
            "delete secrets noobaa-admin noobaa-endpoints noobaa-operator "
            "noobaa-server noobaa-root-master-key-backend noobaa-root-master-key-volume"
        )

    # Scale back noobaa-operator deployment
    logger.info(
        f"Scaling back {constants.NOOBAA_OPERATOR_DEPLOYMENT} deployment to replica: 1"
    )
    dep_ocp.exec_oc_cmd(
        f"scale deployment {constants.NOOBAA_OPERATOR_DEPLOYMENT} --replicas=1"
    )

    # Wait and validate noobaa PVC is in bound state
    pvc_obj.wait_for_resource(
        condition=constants.STATUS_BOUND,
        resource_name=noobaa_pvc_obj[0].name,
        timeout=600,
        sleep=120,
    )

    # Validate noobaa pods are up and running
    pod_obj = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    noobaa_pods = get_noobaa_pods()
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        resource_count=len(noobaa_pods),
        selector=constants.NOOBAA_APP_LABEL,
        timeout=900,
    )

    # Verify everything running fine
    logger.info("Verifying all resources are Running and matches expected result")
    self.sanity_helpers.health_check(tries=120)

    # Since the rebuild changed the noobaa-admin secret, update
    # the s3 credentials in mcg_object_session
    mcg_obj_session.update_s3_creds()

    # Verify default backingstore/bucketclass
    default_bs = OCP(
        kind=constants.BACKINGSTORE, namespace=config.ENV_DATA["cluster_namespace"]
    ).get(resource_name=DEFAULT_NOOBAA_BACKINGSTORE)
    default_bc = OCP(
        kind=constants.BUCKETCLASS, namespace=config.ENV_DATA["cluster_namespace"]
    ).get(resource_name=DEFAULT_NOOBAA_BUCKETCLASS)
    assert (
        default_bs["status"]["phase"]
        == default_bc["status"]["phase"]
        == constants.STATUS_READY
    ), "Failed: Default bs/bc are not in ready state"

    # Create OBCs
    logger.info("Creating OBCs after noobaa rebuild")
    bucket_factory_session(amount=3, interface="OC", verify_health=True)


def validate_noobaa_db_backup_recovery_locally_system(
    self,
    bucket_factory_session,
    noobaa_db_backup_and_recovery_locally,
    warps3,
    mcg_obj_session,
):
    """
    Test to verify Backup and Restore for Multicloud Object Gateway database locally
    Backup procedure:
    1. Create a test bucket and write some data
    2. Backup noobaa secrets to local folder OR store it in secret objects
    3. Backup the PostgreSQL database and save it to a local folder
    4. For testing, write new data to show a little data loss between backup and restore
    Restore procedure:
    1. Stop MCG reconciliation
    2. Stop the NooBaa Service before restoring the NooBaa DB. There will be no object service after this point
    3. Verify that all NooBaa components (except NooBaa DB) have 0 replicas
    4. Login to the NooBaa DB pod and cleanup potential database clients to nbcore
    5. Restore DB from a local folder
    6. Delete current noobaa secrets and restore them from a local folder OR secrets objects.
    7. Restore MCG reconciliation
    8. Start the NooBaa service
    9. Restart the NooBaa DB pod
    10. Check that the old data exists, but not s3://testloss/
    Run multi client warp benchmarking to verify bug https://bugzilla.redhat.com/show_bug.cgi?id=2141035

    """

    # create a bucket for warp benchmarking
    bucket_name = bucket_factory_session()[0].name

    # Backup and restore noobaa db using fixture
    noobaa_db_backup_and_recovery_locally(bucket_factory_session)

    # Verify everything running fine
    logger.info("Verifying all resources are Running and matches expected result")
    self.sanity_helpers.health_check(tries=120)

    # Run multi client warp benchmarking
    warps3.run_benchmark(
        bucket_name=bucket_name,
        access_key=mcg_obj_session.access_key_id,
        secret_key=mcg_obj_session.access_key,
        duration="10m",
        concurrent=10,
        objects=100,
        obj_size="1MiB",
        validate=True,
        timeout=4000,
        multi_client=True,
        tls=True,
        debug=True,
        insecure=True,
    )

    # make sure no errors in the noobaa pod logs
    search_string = (
        "AssertionError [ERR_ASSERTION]: _id must be unique. "
        "found 2 rows with _id=undefined in table bucketstats"
    )
    nb_pods = get_noobaa_pods()
    for pod in nb_pods:
        pod_logs = get_pod_logs(pod_name=pod.name)
        for line in pod_logs:
            assert (
                search_string not in line
            ), f"[Error] {search_string} found in the noobaa pod logs"
    logger.info(f"No {search_string} errors are found in the noobaa pod logs")


class Run_fio_till_cluster_full:
    """
    Run fio from multiple pods to fill cluster 85% of raw capacity.
    """

    def cleanup(self):
        if self.benchmark_operator_teardown:
            change_ceph_full_ratio(95)
            self.benchmark_obj.cleanup()
            ceph_health_check(tries=30, delay=60)
        change_ceph_full_ratio(85)

    def run_cluster_full_fio(
        self,
        teardown_project_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        1.Create PVC1 [FS + RBD]
        2.Verify new PVC1 [FS + RBD] on Bound state
        3.Run FIO on PVC1_FS + PVC1_RBD
        4.Calculate Checksum PVC1_FS + PVC1_RBD
        5.Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator
        """
        self.benchmark_operator_teardown = False
        project_name = "system-test-fullcluster"
        self.project_obj = helpers.create_project(project_name=project_name)
        teardown_project_factory(self.project_obj)

        logger.info("Create PVC1 CEPH-RBD, Run FIO and get checksum")
        pvc_obj_rbd1 = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=self.project_obj,
            size=2,
            status=constants.STATUS_BOUND,
        )
        pod_rbd1_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj_rbd1,
            status=constants.STATUS_RUNNING,
        )
        pod_rbd1_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            runtime=60,
        )
        pod_rbd1_obj.get_fio_results()
        logger.info(f"IO finished on pod {pod_rbd1_obj.name}")
        pod_rbd1_obj.md5 = cal_md5sum(
            pod_obj=pod_rbd1_obj,
            file_name="fio-rand-write",
            block=False,
        )

        logger.info("Create PVC1 CEPH-FS, Run FIO and get checksum")
        pvc_obj_fs1 = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            project=self.project_obj,
            size=2,
            status=constants.STATUS_BOUND,
        )
        pod_fs1_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj_fs1,
            status=constants.STATUS_RUNNING,
        )
        pod_fs1_obj.run_io(
            storage_type="fs",
            size="1G",
            io_direction="write",
            runtime=60,
        )
        pod_fs1_obj.get_fio_results()
        logger.info(f"IO finished on pod {pod_fs1_obj.name}")
        pod_fs1_obj.md5 = cal_md5sum(
            pod_obj=pod_fs1_obj,
            file_name="fio-rand-write",
            block=False,
        )

        logger.info(
            "Fill the cluster to “Full ratio” (usually 85%) with benchmark-operator"
        )
        size = get_file_size(100)
        self.benchmark_obj = BenchmarkOperatorFIO()
        self.benchmark_obj.setup_benchmark_fio(total_size=size)
        self.benchmark_obj.run_fio_benchmark_operator(is_completed=False)
        self.benchmark_operator_teardown = True

        logger.info("Verify used capacity bigger than 85%")
        sample = TimeoutSampler(
            timeout=2500,
            sleep=40,
            func=verify_osd_used_capacity_greater_than_expected,
            expected_used_capacity=85.0,
        )
        if not sample.wait_for_func_status(result=True):
            logger.error("The after 1800 seconds the used capacity smaller than 85%")
            raise TimeoutExpiredError


def verify_osd_used_capacity_greater_than_expected(expected_used_capacity):
    """
    Verify OSD percent used capacity greate than ceph_full_ratio

    Args:
        expected_used_capacity (float): expected used capacity

    Returns:
         bool: True if used_capacity greater than expected_used_capacity, False otherwise

    """
    used_capacity = get_percent_used_capacity()
    logger.info(f"Used Capacity is {used_capacity}%")
    ceph_df_detail = get_ceph_df_detail()
    logger.info(f"ceph df detail: {ceph_df_detail}")
    osds_utilization = get_osd_utilization()
    logger.info(f"osd utilization: {osds_utilization}")
    for osd_id, osd_utilization in osds_utilization.items():
        if osd_utilization > expected_used_capacity:
            logger.info(f"OSD ID:{osd_id}:{osd_utilization} greater than 85%")
            return True
    return False
