import logging
import threading
from time import time, sleep

from ocs_ci.framework.pytest_customization.marks import (
    tier4a,
    tier2,
    red_squad,
    mcg,
)
from ocs_ci.ocs import platform_nodes
from ocs_ci.ocs.node import get_node_objs
from ocs_ci.ocs.bucket_utils import (
    write_random_objects_in_pod,
    sync_object_directory,
    verify_s3_object_integrity,
    list_objects_from_bucket,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running, get_pod_node
from ocs_ci.utility.utils import get_primary_nb_db_pod

logger = logging.getLogger(__name__)


@mcg
@red_squad
class TestNoobaaDbOps:
    """
    Test noobaa DB Postgress functionality
    """

    def perform_simultaneous_io(
        self, awscli_pod, test_directory_setup, full_object_path, mcg_obj, start_time
    ):
        logger.info(
            "Uploading, downloading and listing objects to and from the bucket simulteneously"
        )
        while (not self.stop_sig.is_set()) or (time() - start_time > 240):
            func_t1 = threading.Thread(
                target=list_objects_from_bucket,
                args=(awscli_pod, full_object_path, mcg_obj),
                daemon=True,
            )
            func_t2 = threading.Thread(
                target=sync_object_directory,
                args=(
                    awscli_pod,
                    full_object_path,
                    test_directory_setup.result_dir,
                    mcg_obj,
                ),
                daemon=True,
            )
            func_t1.start()
            func_t2.start()
            write_random_objects_in_pod(
                awscli_pod, test_directory_setup.origin_dir, 10, bs="1K"
            )
            sync_object_directory(
                awscli_pod,
                test_directory_setup.origin_dir,
                full_object_path,
                mcg_obj,
            )

    @tier4a
    def test_cnpg_repnetetive_failover(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        1. Create an MCG bucket
        2. Upload objects
        3. Delete the primary pod and wait for recovery
        4. Verify objects MD
        5. Repeat 2-4 for 5 iterations
        """

        # Create an MCG bucket
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"

        for i in range(5):
            # Upload objects
            write_random_objects_in_pod(
                awscli_pod, test_directory_setup.origin_dir, amount=i + 1
            )
            logger.info("Uploading object to the bucket")
            sync_object_directory(
                awscli_pod, test_directory_setup.origin_dir, full_object_path, mcg_obj
            )

            # Delete the primary pod and wait for recovery
            noobaa_db_pod = get_primary_nb_db_pod()
            noobaa_db_pod.delete(force=True)

            # wait for secondary DB pod to become primary pod
            res = wait_for_pods_to_be_running(
                pod_names=[noobaa_db_pod.name],
                raise_pod_not_found_error=True,
            )
            assert res, f"{noobaa_db_pod} pod is not in a Running state"

            logger.info("Downloading objects from the bucket")
            sync_object_directory(
                awscli_pod, full_object_path, test_directory_setup.result_dir, mcg_obj
            )

            # Verify objects MD
            downloaded_objs = awscli_pod.exec_cmd_on_pod(
                f"ls -A1 {test_directory_setup.result_dir}"
            ).split(" ")
            for obj in downloaded_objs:
                assert verify_s3_object_integrity(
                    original_object_path=f"{test_directory_setup.origin_dir}/{obj}",
                    result_object_path=f"{test_directory_setup.result_dir}/{obj}",
                    awscli_pod=awscli_pod,
                ), "Checksum comparision between original and result object failed"
                # clear origin_dir and result_dir for next iteration to avoid md failure
                awscli_pod.exec_cmd_on_pod(
                    f"rm -rf {test_directory_setup.origin_dir}/{obj}"
                )
                awscli_pod.exec_cmd_on_pod(
                    f"rm -rf {test_directory_setup.result_dir}/{obj}"
                )

    @tier2
    def test_cnpg_failover_pod_deletion(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Test to verify that there should not be noobaa downtime more than 3 mins
            1. Create an MCG bucket
            2. Repeatedly upload, download and list objects in thread A
            3. Delete the primary CNPG pod in thread B
            4. Verify that primary pod comes in running state within 3 mins
            5. Repeatedly upload, download and list objects in thread A
            6. Delete the secondary CNPG pod which will be the primary pod in thread B
            7. Verify that primary pod comes in running state within 3 mins
        """

        def _delete_db_pod_op():
            noobaa_db_pod = get_primary_nb_db_pod()
            noobaa_db_pod.delete(force=True)
            # wait for secondary DB pod to become primary pod
            res = wait_for_pods_to_be_running(
                pod_names=[noobaa_db_pod.name],
                raise_pod_not_found_error=True,
                timeout=180,
                sleep=10,
            )
            assert res, f"{noobaa_db_pod} pod is not in a Running state"
            self.stop_sig.set()

        # Create an MCG bucket
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"
        for _ in range(2):
            self.stop_sig = threading.Event()
            write_random_objects_in_pod(
                awscli_pod, test_directory_setup.origin_dir, 10, bs="1K"
            )
            sync_object_directory(
                awscli_pod, test_directory_setup.origin_dir, full_object_path, mcg_obj
            )
            start_time = time()
            t1 = threading.Thread(
                target=self.perform_simultaneous_io,
                args=(
                    awscli_pod,
                    test_directory_setup,
                    full_object_path,
                    mcg_obj,
                    start_time,
                ),
            )
            t1.start()
            # Delete the primary pod in second thread
            t2 = threading.Thread(target=_delete_db_pod_op)
            sleep(30)
            t2.start()
            t1.join()
            t2.join()

    @tier4a
    def test_cnpg_failover_node_failure(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Test to verify that there should not be noobaa downtime more than 3 mins after node reboot
           1. Create an MCG bucket
           2. Repeatedly upload, download and delete objects in threadA
           3. Restart the node that hosts the CNPG primary pod
           4. Verify DB pod is scheduled in another node"
        """

        def _restart_db_pod_node():
            """
            Function to restart node which has noobaa pod's running

            Args:
                pod_name (str): Name of noobaa pod

            """
            noobaa_db_pod = get_primary_nb_db_pod()
            nb_node_name = get_pod_node(noobaa_db_pod).name
            factory = platform_nodes.PlatformNodesFactory()
            nodes = factory.get_nodes_platform()
            nb_nodes = get_node_objs(node_names=nb_node_name)
            logger.info(f"{noobaa_db_pod.name} is running on {nb_node_name}")
            logger.info(f"Restating node: {nb_node_name}....")
            nodes.restart_nodes_by_stop_and_start(nodes=nb_nodes, force=True)

            res = wait_for_pods_to_be_running(
                pod_names=[noobaa_db_pod.name],
                raise_pod_not_found_error=True,
                timeout=180,
                sleep=10,
            )
            assert res, f"{noobaa_db_pod} pod is not in a Running state"
            self.stop_sig.set()

        # Create an MCG bucket
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"
        self.stop_sig = threading.Event()
        write_random_objects_in_pod(
            awscli_pod, test_directory_setup.origin_dir, 10, bs="1K"
        )
        sync_object_directory(
            awscli_pod, test_directory_setup.origin_dir, full_object_path, mcg_obj
        )
        start_time = time()
        t1 = threading.Thread(
            target=self.perform_simultaneous_io,
            args=(
                awscli_pod,
                test_directory_setup,
                full_object_path,
                mcg_obj,
                start_time,
            ),
        )
        t1.start()
        # Restart the node where primary pod is running in second thread
        t2 = threading.Thread(target=_restart_db_pod_node)
        sleep(10)
        t2.start()
        t1.join()
        t2.join()
