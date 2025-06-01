import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import ignore_leftovers
from ocs_ci.ocs.ocp import wait_for_cluster_connectivity
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.resources.pvc import delete_pvcs
from ocs_ci.helpers import helpers
from ocs_ci.ocs.bucket_utils import s3_delete_object, s3_get_object, s3_put_object
from ocs_ci.helpers.pvc_ops import create_pvcs
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.cluster import (
    CephCluster,
    CephClusterExternal,
    is_ms_consumer_cluster,
    is_hci_client_cluster,
    is_hci_provider_cluster,
    client_clusters_health_check,
)
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.decorators import switch_to_orig_index_at_last

logger = logging.getLogger(__name__)


class Sanity:
    """
    Class for cluster health and functional validations
    """

    def __init__(self):
        """
        Initializer for Sanity class - Init CephCluster() in order to
        set the cluster status before starting the tests
        """
        self.pvc_objs = list()
        self.pod_objs = list()
        self.obc_objs = list()
        self.obj_data = ""
        if not is_hci_client_cluster():
            self.ceph_cluster = CephCluster()

    def health_check(self, cluster_check=True, tries=20):
        """
        Perform Ceph and cluster health checks
        """
        wait_for_cluster_connectivity(tries=400)
        logger.info("Checking cluster and Ceph health")
        node.wait_for_nodes_status(timeout=300)

        if not (
            config.ENV_DATA["mcg_only_deployment"]
            or (
                config.ENV_DATA.get("platform") == constants.FUSIONAAS_PLATFORM
                and config.ENV_DATA["cluster_type"].lower() == "consumer"
            )
            or is_hci_client_cluster()
        ):
            ceph_health_check(
                namespace=config.ENV_DATA["cluster_namespace"], tries=tries
            )
            if cluster_check:
                self.ceph_cluster.cluster_health_check(timeout=120)

    def create_resources(
        self,
        pvc_factory,
        pod_factory,
        bucket_factory,
        rgw_bucket_factory,
        run_io=True,
        bucket_creation_timeout=180,
    ):
        """
        Sanity validation: Create resources - pods, OBCs (RGW and MCG), PVCs (FS and RBD) and run IO

        Args:
            pvc_factory (function): A call to pvc_factory function
            pod_factory (function): A call to pod_factory function
            bucket_factory (function): A call to bucket_factory function
            rgw_bucket_factory (function): A call to rgw_bucket_factory function
            run_io (bool): True for run IO, False otherwise
            bucket_creation_timeout (int): Time to wait for the bucket object creation.

        """
        logger.info(
            "Creating resources and running IO as a sanity functional validation"
        )

        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            pvc_obj = pvc_factory(interface)
            self.pvc_objs.append(pvc_obj)
            self.pod_objs.append(pod_factory(pvc=pvc_obj, interface=interface))
        if run_io:
            for pod in self.pod_objs:
                pod.run_io("fs", "1G", runtime=30)
            for pod in self.pod_objs:
                get_fio_rw_iops(pod)

        if rgw_bucket_factory:
            self.obc_objs.extend(rgw_bucket_factory(1, "rgw-oc"))

        if bucket_factory:
            self.obc_objs.extend(
                bucket_factory(
                    amount=1, interface="OC", timeout=bucket_creation_timeout
                )
            )

            self.ceph_cluster.wait_for_noobaa_health_ok()

    def delete_resources(self):
        """
        Sanity validation - Delete resources (pods, PVCs and OBCs)

        """
        logger.info("Deleting resources as a sanity functional validation")

        for pod_obj in self.pod_objs:
            pod_obj.delete()
        for pod_obj in self.pod_objs:
            pod_obj.ocp.wait_for_delete(pod_obj.name)
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
        for pvc_obj in self.pvc_objs:
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)
        for obc_obj in self.obc_objs:
            obc_obj.delete(), f"OBC {obc_obj.name} still exists"

    @ignore_leftovers
    def create_pvc_delete(self, multi_pvc_factory, project=None):
        """
        Creates and deletes all types of PVCs

        """
        # Create rbd pvcs
        pvc_objs_rbd = create_pvcs(
            multi_pvc_factory=multi_pvc_factory,
            interface="CephBlockPool",
            project=project,
            status="",
            storageclass=None,
        )

        # Create cephfs pvcs
        pvc_objs_cephfs = create_pvcs(
            multi_pvc_factory=multi_pvc_factory,
            interface="CephFileSystem",
            project=project,
            status="",
            storageclass=None,
        )

        all_pvc_to_delete = pvc_objs_rbd + pvc_objs_cephfs

        # Check pvc status
        for pvc_obj in all_pvc_to_delete:
            helpers.wait_for_resource_state(
                resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300
            )

        # Start deleting PVC
        delete_pvcs(all_pvc_to_delete)

        # Check PVCs are deleted
        for pvc_obj in all_pvc_to_delete:
            pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

        logger.info("All PVCs are deleted as expected")

    def obc_put_obj_create_delete(self, mcg_obj, bucket_factory, timeout=300):
        """
        Creates bucket then writes, reads and deletes objects

        """
        bucket_name = bucket_factory(
            amount=1,
            interface="OC",
            timeout=timeout,
        )[0].name
        self.obj_data = "A string data"

        for i in range(0, 30):
            key = "Object-key-" + f"{i}"
            logger.info(f"Write, read and delete object with key: {key}")
            assert s3_put_object(
                mcg_obj, bucket_name, key, self.obj_data
            ), f"Failed: Put object, {key}"
            assert s3_get_object(
                mcg_obj, bucket_name, key
            ), f"Failed: Get object, {key}"
            assert s3_delete_object(
                mcg_obj, bucket_name, key
            ), f"Failed: Delete object, {key}"


class SanityExternalCluster(Sanity):
    """
    Helpers for health check and functional validation
    in External mode
    """

    def __init__(self):
        """
        Initializer for Sanity class - Init CephCluster() in order to
        set the cluster status before starting the tests
        """
        self.pvc_objs = list()
        self.pod_objs = list()
        self.obc_objs = list()
        self.ceph_cluster = CephClusterExternal()


class SanityManagedService(Sanity):
    """
    Class for cluster health and functional validations for the Managed Service
    """

    def __init__(
        self,
        create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers,
        scale_count=None,
        pvc_per_pod_count=5,
        start_io=True,
        io_runtime=None,
        pvc_size=None,
        max_pvc_size=30,
        consumer_indexes=None,
    ):
        """
        Init the sanity managed service class.
        Init the 'create resources on MS consumers' factory.
        This method uses the 'create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers' factory.

        Args:
           create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers (function): Factory for creating scale
               pods and PVCs using k8s on MS consumers fixture.
           scale_count (int): No of PVCs to be Scaled. Should be one of the values in the dict
               "constants.SCALE_PVC_ROUND_UP_VALUE".
           pvc_per_pod_count (int): Number of PVCs to be attached to single POD
               Example, If 20 then 20 PVCs will be attached to single POD
           start_io (bool): Binary value to start IO default it's True
           io_runtime (seconds): Runtime in Seconds to continue IO
           pvc_size (int): Size of PVC to be created
           max_pvc_size (int): The max size of the pvc
           consumer_indexes (list): The list of the consumer indexes to create scale pods and PVCs.
               If not specified - if it's a consumer cluster, it creates scale pods and PVCs only
               on the current consumer. And if it's a provider it creates scale pods and PVCs on
               all the consumers.
        """
        super(SanityManagedService, self).__init__()
        # A dictionary of a consumer index per the fio_scale object
        self.consumer_i_per_fio_scale = {}

        self.create_resources_on_ms_consumers_factory = (
            create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers
        )
        self.scale_count = scale_count
        self.pvc_per_pod_count = pvc_per_pod_count
        self.start_io = start_io
        self.io_runtime = io_runtime
        self.pvc_size = pvc_size
        self.max_pvc_size = max_pvc_size

        if consumer_indexes:
            logger.info(
                f"The consumer indexes for creating resources passed to the method are: {consumer_indexes}"
            )
            self.consumer_indexes = consumer_indexes
        else:
            logger.info(
                "The consumer indexes were not passed to the method. Checking the cluster type..."
            )
            if is_ms_consumer_cluster():
                logger.info(
                    "The cluster is a consumer cluster. We will create the resources only for this cluster"
                )
                self.consumer_indexes = [config.cur_index]
            else:
                logger.info(
                    "The cluster is a provider cluster. We will create the resources for "
                    "all the consumer clusters"
                )
                self.consumer_indexes = config.get_consumer_indexes_list()

        logger.info(
            f"The consumer indexes for creating resources are: {self.consumer_indexes}"
        )

    def create_resources_on_ms_consumers(self, tries=1, delay=30):
        """
        Try creates resources for MS consumers 'tries' times with delay 'delay' between the iterations
        using the method 'base_create_resources_on_ms_consumers'. If not specified, the default value of
        'tries' is 1 - which means that by default, it only tries to create the resources once.
        In every iteration, if it fails to generate the resources, it cleans up the current resources
        before continuing to the next iteration.

        Args:
            tries (int): The number of tries to create the resources on MS consumers
            delay (int): The delay in seconds between retries

        Raises:
            ocs_ci.ocs.exceptions.CommandFailed: In case of a command failed

        """
        retry(
            (FileNotFoundError, CommandFailed),
            tries=tries,
            delay=delay,
            backoff=1,
        )(self.base_create_resources_on_ms_consumers)()

    def base_create_resources_on_ms_consumers(self):
        """
        Create resources on MS consumers.
        This function uses the factory "create_scale_pods_and_pvcs_using_kube_job_on_ms_consumers"
        for creating the resources - Create scale pods, PVCs, and run IO using a Kube job on MS consumers.
        If it fails to create the resources, it cleans up the current resources.

        Raises:
            ocs_ci.ocs.exceptions.CommandFailed: In case of a command failed

        """
        orig_index = config.cur_index
        is_success = False

        try:
            # Create the scale pods and PVCs using k8s on MS consumers factory
            self.consumer_i_per_fio_scale = (
                self.create_resources_on_ms_consumers_factory(
                    scale_count=self.scale_count,
                    pvc_per_pod_count=self.pvc_per_pod_count,
                    start_io=self.start_io,
                    io_runtime=self.io_runtime,
                    pvc_size=self.pvc_size,
                    max_pvc_size=self.max_pvc_size,
                    consumer_indexes=self.consumer_indexes,
                )
            )
            is_success = True
            logger.info("Successfully create the resources on MS consumers")
        finally:
            # Switch back to the original index
            config.switch_ctx(orig_index)
            if not is_success:
                logger.info(
                    "Failed to create the resources on MS consumers. Deleting the leftovers..."
                )
                self.delete_resources_on_ms_consumers()

    def delete_resources_on_ms_consumers(self):
        """
        Delete the resources from the MS consumers

        """
        orig_index = config.cur_index
        try:
            logger.info("Clean up the pods and PVCs from all consumers")
            for consumer_i, fio_scale in self.consumer_i_per_fio_scale.items():
                config.switch_ctx(consumer_i)
                fio_scale.cleanup()
        finally:
            # Switch back to the original index
            config.switch_ctx(orig_index)

    def health_check_ms(
        self,
        cluster_check=True,
        tries=20,
        consumers_ceph_health_check=True,
        consumers_tries=10,
    ):
        """
        Perform Ceph and cluster health checks on Managed Service cluster

        Args:
            cluster_check (bool): If true, perform the cluster check. False, otherwise.
            tries (int): The number of tries to perform ceph health check
            consumers_ceph_health_check (bool): If true and the cluster is an MS provider cluster,
                perform ceph health check on the ms consumer clusters.
            consumers_tries: The number of tries to perform ceph health check on the MS consumer clusters

        """
        orig_index = config.cur_index
        try:
            self.health_check(cluster_check=cluster_check, tries=tries)
            if consumers_ceph_health_check and not is_ms_consumer_cluster():
                # Check the ceph health on the consumers
                consumer_indexes = config.get_consumer_indexes_list()
                for consumer_i in consumer_indexes:
                    config.switch_ctx(consumer_i)
                    ceph_health_check(tries=consumers_tries)

        finally:
            config.switch_ctx(orig_index)


class SanityProviderMode(Sanity):
    """
    Class for cluster health and functional validations for the Provider mode platform
    """

    def __init__(
        self,
        create_scale_pods_and_pvcs_using_kube_job_on_hci_clients,
        scale_count=None,
        pvc_per_pod_count=5,
        start_io=True,
        io_runtime=None,
        pvc_size=None,
        max_pvc_size=30,
        client_indices=None,
    ):
        """
        Init the sanity Provider Mode class.
        Init the 'create resources on HCI clients' factory.
        This method uses the 'create_scale_pods_and_pvcs_using_kube_job_on_hci_clients' factory.

        Args:
           create_scale_pods_and_pvcs_using_kube_job_on_hci_clients (function): Factory for creating scale
               pods and PVCs using k8s on HCI clients fixture.
           scale_count (int): Number of PVCs to be Scaled. Should be one of the values in the dict
               "constants.SCALE_PVC_ROUND_UP_VALUE".
           pvc_per_pod_count (int): Number of PVCs to be attached to single POD
               Example, If 20 then 20 PVCs will be attached to single POD
           start_io (bool): Boolean value to start IO. default it's True
           io_runtime (seconds): Runtime in Seconds to continue IO
           pvc_size (int): Size of PVC to be created
           max_pvc_size (int): The max size of the pvc
           client_indices (list): The list of the client indices to create scale pods and PVCs.
               If not specified - if it's a client cluster, it creates scale pods and PVCs only
               on the current client. And if it's a provider it creates scale pods and PVCs on
               all the clients.
        """
        super(SanityProviderMode, self).__init__()
        # A dictionary of a client index per the fio_scale object
        self.client_i_per_fio_scale = {}

        self.create_resources_on_hci_clients_factory = (
            create_scale_pods_and_pvcs_using_kube_job_on_hci_clients
        )
        self.scale_count = scale_count
        self.pvc_per_pod_count = pvc_per_pod_count
        self.start_io = start_io
        self.io_runtime = io_runtime
        self.pvc_size = pvc_size
        self.max_pvc_size = max_pvc_size

        if client_indices:
            logger.info(
                f"The client indexes for creating resources passed to the method are: {client_indices}"
            )
            self.client_indices = client_indices
        else:
            logger.info(
                "The client indexes were not passed to the method. Checking the cluster type..."
            )
            if is_hci_client_cluster():
                logger.info(
                    "The cluster is a client cluster. We will create the resources only for this cluster"
                )
                self.client_indices = [config.cur_index]
            else:
                logger.info(
                    "The cluster is a provider cluster. We will create the resources for "
                    "all the client clusters"
                )
                self.client_indices = config.get_consumer_indexes_list()

        logger.info(
            f"The client indexes for creating resources are: {self.client_indices}"
        )

    def create_resources_on_clients(self, tries=1, delay=30):
        """
        Try creates resources for client 'tries' times with delay 'delay' between the iterations
        using the method 'base_create_resources_on_hci_clients'. If not specified, the default value of
        'tries' is 1 - which means that by default, it only tries to create the resources once.
        In every iteration, if it fails to generate the resources, it cleans up the current resources
        before continuing to the next iteration.

        Args:
            tries (int): The number of tries to create the resources on the clients
            delay (int): The delay in seconds between retries

        Raises:
            ocs_ci.ocs.exceptions.CommandFailed: In case of a command failed

        """
        retry(
            (FileNotFoundError, CommandFailed),
            tries=tries,
            delay=delay,
            backoff=1,
        )(self.base_create_resources_on_clients)()

    def base_create_resources_on_clients(self):
        """
        Create resources on clients.
        This function uses the factory "create_scale_pods_and_pvcs_using_kube_job_on_hci_clients"
        for creating the resources - Create scale pods, PVCs, and run IO using a Kube job on HCI clients.
        If it fails to create the resources, it cleans up the current resources.

        Raises:
            ocs_ci.ocs.exceptions.CommandFailed: In case of a command failed

        """
        orig_index = config.cur_index
        is_success = False

        try:
            # Create the scale pods and PVCs using k8s on HCI clients factory
            self.client_i_per_fio_scale = self.create_resources_on_hci_clients_factory(
                scale_count=self.scale_count,
                pvc_per_pod_count=self.pvc_per_pod_count,
                start_io=self.start_io,
                io_runtime=self.io_runtime,
                pvc_size=self.pvc_size,
                max_pvc_size=self.max_pvc_size,
                client_indexes=self.client_indices,
            )
            is_success = True
            logger.info("Successfully created the resources on the clients")
        finally:
            # Switch back to the original index
            config.switch_ctx(orig_index)
            if not is_success:
                logger.info(
                    "Failed to create the resources on the clients. Deleting the leftovers..."
                )
                self.delete_resources_on_clients()

    @switch_to_orig_index_at_last
    def delete_resources_on_clients(self):
        """
        Delete the resources from the clients

        """
        logger.info("Clean up the pods and PVCs from all clients")
        for client_i, fio_scale in self.client_i_per_fio_scale.items():
            config.switch_ctx(client_i)
            fio_scale.cleanup()

    def health_check_provider_mode(
        self,
        cluster_check=True,
        tries=20,
        run_client_clusters_health_check=True,
    ):
        """
        Perform Ceph and cluster health checks on the cluster

        Args:
            cluster_check (bool): If true, perform the cluster check. False, otherwise.
            tries (int): The number of tries to perform ceph health check
            run_client_clusters_health_check (bool): If true and the cluster is a provider cluster,
                run the cluster health check on the client clusters.

        """
        self.health_check(cluster_check=cluster_check, tries=tries)
        if run_client_clusters_health_check and is_hci_provider_cluster():
            client_clusters_health_check()
