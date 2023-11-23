"""
Couchbase workload class
"""
import json
import logging
import random
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime

from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.ocs.resources.csv import CSV
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP, switch_to_project
from ocs_ci.utility import templating
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.pillowfight import PillowFight
from ocs_ci.ocs.ocp import switch_to_default_rook_cluster_project
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.helpers.helpers import (
    wait_for_resource_state,
    storagecluster_independent_check,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs import constants
from ocs_ci.framework import config


log = logging.getLogger(__name__)


class CouchBase(PillowFight):
    """
    CouchBase workload operation
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        """
        super().__init__(**kwargs)
        self.args = kwargs
        self.pod_obj = OCP(kind="pod", namespace=constants.COUCHBASE_OPERATOR)
        self.ns_obj = OCP(kind="namespace")
        self.couchbase_pod = OCP(kind="pod")
        self.create_namespace(namespace=constants.COUCHBASE_OPERATOR)
        self.cb_create_cb_secret = False
        self.cb_create_cb_cluster = False
        self.cb_create_bucket = False
        self.cb_subscription = False

    def create_namespace(self, namespace):
        """
        create namespace for couchbase

        Args:
            namespace (str): Namespace for deploying couchbase pods

        """
        try:
            self.ns_obj.new_project(namespace)
        except CommandFailed as ef:
            log.info("Already present")
            if f'project.project.openshift.io "{namespace}" already exists' not in str(
                ef
            ):
                raise ef

    def couchbase_operatorgroup(self):
        """
        Creates an operator group for Couchbase

        """
        operatorgroup_yaml = templating.load_yaml(
            constants.COUCHBASE_OPERATOR_GROUP_YAML
        )
        self.operatorgroup_yaml = OCS(**operatorgroup_yaml)
        self.operatorgroup_yaml.create()

    def couchbase_subscription(self):
        """
        Creates subscription for Couchbase operator

        """
        # Create an operator group for Couchbase
        log.info("Creating operator group for couchbase")
        self.couchbase_operatorgroup()
        subscription_yaml = templating.load_yaml(
            constants.COUCHBASE_OPERATOR_SUBSCRIPTION_YAML
        )
        self.subscription_yaml = OCS(**subscription_yaml)
        self.subscription_yaml.create()
        self.cb_subscription = False

        # Wait for the CSV to reach succeeded state
        cb_csv = self.get_couchbase_csv()
        cb_csv_obj = CSV(resource_name=cb_csv, namespace=constants.COUCHBASE_OPERATOR)
        cb_csv_obj.wait_for_phase("Succeeded", timeout=720)

    def get_couchbase_csv(self):
        """ "
        Get the Couchbase CSV object

        Returns:
            CSV: Couchbase CSV object

        Raises:
            CSVNotFound: In case no CSV found.

        """
        cb_package_manifest = PackageManifest(
            resource_name="couchbase-enterprise-certified"
        )
        cb_enter_csv = cb_package_manifest.get_current_csv(
            channel="stable", csv_pattern=constants.COUCHBASE_CSV_PREFIX
        )
        return cb_enter_csv

    def create_cb_secrets(self):
        """ "
        Create secrets for running Couchbase workers

        """
        cb_secrets = templating.load_yaml(constants.COUCHBASE_WORKER_SECRET)
        self.cb_secrets = OCS(**cb_secrets)
        self.cb_secrets.create()
        log.info("Successfully created secrets for Couchbase")
        self.cb_create_cb_secret = True

    def create_cb_cluster(self, replicas=1, sc_name=None, image=None):
        """
        Deploy a Couchbase server using Couchbase operator

        Once the couchbase operator is running, we need to wait for the
        worker pods to be up.  Once the Couchbase worker pods are up, pillowfight
        task is started.

        After the pillowfight task has finished, the log is collected and
        analyzed.

        Raises:
            Exception: If pillowfight results indicate that a minimum performance
                level is not reached (1 second response time, less than 1000 ops
                per second)

        """
        log.info("Creating Couchbase worker pods...")
        cb_example = templating.load_yaml(constants.COUCHBASE_WORKER_EXAMPLE)
        if not image:
            cb_package_manifest = PackageManifest(
                resource_name="couchbase-enterprise-certified"
            )
            data = json.loads(
                cb_package_manifest.get()["status"]["channels"][0]["currentCSVDesc"][
                    "annotations"
                ]["alm-examples"]
            )
            cb_example["spec"]["image"] = data[0]["spec"]["image"]
        if (
            storagecluster_independent_check()
            and config.ENV_DATA["platform"].lower()
            not in constants.HCI_PC_OR_MS_PLATFORM
        ):
            cb_example["spec"]["volumeClaimTemplates"][0]["spec"][
                "storageClassName"
            ] = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
        cb_example["spec"]["servers"][0]["size"] = replicas
        if sc_name:
            cb_example["spec"]["volumeClaimTemplates"][0]["spec"][
                "storageClassName"
            ] = sc_name
        self.cb_example = OCS(**cb_example)
        self.cb_example.create()
        self.cb_create_cb_cluster = True

        # Wait for the Couchbase workers to be running.

        log.info("Waiting for the Couchbase pods to be Running")
        self.pod_obj.wait_for_resource(
            condition="Running",
            selector="app=couchbase",
            resource_count=replicas,
            timeout=900,
        )
        log.info(
            f"Expected number: {replicas} of couchbase workers reached running state"
        )

    def create_data_buckets(self):
        """
        Create data buckets

        """
        cb_bucket = templating.load_yaml(constants.COUCHBASE_DATA_BUCKET)
        self.cb_bucket = OCS(**cb_bucket)
        self.cb_bucket.create()
        log.info("Successfully created data buckets")
        self.cb_create_bucket = True

    def run_workload(
        self,
        replicas,
        num_items=None,
        num_threads=None,
        num_of_cycles=None,
        run_in_bg=False,
    ):
        """
        Running workload with pillow fight operator
        Args:
            replicas (int): Number of pods
            num_items (int): Number of items to be loaded to the cluster
            num_threads (int): Number of threads
            num_of_cycles (int): Specify the number of times the workload should cycle
            run_in_bg (bool) : Optional run IOs in background

        """
        self.result = None
        log.info("Running IOs using Pillow-fight")
        if run_in_bg:
            executor = ThreadPoolExecutor(1)
            self.result = executor.submit(
                PillowFight.run_pillowfights,
                self,
                replicas=replicas,
                num_items=num_items,
                num_threads=num_threads,
                num_of_cycles=num_of_cycles,
            )
            return self.result
        PillowFight.run_pillowfights(
            self,
            replicas=replicas,
            num_items=num_items,
            num_threads=num_threads,
            num_of_cycles=num_of_cycles,
        )

    def wait_for_pillowfights_to_complete(self, timeout=3600):
        """
        Wait for the pillowfight workload to complete
        """
        PillowFight.wait_for_pillowfights_to_complete(self, timeout=timeout)

    def analyze_run(self, skip_analyze=False):
        """
        Analyzing the workload run logs

        Args:
            skip_analyze (bool): Option to skip logs analysis

        """
        if not skip_analyze:
            log.info("Analyzing  workload run logs..")
            PillowFight.analyze_all(self)

    def respin_couchbase_app_pod(self):
        """
        Respin the couchbase app pod

        Returns:
            pod status

        """
        app_pod_list = get_pod_name_by_pattern(
            "cb-example", constants.COUCHBASE_OPERATOR
        )
        app_pod = app_pod_list[random.randint(0, len(app_pod_list) - 1)]
        log.info(f"respin pod {app_pod}")
        app_pod_obj = get_pod_obj(app_pod, namespace=constants.COUCHBASE_OPERATOR)
        app_pod_obj.delete(wait=True, force=False)
        wait_for_resource_state(
            resource=app_pod_obj, state=constants.STATUS_RUNNING, timeout=300
        )

    def get_couchbase_nodes(self):
        """
        Get nodes that contain a couchbase app pod

        Returns:
            list: List of nodes

        """
        app_pods_list = get_pod_name_by_pattern(
            "cb-example", constants.COUCHBASE_OPERATOR
        )
        app_pod_objs = list()
        for pod in app_pods_list:
            app_pod_objs.append(
                get_pod_obj(pod, namespace=constants.COUCHBASE_OPERATOR)
            )
        log.info("Create a list of nodes that contain a couchbase app pod")
        nodes_set = set()
        for pod in app_pod_objs:
            log.info(
                f"pod {pod.name} located on "
                f"node {pod.get().get('spec').get('nodeName')}"
            )
            nodes_set.add(pod.get().get("spec").get("nodeName"))
        return list(nodes_set)

    def cleanup(self):
        """
        Cleaning up the resources created during Couchbase deployment

        """
        switch_to_project(constants.COUCHBASE_OPERATOR)
        if self.cb_create_cb_secret:
            self.cb_secrets._is_deleted = False
            self.cb_secrets.delete()
        if self.cb_create_cb_cluster:
            self.cb_example._is_deleted = False
            self.cb_example.delete()
        if self.cb_create_bucket:
            self.cb_bucket._is_deleted = False
            self.cb_bucket.delete()
        if self.cb_subscription:
            self.subscription_yaml._is_deleted = False
            self.subscription_yaml.delete()
        switch_to_project("default")
        self.ns_obj.delete_project(constants.COUCHBASE_OPERATOR)
        self.ns_obj.wait_for_delete(
            resource_name=constants.COUCHBASE_OPERATOR, timeout=90
        )
        PillowFight.cleanup(self)
        switch_to_default_rook_cluster_project()

    def couchbase_full(self):
        """
        Run full CouchBase workload
        """
        # Create Couchbase subscription
        self.couchbase_subscription()
        # Create Couchbase worker secrets
        self.create_cb_secrets()
        # Create couchbase workers
        self.create_cb_cluster(replicas=3)
        self.create_data_buckets()
        # Start measuring time
        start_time = datetime.now()
        # Run couchbase workload
        self.run_workload(
            replicas=3, num_items=50000, num_of_cycles=150000, timeout=10800
        )
        # Calculate the PillowFight pod run time from running state to completed state
        end_time = datetime.now()
        diff_time = end_time - start_time
        log.info(f"Pillowfight pod reached to completed state after {diff_time}")
