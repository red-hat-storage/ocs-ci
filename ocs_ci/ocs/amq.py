"""
AMQ Class to run amq specific tests
"""
import logging
import os
import tempfile
import time
import json
from subprocess import run, CalledProcessError

import pytest
from prettytable import PrettyTable
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import ResourceWrongStatusException, CommandFailed
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating, utils
from ocs_ci.utility.utils import run_cmd, exec_cmd, TimeoutSampler
from ocs_ci.utility.spreadsheet.spreadsheet_api import GoogleSpreadSheetAPI
from ocs_ci.helpers.helpers import storagecluster_independent_check, validate_pv_delete
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs, delete_pvcs

log = logging.getLogger(__name__)
URL = "https://get.helm.sh/helm-v2.16.1-linux-amd64.tar.gz"
AMQ_BENCHMARK_NAMESPACE = "tiller"


class AMQ(object):
    """
    Workload operation using AMQ
    """

    def __init__(self, **kwargs):
        """
        Initializer function

        Args:
            kwargs (dict):
                Following kwargs are valid
                namespace: namespace for the operator
                repo: AMQ repo where all necessary yaml file are there - a github link
                branch: branch to use from the repo
        """
        self.args = kwargs
        self.repo = self.args.get("repo", constants.KAFKA_OPERATOR)
        self.branch = self.args.get("branch", "master")
        self.ocp = OCP()
        self.ns_obj = OCP(kind="namespace")
        self.pod_obj = OCP(kind="pod")
        self.kafka_obj = OCP(kind="Kafka")
        self.kafka_connect_obj = OCP(kind="KafkaConnect")
        self.kafka_bridge_obj = OCP(kind="KafkaBridge")
        self.kafka_topic_obj = OCP(kind="KafkaTopic")
        self.kafka_user_obj = OCP(kind="KafkaUser")
        self.amq_is_setup = False
        self.messaging = False
        self.benchmark = False
        self.consumer_pod = self.producer_pod = None
        self.kafka_topic = self.kafka_user = None
        self.kafka_connect = self.kafka_bridge = self.kafka_persistent = None
        # ToDo: Remove skip once the issue is fixed
        if config.ENV_DATA.get("fips"):
            pytest.skip(
                "Skipped due to open bug in AMQ. "
                "For more info: https://issues.redhat.com/browse/ENTMQST-3422"
            )
        self.dir = tempfile.mkdtemp(prefix="amq_")
        self._clone_amq()

    def _clone_amq(self):
        """
        clone the amq repo
        """
        try:
            log.info(f"cloning amq in {self.dir}")
            git_clone_cmd = f"git clone {self.repo} "
            run(git_clone_cmd, shell=True, cwd=self.dir, check=True)
            self.amq_dir = "strimzi-kafka-operator/packaging/install/cluster-operator/"
            self.amq_kafka_pers_yaml = (
                "strimzi-kafka-operator/packaging/examples/kafka/kafka-persistent.yaml"
            )
            self.amq_kafka_connect_yaml = (
                "strimzi-kafka-operator/packaging/examples/connect/kafka-connect.yaml"
            )
            self.amq_kafka_bridge_yaml = (
                "strimzi-kafka-operator/packaging/examples/bridge/kafka-bridge.yaml"
            )
            self.kafka_topic_yaml = (
                "strimzi-kafka-operator/packaging/examples/topic/kafka-topic.yaml"
            )
            self.kafka_user_yaml = (
                "strimzi-kafka-operator/packaging/examples/user/kafka-user.yaml"
            )
            self.hello_world_producer_yaml = constants.HELLO_WORLD_PRODUCER_YAML
            self.hello_world_consumer_yaml = constants.HELLO_WORLD_CONSUMER_YAML

        except (CommandFailed, CalledProcessError) as cf:
            log.error("Error during cloning of amq repository")
            raise cf

    def create_namespace(self, namespace):
        """
        create namespace for amq

        Args:
            namespace (str): Namespace for amq pods
        """
        self.ocp.new_project(namespace)

    def setup_amq_cluster_operator(self, namespace=constants.AMQ_NAMESPACE):
        """
        Function to setup amq-cluster_operator,
        the file is pulling from github
        it will make sure cluster-operator pod is running

        Args:
            namespace (str): Namespace for AMQ pods

        """

        # Namespace for amq
        try:
            self.create_namespace(namespace)
        except CommandFailed as ef:
            if f'project.project.openshift.io "{namespace}" already exists' not in str(
                ef
            ):
                raise ef

        # Create strimzi-cluster-operator pod
        run(
            f"for i in `(ls strimzi-kafka-operator/packaging/install/cluster-operator/)`;"
            f"do sed 's/{namespace}/myproject/g' "
            f"strimzi-kafka-operator/packaging/install/cluster-operator/$i;done",
            shell=True,
            check=True,
            cwd=self.dir,
        )
        self.strimzi_kafka_operator = os.path.join(self.dir, self.amq_dir)
        pf_files = os.listdir(self.strimzi_kafka_operator)
        crds = []
        for crd in pf_files:
            crds.append(crd)
        self.crd_objects = []
        for adm_yaml in crds:
            try:
                adm_data = templating.load_yaml(self.strimzi_kafka_operator + adm_yaml)
                utils.update_container_with_mirrored_image(adm_data)
                adm_obj = OCS(**adm_data)
                adm_obj.create()
                self.crd_objects.append(adm_obj)
            except (CommandFailed, CalledProcessError) as cfe:
                if "Error is Error from server (AlreadyExists):" in str(cfe):
                    log.warn(
                        "Some amq leftovers are present, please cleanup the cluster"
                    )
                    pytest.skip(
                        "AMQ leftovers are present needs to cleanup the cluster"
                    )
        time.sleep(30)
        #  Check strimzi-cluster-operator pod created
        if self.is_amq_pod_running(pod_pattern="cluster-operator", expected_pods=1):
            log.info("strimzi-cluster-operator pod is in running state")
        else:
            raise ResourceWrongStatusException(
                "strimzi-cluster-operator pod is not getting to running state"
            )

    def is_amq_pod_running(
        self, pod_pattern, expected_pods, namespace=constants.AMQ_NAMESPACE
    ):
        """
        The function checks if provided pod_pattern finds a pod and if the status is running or not

        Args:
            pod_pattern (str): the pattern for pod
            expected_pods (int): Number of pods
            namespace (str): Namespace for amq pods

        Returns:
            bool: status of pod: True if found pod is running

        """

        _rc = True

        for pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, pod_pattern, namespace
        ):
            try:
                if pod is not None and len(pod) == expected_pods:
                    amq_pod = pod
                    break
            except IndexError as ie:
                log.error(" pod not ready yet")
                raise ie

        # checking pod status
        for pod in amq_pod:
            if self.pod_obj.wait_for_resource(
                condition="Running",
                resource_name=pod,
                timeout=1600,
                sleep=30,
            ):
                log.info(f"{pod} pod is up and running")
            else:
                _rc = False
                log.error(f"{pod} pod is not running")

        return _rc

    def setup_amq_kafka_persistent(self, sc_name, size=100, replicas=3):
        """
        Function to setup amq-kafka-persistent, the file is pulling from github
        it will make kind: Kafka and will make sure the status is running

        Args:
            sc_name (str): Name of sc
            size (int): Size of the storage in Gi
            replicas (int): Number of kafka and zookeeper pods to be created

        return : kafka_persistent

        """
        if (
            storagecluster_independent_check()
            and config.ENV_DATA["platform"].lower()
            not in constants.MANAGED_SERVICE_PLATFORMS
        ):
            sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
        try:
            kafka_persistent = templating.load_yaml(
                os.path.join(self.dir, self.amq_kafka_pers_yaml)
            )
            kafka_persistent["spec"]["kafka"]["replicas"] = replicas
            kafka_persistent["spec"]["kafka"]["storage"]["volumes"][0][
                "class"
            ] = sc_name
            kafka_persistent["spec"]["kafka"]["storage"]["volumes"][0][
                "size"
            ] = f"{size}Gi"

            kafka_persistent["spec"]["zookeeper"]["replicas"] = replicas
            kafka_persistent["spec"]["zookeeper"]["storage"]["class"] = sc_name
            kafka_persistent["spec"]["zookeeper"]["storage"]["size"] = f"{size}Gi"
            self.kafka_persistent = OCS(**kafka_persistent)
            self.kafka_persistent.create()

        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during setup of AMQ Kafka-persistent")
            raise cf
        time.sleep(40)

        if self.is_amq_pod_running(
            pod_pattern="my-cluster", expected_pods=(replicas * 2) + 1
        ):
            return self.kafka_persistent
        else:
            raise ResourceWrongStatusException(
                "my-cluster-kafka and my-cluster-zookeeper "
                "Pod is not getting to running state"
            )

    def setup_amq_kafka_connect(self):
        """
        The function is to setup amq-kafka-connect, the yaml file is pulling from github
        it will make kind: KafkaConnect and will make sure the status is running

        Returns: kafka_connect object
        """
        try:
            kafka_connect = templating.load_yaml(
                os.path.join(self.dir, self.amq_kafka_connect_yaml)
            )
            self.kafka_connect = OCS(**kafka_connect)
            self.kafka_connect.create()
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during setup of AMQ KafkaConnect")
            raise cf

        if self.is_amq_pod_running(
            pod_pattern="my-connect-cluster-connect", expected_pods=1
        ):
            return self.kafka_connect
        else:
            raise ResourceWrongStatusException(
                "my-connect-cluster-connect pod is not getting to running state"
            )

    def setup_amq_kafka_bridge(self):
        """
        Function to setup amq-kafka, the file file is pulling from github
        it will make kind: KafkaBridge and will make sure the pod status is running

        Return: kafka_bridge object
        """
        try:
            kafka_bridge = templating.load_yaml(
                os.path.join(self.dir, self.amq_kafka_bridge_yaml)
            )
            self.kafka_bridge = OCS(**kafka_bridge)
            self.kafka_bridge.create()
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during setup of AMQ KafkaConnect")
            raise cf
        # Making sure the kafka_bridge is running
        if self.is_amq_pod_running(pod_pattern="my-bridge-bridge", expected_pods=1):
            return self.kafka_bridge
        else:
            raise ResourceWrongStatusException(
                "kafka_bridge_pod pod is not getting to running state"
            )

    def create_kafka_topic(self, name="my-topic", partitions=1, replicas=1):
        """
        Creates kafka topic

        Args:
            name (str): Name of the kafka topic
            partitions (int): Number of partitions
            replicas (int): Number of replicas

        Return: kafka_topic object
        """
        try:
            kafka_topic = templating.load_yaml(
                os.path.join(self.dir, self.kafka_topic_yaml)
            )
            kafka_topic["metadata"]["name"] = name
            kafka_topic["spec"]["partitions"] = partitions
            kafka_topic["spec"]["replicas"] = replicas
            self.kafka_topic = OCS(**kafka_topic)
            self.kafka_topic.create()
        except (CommandFailed, CalledProcessError) as cf:
            if f'kafkatopics.kafka.strimzi.io "{name}" already exists' not in str(cf):
                log.error("Failed during creating of Kafka topic")
                raise cf

        # Making sure kafka topic created
        if self.kafka_topic_obj.get(resource_name=name):
            return self.kafka_topic
        else:
            raise ResourceWrongStatusException("kafka topic is not created")

    def create_kafka_user(self, name="my-user"):
        """
        Creates kafka user

        Args:
             name (str): Name of the kafka user

        Return: kafka_user object

        """
        try:
            kafka_user = templating.load_yaml(
                os.path.join(self.dir, self.kafka_user_yaml)
            )
            kafka_user["metadata"]["name"] = name
            self.kafka_user = OCS(**kafka_user)
            self.kafka_user.create()
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during creating of Kafka user")
            raise cf

        # Making sure kafka user created
        if self.kafka_user_obj.get(resource_name=name):
            return self.kafka_user
        else:
            raise ResourceWrongStatusException("kafka user is not created")

    def create_producer_pod(self, num_of_pods=1, value="10000"):
        """
        Creates producer pods

        Args:
            num_of_pods (int): Number of producer pods to be created
            value (str): Number of the messages to be sent

        Returns: producer pod object

        """
        try:
            producer_pod = templating.load_yaml(constants.HELLO_WORLD_PRODUCER_YAML)
            producer_pod["spec"]["replicas"] = num_of_pods
            producer_pod["spec"]["template"]["spec"]["containers"][0]["env"][4][
                "value"
            ] = value
            self.producer_pod = OCS(**producer_pod)
            self.producer_pod.create()
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during creation of producer pod")
            raise cf

        # Making sure the producer pod is running
        if self.is_amq_pod_running(
            pod_pattern="hello-world-producer", expected_pods=num_of_pods
        ):
            return self.producer_pod
        else:
            raise ResourceWrongStatusException(
                "producer pod is not getting to running state"
            )

    def create_consumer_pod(self, num_of_pods=1, value="10000"):
        """
        Creates producer pods

        Args:
            num_of_pods (int): Number of consumer pods to be created
            value (str): Number of messages to be received

        Returns: consumer pod object

        """
        try:
            consumer_pod = templating.load_yaml(constants.HELLO_WORLD_CONSUMER_YAML)
            consumer_pod["spec"]["replicas"] = num_of_pods
            consumer_pod["spec"]["template"]["spec"]["containers"][0]["env"][4][
                "value"
            ] = value
            self.consumer_pod = OCS(**consumer_pod)
            self.consumer_pod.create()
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during creation of consumer pod")
            raise cf

        # Making sure the producer pod is running
        if self.is_amq_pod_running(
            pod_pattern="hello-world-consumer", expected_pods=num_of_pods
        ):
            return self.consumer_pod
        else:
            raise ResourceWrongStatusException(
                "consumer pod is not getting to running state"
            )

    def validate_msg(
        self, pod, namespace=constants.AMQ_NAMESPACE, value="10000", since_time=1800
    ):
        """
        Validate if messages are sent or received

        Args:
            pod (str): Name of the pod
            namespace (str): Namespace of the pod
            value (str): Number of messages are sent
            since_time (int): Number of seconds to required to sent the msg

        Returns:
            bool : True if all messages are sent/received

        """
        cmd = f"oc logs -n {namespace} {pod} --since={since_time}s"
        msg = run_cmd(cmd)
        substring = f"Hello world - {int(value) - 1}"
        if msg.find(substring) == -1:
            return False
        else:
            return True

    def validate_messages_are_produced(
        self, namespace=constants.AMQ_NAMESPACE, value="10000", since_time=1800
    ):
        """
        Validates if all messages are sent in producer pod

        Args:
            namespace (str): Namespace of the pod
            value (str): Number of messages are sent
            since_time (int): Number of seconds to required to sent the msg

        Raises exception on failures

        """
        # ToDo: Support multiple topics and users
        producer_pod_objs = [
            get_pod_obj(pod)
            for pod in get_pod_name_by_pattern("hello-world-produce", namespace)
        ]
        for pod in producer_pod_objs:
            for msg in TimeoutSampler(
                900, 30, self.validate_msg, pod.name, namespace, value, since_time
            ):
                if msg:
                    break
        assert msg, "Few messages are not sent by producer"
        log.info("Producer sent all messages")

    def validate_messages_are_consumed(
        self, namespace=constants.AMQ_NAMESPACE, value="10000", since_time=1800
    ):
        """
        Validates if all messages are received in consumer pod

        Args:
            namespace (str): Namespace of the pod
            value (str): Number of messages are recieved
            since_time (int): Number of seconds to required to receive the msg

        Raises exception on failures

        """
        # ToDo: Support multiple topics and users
        consumer_pod_objs = [
            get_pod_obj(pod)
            for pod in get_pod_name_by_pattern("hello-world-consumer", namespace)
        ]
        for pod in consumer_pod_objs:
            for msg in TimeoutSampler(
                900, 30, self.validate_msg, pod.name, namespace, value, since_time
            ):
                if msg:
                    break
        assert msg, "Consumer didn't receive all messages"
        log.info("Consumer received all messages")

    def run_in_bg(
        self, namespace=constants.AMQ_NAMESPACE, value="10000", since_time=1800
    ):
        """
        Validate messages are produced and consumed in bg

        Args:
            namespace (str): Namespace of the pod
            value (str): Number of messages to be sent and received
            since_time (int): Number of seconds to required to sent and receive msg

        """
        # Todo: Check for each messages sent and received
        log.info("Running open messages on pod in bg")
        threads = []

        executor = ThreadPoolExecutor(2)
        threads.append(
            executor.submit(
                self.validate_messages_are_produced, namespace, value, since_time
            )
        )
        threads.append(
            executor.submit(
                self.validate_messages_are_consumed, namespace, value, since_time
            )
        )

        return threads

    def run_amq_benchmark(
        self,
        benchmark_pod_name="benchmark",
        kafka_namespace=constants.AMQ_NAMESPACE,
        tiller_namespace=AMQ_BENCHMARK_NAMESPACE,
        num_of_clients=8,
        worker=None,
        timeout=1800,
        amq_workload_yaml=None,
        run_in_bg=False,
    ):
        """
        Run benchmark pod and get the results

        Args:
            benchmark_pod_name (str): Name of the benchmark pod
            kafka_namespace (str): Namespace where kafka cluster created
            tiller_namespace (str): Namespace where tiller pod needs to be created
            num_of_clients (int): Number of clients to be created
            worker (str) : Loads to create on workloads separated with commas
                e.g http://benchmark-worker-0.benchmark-worker:8080,
                http://benchmark-worker-1.benchmark-worker:8080
            timeout (int): Time to complete the run
            amq_workload_yaml (dict): Contains amq workloads information keys and values
                :name (str): Name of the workloads
                :topics (int): Number of topics created
                :partitions_per_topic (int): Number of partitions per topic
                :message_size (int): Message size
                :payload_file (str): Load to run on workload
                :subscriptions_per_topic (int): Number of subscriptions per topic
                :consumer_per_subscription (int): Number of consumers per subscription
                :producers_per_topic (int): Number of producers per topic
                :producer_rate (int): Producer rate
                :consumer_backlog_sizegb (int): Size of block in gb
                :test_duration_minutes (int): Time to run the workloads
            run_in_bg (bool): On true the workload will run in background

        Return:
            result (str/Thread obj): Returns benchmark run information if run_in_bg is False.
                Otherwise a thread of the amq workload execution

        """

        # Namespace for to helm/tiller
        try:
            self.create_namespace(tiller_namespace)
        except CommandFailed as ef:
            if (
                f'project.project.openshift.io "{tiller_namespace}" already exists'
                not in str(ef)
            ):
                raise ef

        # Create rbac file
        try:
            sa_tiller = list(
                templating.load_yaml(constants.AMQ_RBAC_YAML, multi_document=True)
            )
            sa_tiller[0]["metadata"]["namespace"] = tiller_namespace
            sa_tiller[1]["subjects"][0]["namespace"] = tiller_namespace
            self.sa_tiller = OCS(**sa_tiller[0])
            self.crb_tiller = OCS(**sa_tiller[1])
            self.sa_tiller.create()
            self.crb_tiller.create()
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during creation of service account tiller")
            raise cf

        # Install helm cli (version v2.16.0 as we need tiller component)
        # And create tiller pods
        wget_cmd = f"wget -c --read-timeout=5 --tries=0 {URL}"
        untar_cmd = "tar -zxvf helm-v2.16.1-linux-amd64.tar.gz"
        tiller_cmd = (
            f"linux-amd64/helm init --tiller-namespace {tiller_namespace}"
            f" --service-account {tiller_namespace}"
        )
        exec_cmd(cmd=wget_cmd, cwd=self.dir)
        exec_cmd(cmd=untar_cmd, cwd=self.dir)
        exec_cmd(cmd=tiller_cmd, cwd=self.dir)

        # Validate tiller pod is running
        log.info("Waiting for 30s for tiller pod to come up")
        time.sleep(30)
        if self.is_amq_pod_running(
            pod_pattern="tiller", expected_pods=1, namespace=tiller_namespace
        ):
            log.info("Tiller pod is running")
        else:
            raise ResourceWrongStatusException("Tiller pod is not in running state")

        # Create benchmark pods
        log.info("Create benchmark pods")
        values = templating.load_yaml(constants.AMQ_BENCHMARK_VALUE_YAML)
        values["numWorkers"] = num_of_clients
        benchmark_cmd = (
            f"linux-amd64/helm install {constants.AMQ_BENCHMARK_POD_YAML}"
            f" --name {benchmark_pod_name} --tiller-namespace {tiller_namespace}"
        )
        exec_cmd(cmd=benchmark_cmd, cwd=self.dir)

        # Making sure the benchmark pod and clients are running
        if self.is_amq_pod_running(
            pod_pattern="benchmark",
            expected_pods=(1 + num_of_clients),
            namespace=tiller_namespace,
        ):
            log.info("All benchmark pod is up and running")
        else:
            raise ResourceWrongStatusException(
                "Benchmark pod is not getting to running state"
            )

        # Update commonConfig with kafka-bootstrap server details
        driver_kafka = templating.load_yaml(constants.AMQ_DRIVER_KAFKA_YAML)
        driver_kafka[
            "commonConfig"
        ] = f"bootstrap.servers=my-cluster-kafka-bootstrap.{kafka_namespace}.svc.cluster.local:9092"
        json_file = f"{self.dir}/driver_kafka"
        templating.dump_data_to_json(driver_kafka, json_file)
        cmd = f"cp {json_file} {benchmark_pod_name}-driver:/"
        self.pod_obj.exec_oc_cmd(cmd)

        # Update the workload yaml
        if not amq_workload_yaml:
            amq_workload_yaml = templating.load_yaml(constants.AMQ_WORKLOAD_YAML)
        yaml_file = f"{self.dir}/amq_workload.yaml"
        templating.dump_data_to_temp_yaml(amq_workload_yaml, yaml_file)
        cmd = f"cp {yaml_file} {benchmark_pod_name}-driver:/"
        self.pod_obj.exec_oc_cmd(cmd)

        self.benchmark = True

        # Run the benchmark
        if worker:
            cmd = f"bin/benchmark --drivers /driver_kafka --workers {worker} /amq_workload.yaml"
        else:
            cmd = "bin/benchmark --drivers /driver_kafka /amq_workload.yaml"
        log.info(f"Run benchmark and running command {cmd} inside the benchmark pod ")

        if run_in_bg:
            executor = ThreadPoolExecutor(1)
            result = executor.submit(
                self.run_amq_workload,
                cmd,
                benchmark_pod_name,
                tiller_namespace,
                timeout,
            )
            return result

        pod_obj = get_pod_obj(
            name=f"{benchmark_pod_name}-driver", namespace=tiller_namespace
        )
        result = pod_obj.exec_cmd_on_pod(
            command=cmd, out_yaml_format=False, timeout=timeout
        )

        return result

    def run_amq_workload(self, command, benchmark_pod_name, tiller_namespace, timeout):
        """
        Runs amq workload in bg

        Args:
             command (str): Command to run on pod
             benchmark_pod_name (str): Pod name
             tiller_namespace (str): Namespace of pod
             timeout (int): Time to complete the run

        Returns:
            result (str): Returns benchmark run information

        """
        pod_obj = get_pod_obj(
            name=f"{benchmark_pod_name}-driver", namespace=tiller_namespace
        )
        return pod_obj.exec_cmd_on_pod(
            command=command, out_yaml_format=False, timeout=timeout
        )

    def validate_amq_benchmark(
        self, result, amq_workload_yaml, benchmark_pod_name="benchmark"
    ):
        """
        Validates amq benchmark run

        Args:
            result (str): Benchmark run information
            amq_workload_yaml (dict): AMQ workload information
            benchmark_pod_name (str): Name of the benchmark pod

        Returns:
            res_dict (dict): Returns the dict output on success, Otherwise none

        """
        res_dict = {}
        res_dict["topic"] = amq_workload_yaml["topics"]
        res_dict["partitionsPerTopic"] = amq_workload_yaml["partitionsPerTopic"]
        res_dict["messageSize"] = amq_workload_yaml["messageSize"]
        res_dict["payloadFile"] = amq_workload_yaml["payloadFile"]
        res_dict["subscriptionsPerTopic"] = amq_workload_yaml["subscriptionsPerTopic"]
        res_dict["producersPerTopic"] = amq_workload_yaml["producersPerTopic"]
        res_dict["consumerPerSubscription"] = amq_workload_yaml[
            "consumerPerSubscription"
        ]
        res_dict["producerRate"] = amq_workload_yaml["producerRate"]

        # Validate amq benchmark is completed
        for part in result.split():
            if ".json" in part:
                workload_json_file = part

        if workload_json_file:
            cmd = f"rsync {benchmark_pod_name}-driver:{workload_json_file} {self.dir} -n {AMQ_BENCHMARK_NAMESPACE}"
            self.pod_obj.exec_oc_cmd(command=cmd, out_yaml_format=False)
            # Parse the json file
            with open(f"{self.dir}/{workload_json_file}") as json_file:
                data = json.load(json_file)
            res_dict["AvgpublishRate"] = sum(data.get("publishRate")) / len(
                data.get("publishRate")
            )
            res_dict["AvgConsumerRate"] = sum(data.get("consumeRate")) / len(
                data.get("consumeRate")
            )
            res_dict["AvgMsgBacklog"] = sum(data.get("backlog")) / len(
                data.get("backlog")
            )
            res_dict["publishLatencyAvg"] = sum(data.get("publishLatencyAvg")) / len(
                data.get("publishLatencyAvg")
            )
            res_dict["aggregatedPublishLatencyAvg"] = data.get(
                "aggregatedPublishLatencyAvg"
            )
            res_dict["aggregatedPublishLatency50pct"] = data.get(
                "aggregatedPublishLatency50pct"
            )
            res_dict["aggregatedPublishLatency75pct"] = data.get(
                "aggregatedPublishLatency75pct"
            )
            res_dict["aggregatedPublishLatency95pct"] = data.get(
                "aggregatedPublishLatency95pct"
            )
            res_dict["aggregatedPublishLatency99pct"] = data.get(
                "aggregatedPublishLatency99pct"
            )
            res_dict["aggregatedPublishLatency999pct"] = data.get(
                "aggregatedPublishLatency999pct"
            )
            res_dict["aggregatedPublishLatency9999pct"] = data.get(
                "aggregatedPublishLatency9999pct"
            )
            res_dict["aggregatedPublishLatencyMax"] = data.get(
                "aggregatedPublishLatencyMax"
            )
            res_dict["aggregatedEndToEndLatencyAvg"] = data.get(
                "aggregatedEndToEndLatencyAvg"
            )
            res_dict["aggregatedEndToEndLatency50pct"] = data.get(
                "aggregatedEndToEndLatency50pct"
            )
            res_dict["aggregatedEndToEndLatency75pct"] = data.get(
                "aggregatedEndToEndLatency75pct"
            )
            res_dict["aggregatedEndToEndLatency95pct"] = data.get(
                "aggregatedEndToEndLatency95pct"
            )
            res_dict["aggregatedEndToEndLatency99pct"] = data.get(
                "aggregatedEndToEndLatency99pct"
            )
            res_dict["aggregatedEndToEndLatency999pct"] = data.get(
                "aggregatedEndToEndLatency999pct"
            )
            res_dict["aggregatedEndToEndLatency9999pct"] = data.get(
                "aggregatedEndToEndLatency9999pct"
            )
            res_dict["aggregatedEndToEndLatencyMax"] = data.get(
                "aggregatedEndToEndLatencyMax"
            )
        else:
            log.error("Benchmark didn't run completely")
            return None

        amq_benchmark_pod_table = PrettyTable(["key", "value"])
        for key, val in res_dict.items():
            amq_benchmark_pod_table.add_row([key, val])
        log.info(f"\n{amq_benchmark_pod_table}\n")

        return res_dict

    def export_amq_output_to_gsheet(self, amq_output, sheet_name, sheet_index):
        """
        Collect amq data to google spreadsheet

        Args:
            amq_output (dict):  amq output in dict
            sheet_name (str): Name of the sheet
            sheet_index (int): Index of sheet

        """
        # Collect data and export to Google doc spreadsheet
        g_sheet = GoogleSpreadSheetAPI(sheet_name=sheet_name, sheet_index=sheet_index)
        log.info("Exporting amq data to google spreadsheet")

        headers_to_key = []
        values = []
        for key, val in amq_output.items():
            headers_to_key.append(key)
            values.append(val)

        # Update amq_result to gsheet
        g_sheet.insert_row(values, 2)
        g_sheet.insert_row(headers_to_key, 2)

        # Capturing versions(OCP, OCS and Ceph) and test run name
        g_sheet.insert_row(
            [
                f"ocp_version:{utils.get_cluster_version()}",
                f"ocs_build_number:{utils.get_ocs_build_number()}",
                f"ceph_version:{utils.get_ceph_version()}",
                f"test_run_name:{utils.get_testrun_name()}",
            ],
            2,
        )

    def create_messaging_on_amq(
        self,
        topic_name="my-topic",
        user_name="my-user",
        partitions=1,
        replicas=1,
        num_of_producer_pods=1,
        num_of_consumer_pods=1,
        value="10000",
    ):
        """
        Creates workload using Open Messaging tool on amq cluster

        Args:
            topic_name (str): Name of the topic to be created
            user_name (str): Name of the user to be created
            partitions (int): Number of partitions of topic
            replicas (int): Number of replicas of topic
            num_of_producer_pods (int): Number of producer pods to be created
            num_of_consumer_pods (int): Number of consumer pods to be created
            value (str): Number of messages to be sent and received

        """
        self.create_kafka_topic(topic_name, partitions, replicas)
        self.create_kafka_user(user_name)
        self.create_producer_pod(num_of_producer_pods, value)
        self.create_consumer_pod(num_of_consumer_pods, value)
        self.messaging = True

    def setup_amq_cluster(
        self, sc_name, namespace=constants.AMQ_NAMESPACE, size=100, replicas=3
    ):
        """
        Creates amq cluster with persistent storage.

        Args:
            sc_name (str): Name of sc
            namespace (str): Namespace for amq cluster
            size (int): Size of the storage
            replicas (int): Number of kafka and zookeeper pods to be created

        """
        if storagecluster_independent_check():
            sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
        self.setup_amq_cluster_operator(namespace)
        self.setup_amq_kafka_persistent(sc_name, size, replicas)
        self.setup_amq_kafka_connect()
        self.setup_amq_kafka_bridge()
        self.amq_is_setup = True
        return self

    def create_kafkadrop(self, wait=True):
        """
        Create kafkadrop pod, service and routes

        Args:
            wait (bool): If true waits till kafkadrop pod running

        Return:
            tuple: Contains objects of kafkadrop pod, service and route

        """
        # Create kafkadrop pod
        try:
            kafkadrop = list(
                templating.load_yaml(constants.KAFKADROP_YAML, multi_document=True)
            )
            self.kafkadrop_pod = OCS(**kafkadrop[0])
            self.kafkadrop_svc = OCS(**kafkadrop[1])
            self.kafkadrop_route = OCS(**kafkadrop[2])
            self.kafkadrop_pod.create()
            self.kafkadrop_svc.create()
            self.kafkadrop_route.create()
        except (CommandFailed, CalledProcessError) as cf:
            log.error("Failed during creation of kafkadrop which kafka UI")
            raise cf

        # Validate kafkadrop pod running
        if wait:
            ocp_obj = OCP(kind=constants.POD, namespace=constants.AMQ_NAMESPACE)
            ocp_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector="app=kafdrop",
                timeout=120,
                sleep=5,
            )

        return self.kafkadrop_pod, self.kafkadrop_svc, self.kafkadrop_route

    def cleanup(
        self,
        kafka_namespace=constants.AMQ_NAMESPACE,
        tiller_namespace=AMQ_BENCHMARK_NAMESPACE,
    ):
        """
        Clean up function,
        will start to delete from amq cluster operator
        then amq-connector, persistent, bridge, at the end it will delete the created namespace

        Args:
            kafka_namespace (str): Created namespace for amq
            tiller_namespace (str): Created namespace for benchmark

        """

        if self.consumer_pod:
            self.consumer_pod.delete()
        if self.producer_pod:
            self.producer_pod.delete()
        if self.kafka_user:
            self.kafka_user.delete()
        if self.kafka_topic:
            self.kafka_topic.delete()

        if self.benchmark:
            # Delete the helm app
            try:
                purge_cmd = f"linux-amd64/helm delete benchmark --purge --tiller-namespace {tiller_namespace}"
                run(purge_cmd, shell=True, cwd=self.dir, check=True)
            except (CommandFailed, CalledProcessError) as cf:
                log.error("Failed to delete help app")
                raise cf
            # Delete the pods and namespace created
            self.sa_tiller.delete()
            self.crb_tiller.delete()
            run_cmd(f"oc delete project {tiller_namespace}")
            self.ns_obj.wait_for_delete(resource_name=tiller_namespace)

        if self.kafka_connect:
            self.kafka_connect.delete()
        if self.kafka_bridge:
            self.kafka_bridge.delete()
        if self.kafka_persistent:
            self.kafka_persistent.delete()
            log.info("Waiting for 20 seconds to delete persistent")
            time.sleep(20)
            ocs_pvc_obj = get_all_pvc_objs(namespace=kafka_namespace)
            if ocs_pvc_obj:
                delete_pvcs(ocs_pvc_obj)
            for pvc in ocs_pvc_obj:
                log.info(pvc.name)
                validate_pv_delete(pvc.backed_pv)

        if self.crd_objects:
            for adm_obj in self.crd_objects:
                adm_obj.delete()
        time.sleep(20)

        # Reset namespace to default
        switch_to_default_rook_cluster_project()
        run_cmd(f"oc delete project {kafka_namespace}")
        self.ns_obj.wait_for_delete(resource_name=kafka_namespace, timeout=90)
