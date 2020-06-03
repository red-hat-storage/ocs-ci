"""
AMQ Class to run amq specific tests
"""
import logging
import os
import tempfile
import time
from subprocess import run, CalledProcessError
from threading import Thread

from ocs_ci.ocs.exceptions import (ResourceWrongStatusException, CommandFailed)
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, TimeoutSampler

log = logging.getLogger(__name__)


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
        self.repo = self.args.get('repo', constants.KAFKA_OPERATOR)
        self.branch = self.args.get('branch', 'master')
        self.ocp = OCP()
        self.ns_obj = OCP(kind='namespace')
        self.pod_obj = OCP(kind='pod')
        self.kafka_obj = OCP(kind='Kafka')
        self.kafka_connect_obj = OCP(kind="KafkaConnect")
        self.kafka_bridge_obj = OCP(kind="KafkaBridge")
        self.kafka_topic_obj = OCP(kind="KafkaTopic")
        self.kafka_user_obj = OCP(kind="KafkaUser")
        self.amq_is_setup = False
        self.messaging = False
        self._clone_amq()

    def _clone_amq(self):
        """
        clone the amq repo
        """
        self.dir = tempfile.mkdtemp(prefix='amq_')
        try:
            log.info(f'cloning amq in {self.dir}')
            git_clone_cmd = f'git clone -b {self.branch} {self.repo} '
            run(
                git_clone_cmd,
                shell=True,
                cwd=self.dir,
                check=True
            )
            self.amq_dir = "strimzi-kafka-operator/install/cluster-operator/"
            self.amq_kafka_pers_yaml = "strimzi-kafka-operator/examples/kafka/kafka-persistent.yaml"
            self.amq_kafka_connect_yaml = "strimzi-kafka-operator/examples/kafka-connect/kafka-connect.yaml"
            self.amq_kafka_bridge_yaml = "strimzi-kafka-operator/examples/kafka-bridge/kafka-bridge.yaml"
            self.kafka_topic_yaml = "strimzi-kafka-operator/examples/topic/kafka-topic.yaml"
            self.kafka_user_yaml = "strimzi-kafka-operator/examples/user/kafka-user.yaml"
            self.hello_world_producer_yaml = constants.HELLO_WORLD_PRODUCER_YAML
            self.hello_world_consumer_yaml = constants.HELLO_WORLD_CONSUMER_YAML

        except (CommandFailed, CalledProcessError)as cf:
            log.error('Error during cloning of amq repository')
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
            if f'project.project.openshift.io "{namespace}" already exists' not in str(ef):
                raise ef

        # Create strimzi-cluster-operator pod
        run(f"for i in `(ls strimzi-kafka-operator/install/cluster-operator/)`;"
            f"do sed 's/{namespace}/myproject/g' strimzi-kafka-operator/install/cluster-operator/$i;done",
            shell=True, check=True, cwd=self.dir)
        run(f'oc apply -f {self.amq_dir} -n {namespace}', shell=True, check=True, cwd=self.dir)
        time.sleep(10)

        #  Check strimzi-cluster-operator pod created
        if self.is_amq_pod_running(pod_pattern="cluster-operator", expected_pods=1):
            log.info("strimzi-cluster-operator pod is in running state")
        else:
            raise ResourceWrongStatusException("strimzi-cluster-operator pod is not getting to running state")

    def is_amq_pod_running(self, pod_pattern, expected_pods, namespace=constants.AMQ_NAMESPACE):
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
            if (self.pod_obj.wait_for_resource(
                condition='Running',
                resource_name=pod,
                timeout=1600,
                sleep=30,
            )
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
        try:
            kafka_persistent = templating.load_yaml(os.path.join(self.dir, self.amq_kafka_pers_yaml))
            kafka_persistent['spec']['kafka']['replicas'] = replicas
            kafka_persistent['spec']['kafka']['storage']['volumes'][0]['class'] = sc_name
            kafka_persistent['spec']['kafka']['storage']['volumes'][0]['size'] = f"{size}Gi"

            kafka_persistent['spec']['zookeeper']['replicas'] = replicas
            kafka_persistent['spec']['zookeeper']['storage']['class'] = sc_name
            kafka_persistent['spec']['zookeeper']['storage']['size'] = f"{size}Gi"
            self.kafka_persistent = OCS(**kafka_persistent)
            self.kafka_persistent.create()

        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed during setup of AMQ Kafka-persistent')
            raise cf
        time.sleep(40)

        if self.is_amq_pod_running(
            pod_pattern="my-cluster-zookeeper", expected_pods=replicas
        ) and self.is_amq_pod_running(pod_pattern="my-cluster-kafka", expected_pods=replicas):
            return self.kafka_persistent
        else:
            raise ResourceWrongStatusException("my-cluster-kafka and my-cluster-zookeeper "
                                               "Pod is not getting to running state")

    def setup_amq_kafka_connect(self):
        """
        The function is to setup amq-kafka-connect, the yaml file is pulling from github
        it will make kind: KafkaConnect and will make sure the status is running

        Returns: kafka_connect object
        """
        try:
            kafka_connect = templating.load_yaml(os.path.join(self.dir, self.amq_kafka_connect_yaml))
            self.kafka_connect = OCS(**kafka_connect)
            self.kafka_connect.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed during setup of AMQ KafkaConnect')
            raise cf

        if self.is_amq_pod_running(pod_pattern="my-connect-cluster-connect", expected_pods=1):
            return self.kafka_connect
        else:
            raise ResourceWrongStatusException("my-connect-cluster-connect pod is not getting to running state")

    def setup_amq_kafka_bridge(self):
        """
        Function to setup amq-kafka, the file file is pulling from github
        it will make kind: KafkaBridge and will make sure the pod status is running

        Return: kafka_bridge object
        """
        try:
            kafka_bridge = templating.load_yaml(os.path.join(self.dir, self.amq_kafka_bridge_yaml))
            self.kafka_bridge = OCS(**kafka_bridge)
            self.kafka_bridge.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed during setup of AMQ KafkaConnect')
            raise cf
        # Making sure the kafka_bridge is running
        if self.is_amq_pod_running(pod_pattern="my-bridge-bridge", expected_pods=1):
            return self.kafka_bridge
        else:
            raise ResourceWrongStatusException("kafka_bridge_pod pod is not getting to running state")

    def create_kafka_topic(self, name='my-topic', partitions=1, replicas=1):
        """
        Creates kafka topic

        Args:
            name (str): Name of the kafka topic
            partitions (int): Number of partitions
            replicas (int): Number of replicas

        Return: kafka_topic object
        """
        try:
            kafka_topic = templating.load_yaml(os.path.join(self.dir, self.kafka_topic_yaml))
            kafka_topic["metadata"]["name"] = name
            kafka_topic["spec"]["partitions"] = partitions
            kafka_topic["spec"]["replicas"] = replicas
            self.kafka_topic = OCS(**kafka_topic)
            self.kafka_topic.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed during creating of Kafka topic')
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
            kafka_user = templating.load_yaml(os.path.join(self.dir, self.kafka_user_yaml))
            kafka_user["metadata"]["name"] = name
            self.kafka_user = OCS(**kafka_user)
            self.kafka_user.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed during creating of Kafka user')
            raise cf

        # Making sure kafka user created
        if self.kafka_user_obj.get(resource_name=name):
            return self.kafka_user
        else:
            raise ResourceWrongStatusException("kafka user is not created")

    def create_producer_pod(self, num_of_pods=1, value='10000'):
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
            producer_pod["spec"]["template"]["spec"]["containers"][0]["env"][4]["value"] = value
            self.producer_pod = OCS(**producer_pod)
            self.producer_pod.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed during creation of producer pod')
            raise cf

        # Making sure the producer pod is running
        if self.is_amq_pod_running(pod_pattern="hello-world-producer", expected_pods=num_of_pods):
            return self.producer_pod
        else:
            raise ResourceWrongStatusException("producer pod is not getting to running state")

    def create_consumer_pod(self, num_of_pods=1, value='10000'):
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
            consumer_pod["spec"]["template"]["spec"]["containers"][0]["env"][4]["value"] = value
            self.consumer_pod = OCS(**consumer_pod)
            self.consumer_pod.create()
        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed during creation of consumer pod')
            raise cf

        # Making sure the producer pod is running
        if self.is_amq_pod_running(pod_pattern="hello-world-consumer", expected_pods=num_of_pods):
            return self.consumer_pod
        else:
            raise ResourceWrongStatusException("consumer pod is not getting to running state")

    def validate_msg(self, pod, namespace=constants.AMQ_NAMESPACE, value='10000', since_time=1800):
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
        if msg.find(f"Hello world - {int(value) - 1} ") is -1:
            return False
        else:
            return True

    def validate_messages_are_produced(self, namespace=constants.AMQ_NAMESPACE, value='10000', since_time=1800):
        """
        Validates if all messages are sent in producer pod

        Args:
            namespace (str): Namespace of the pod
            value (str): Number of messages are sent
            since_time (int): Number of seconds to required to sent the msg

        Raises exception on failures

        """
        # ToDo: Support multiple topics and users
        producer_pod_objs = [get_pod_obj(
            pod
        )for pod in get_pod_name_by_pattern('hello-world-produce', namespace)
        ]
        for pod in producer_pod_objs:
            for msg in TimeoutSampler(
                900, 30, self.validate_msg, pod.name, namespace, value, since_time
            ):
                if msg:
                    break
        log.error("Few messages are not sent")
        raise Exception("All messages are not sent from the producer pod")

    def validate_messages_are_consumed(self, namespace=constants.AMQ_NAMESPACE, value='10000', since_time=1800):
        """
        Validates if all messages are received in consumer pod

        Args:
            namespace (str): Namespace of the pod
            value (str): Number of messages are recieved
            since_time (int): Number of seconds to required to receive the msg

        Raises exception on failures

        """
        # ToDo: Support multiple topics and users
        consumer_pod_objs = [get_pod_obj(
            pod
        )for pod in get_pod_name_by_pattern('hello-world-consumer', namespace)
        ]
        for pod in consumer_pod_objs:
            for msg in TimeoutSampler(
                900, 30, self.validate_msg, pod.name, namespace, value, since_time
            ):
                if msg:
                    log.info("Consumer pod received all messages sent by producer")
                    break
        log.error("Few messages are not received")
        raise Exception("Consumer pod received all messages sent by producer")

    def run_in_bg(self, namespace=constants.AMQ_NAMESPACE, value='10000', since_time=1800):
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

        thread1 = Thread(target=self.validate_messages_are_produced, args=(namespace, value, since_time))
        thread1.start()
        time.sleep(10)
        threads.append(thread1)

        thread2 = Thread(target=self.validate_messages_are_consumed, args=(namespace, value, since_time))
        thread2.start()
        time.sleep(10)
        threads.append(thread2)

        return threads

    # ToDo: Install helm and get kafka metrics

    def create_messaging_on_amq(self, topic_name='my-topic', user_name="my-user", partitions=1,
                                replicas=1, num_of_producer_pods=1, num_of_consumer_pods=1,
                                value='10000'
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

    def setup_amq_cluster(self, sc_name, namespace=constants.AMQ_NAMESPACE, size=100, replicas=3):
        """
        Creates amq cluster with persistent storage.

        Args:
            sc_name (str): Name of sc
            namespace (str): Namespace for amq cluster
            size (int): Size of the storage
            replicas (int): Number of kafka and zookeeper pods to be created

        """
        self.setup_amq_cluster_operator(namespace)
        self.setup_amq_kafka_persistent(sc_name, size, replicas)
        self.setup_amq_kafka_connect()
        self.setup_amq_kafka_bridge()
        self.amq_is_setup = True
        return self

    def cleanup(self, namespace=constants.AMQ_NAMESPACE):
        """
        Clean up function,
        will start to delete from amq cluster operator
        then amq-connector, persistent, bridge, at the end it will delete the created namespace

        Args:
            namespace (str): Created namespace for amq
        """
        if self.amq_is_setup:
            if self.messaging:
                self.consumer_pod.delete()
                self.producer_pod.delete()
                self.kafka_user.delete()
                self.kafka_topic.delete()
            self.kafka_persistent.delete()
            self.kafka_connect.delete()
            self.kafka_bridge.delete()
            run_cmd(f'oc delete -f {self.amq_dir}', shell=True, check=True, cwd=self.dir)
        run_cmd(f'oc delete project {namespace}')

        # Reset namespace to default
        switch_to_default_rook_cluster_project()
        self.ns_obj.wait_for_delete(resource_name=namespace)
