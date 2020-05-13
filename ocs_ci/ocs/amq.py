"""
AMQ Class to run amq specific tests
"""
import logging
import os
import tempfile
from subprocess import run, CalledProcessError

from ocs_ci.ocs.exceptions import (ResourceWrongStatusException, CommandFailed)
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
import time
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
        self.namespace = self.args.get('namespace', 'myproject')
        self.amq_is_setup = False
        self.ocp = OCP()
        self.ns_obj = OCP(kind='namespace')
        self.pod_obj = OCP(kind='pod')
        self.kafka_obj = OCP(kind='Kafka')
        self.kafka_connect_obj = OCP(kind="KafkaConnect")
        self.kafka_bridge_obj = OCP(kind="KafkaBridge")
        self._create_namespace()
        self._clone_amq()

    def _create_namespace(self):
        """
        create namespace for amq
        """
        self.ocp.new_project(self.namespace)

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

        except (CommandFailed, CalledProcessError)as cf:
            log.error('Error during cloning of amq repository')
            raise cf

    def setup_amq_cluster_operator(self):
        """
        Function to setup amq-cluster_operator,
        the file file is pulling from github
        it will make sure cluster-operator pod is running
        """

        # self.amq_dir = constants.TEMPLATE_DEPLOYMENT_AMQ_CP
        run(f'oc apply -f {self.amq_dir} -n {self.namespace}', shell=True, check=True, cwd=self.dir)
        time.sleep(10)
        # Wait for strimzi-cluster-operator pod to be created
        if self.is_amq_pod_running(pod_pattern="cluster-operator"):
            log.info("strimzi-cluster-operator pod is in running state")
        else:
            raise ResourceWrongStatusException("strimzi-cluster-operator pod is not getting to running state")

    def is_amq_pod_running(self, pod_pattern="cluster-operator"):
        """
        The function checks if provided pod_pattern finds a pod and if the status is running or not
        Args:
            pod_pattern (str): the pattern for pod
        Returns:
            bool: status of pod: True if found pod is running
        """
        for pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, pod_pattern, self.namespace
        ):
            try:
                if pod[0] is not None:
                    amq_pod = pod[0]
                    break
            except IndexError as ie:
                log.error(pod_pattern + " pod not ready yet")
                raise ie
        # checking pod status
        if (self.pod_obj.wait_for_resource(
            condition='Running',
            resource_name=amq_pod,
            timeout=1600,
            sleep=30,
        )
        ):
            log.info(amq_pod + " pod is up and running")
            return True
        else:
            return False

    def setup_amq_kafka_persistent(self):
        """
        Function to setup amq-kafka-persistent, the file file is pulling from github
        it will make kind: Kafka and will make sure the status is running
        :return: kafka_persistent
        """

        try:
            kafka_persistent = templating.load_yaml(os.path.join(self.dir, self.amq_kafka_pers_yaml))
            self.kafka_persistent = OCS(**kafka_persistent)
            self.kafka_persistent.create()

        except(CommandFailed, CalledProcessError) as cf:
            log.error('Failed during setup of AMQ Kafka-persistent')
            raise cf
        time.sleep(20)
        if self.is_amq_pod_running(pod_pattern="zookeeper"):
            return self.kafka_persistent
        else:
            raise ResourceWrongStatusException("my-cluster-zookeeper Pod is not getting to running state")

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

        if self.is_amq_pod_running(pod_pattern="my-connect-cluster-connect"):
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
        if self.is_amq_pod_running(pod_pattern="my-bridge-bridge"):
            return self.kafka_bridge
        else:
            raise ResourceWrongStatusException("kafka_bridge_pod pod is not getting to running state")

    def setup_amq(self):
        """
        Setup AMQ from local folder,
        function will call all necessary sub functions to make sure amq installation is complete
        """
        self.setup_amq_cluster_operator()
        self.setup_amq_kafka_persistent()
        self.setup_amq_kafka_connect()
        self.setup_amq_kafka_bridge()
        self.amq_is_setup = True
        return self

    def cleanup(self):
        """
        Clean up function,
        will start to delete from amq cluster operator
        then amq-connector, persistent, bridge, at the end it will delete the created namespace
        """
        if self.amq_is_setup:
            self.kafka_persistent.delete()
            self.kafka_connect.delete()
            self.kafka_bridge.delete()
            run_cmd(f'oc delete -f {self.amq_dir}', shell=True, check=True, cwd=self.dir)
        run_cmd(f'oc delete project {self.namespace}')
        # Reset namespace to default
        switch_to_default_rook_cluster_project()
        self.ns_obj.wait_for_delete(resource_name=self.namespace)
