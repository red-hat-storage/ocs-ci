import json
import logging
import os
import tempfile
from time import sleep

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import (
    craft_s3_command,
    create_unique_resource_name,
    default_storage_class,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
    wait_for_pods_to_be_running,
)
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)

NOTIFS_YAML_PATH_NB_CR = "/spec/bucketNotifications"


class BucketNotificationsManager:
    """
    A class to manage the MCG bucket notifications feature
    """

    @property
    def nb_config_resource(self):
        return OCP(
            kind="noobaa",
            namespace=self.namespace,
            resource_name="noobaa",
        )

    def __init__(self):
        self.namespace = config.ENV_DATA["cluster_namespace"]
        self.amq = AMQ()
        self.kafka_topics = []
        self.conn_secrets = []
        self.pvc_factory = None

    def setup_kafka(self):
        """
        Deploy an AMQ cluster and set up Kafka
        """
        sc = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

        # Avoid the long setup in dev-mode if the AMQ cluster already exists
        # in dev-mode we don't cleanup the AMQ cluster
        if (
            not config.RUN["cli_params"].get("dev_mode")
            or not self.amq.check_amq_cluster_exists()
        ):
            self.amq.setup_amq_cluster(sc.name)

    def enable_bucket_notifs_on_cr(self, use_provided_pvc=False):
        """
        Set the bucket notifications feature on the NooBaa CR

        Args:
            use_provided_pvc(bool): Whether to set a custom PVC or use NooBaa's default.

        Note that if use_provided_pvc is set to True, a PVC factory must be set
        to the pvc_factory attribute of the BucketNotificationsManager instance.
        """
        logger.info("Enabling bucket notifications on the NooBaa CR")

        # Build a patch command to enable guaranteed bucket logs
        bucket_notifs_dict = {"connections": [], "enabled": True}

        # Set a provided PVC if needed
        provided_notifs_pvc = None
        if use_provided_pvc and not self.pvc_factory:
            logger.error(
                (
                    "BucketNotificationsManager: "
                    "a PVC factory must be set to use a provided PVC.\n"
                    "Continuing with the default PVC."
                )
            )
        elif use_provided_pvc and self.pvc_factory:
            provided_notifs_pvc = self.pvc_factory(
                interface=constants.CEPHFILESYSTEM,
                project=OCP(namespace=self.namespace),
                size=20,
                access_mode=constants.ACCESS_MODE_RWX,
            ).name
            bucket_notifs_dict["pvc"] = provided_notifs_pvc

        patch_params = [
            {
                "op": "add",
                "path": NOTIFS_YAML_PATH_NB_CR,
                "value": bucket_notifs_dict,
            }
        ]

        # Try patching via add, and if it fails - replace instead
        try:
            self.nb_config_resource.patch(
                params=json.dumps(patch_params),
                format_type="json",
            )
        except CommandFailed as e:
            if "already exists" in str(e).lower():
                patch_params[0]["op"] = "replace"
                self.nb_config_resource.patch(
                    params=json.dumps(patch_params),
                    format_type="json",
                )
            else:
                logger.error(f"Failed to enable bucket notifications: {e}")
                raise e

        # Label the PVC and PV to tolerate as leftovers after the test
        pvc_name = provided_notifs_pvc or constants.DEFAULT_MCG_BUCKET_NOTIFS_PVC
        pvc_ocp_obj = OCP(
            namespace=self.namespace,
            kind="pvc",
            resource_name=pvc_name,
        )

        # First get attempts may fail if MCG just created the PVC due to the patch
        @retry((CommandFailed, KeyError), tries=5, delay=10, backoff=1)
        def _get_pv_name():
            pvc_ocp_obj = OCP(
                namespace=self.namespace,
                kind="pvc",
                resource_name=pvc_name,
            )
            pv_name = pvc_ocp_obj.get()["spec"]["volumeName"]
            return pv_name

        pv_name = _get_pv_name()

        pv_ocp_obj = OCP(
            namespace=self.namespace,
            kind="pv",
            resource_name=pv_name,
        )
        for ocp_obj in (pvc_ocp_obj, pv_ocp_obj):
            ocp_obj.add_label(
                resource_name=ocp_obj.resource_name,
                label=constants.CUSTOM_MCG_LABEL,
            )
        wait_for_pods_to_be_running(
            pod_names=[constants.NOOBAA_CORE_POD],
            timeout=60,
            sleep=10,
        )
        logger.info("Guaranteed bucket logs have been enabled")

    def disable_bucket_logging_on_cr(self):
        """
        Unset the bucket notifications feature on the NooBaa CR
        """
        logger.info("Disabling bucket notifications on the NooBaa CR")

        try:
            patch_params = [
                {
                    "op": "replace",
                    "path": NOTIFS_YAML_PATH_NB_CR,
                    "value": None,
                },
            ]
            self.nb_config_resource.patch(
                params=json.dumps(patch_params),
                format_type="json",
            )
        except CommandFailed as e:
            if "not found" in str(e).lower():
                logger.info("The bucketNotifications field was not found")
            else:
                logger.error(f"Failed to disable bucket notifications: {e}")
                raise e
        wait_for_pods_to_be_running(
            pod_names=[constants.NOOBAA_CORE_POD],
            timeout=60,
            sleep=10,
        )
        logger.info("Bucket notifications have been disabled")

    def create_kafka_topic(self, topic_name=""):
        """
        Create a KafkaTopic - a receiver of bucket notifications

        Args:
            topic_name(str|optional): Name of the Kafka topic

        Returns:
            str: Name of the created Kafka topic
        """
        topic_name = topic_name or create_unique_resource_name(
            resource_description="nb-notif", resource_type="kafka-topic"
        )
        topic = self.amq.create_kafka_topic(topic_name)
        self.kafka_topics.append(topic)
        return topic_name

    def create_kafka_conn_secret(self, topic):
        """
        Create secret from a JSON file that
        defines the Kafka connection for NooBaa to use

        Args:
            topic(str): Name of the Kafka topic

        Returns:
            secret_ocp_obj: OCP instance of the created secret
            conn_config_path: MCG's Path to the connection config file
        """
        conn_name = create_unique_resource_name(
            resource_description="nb-notif", resource_type="kafka-conn"
        )
        secret_name = conn_name + "-secret"
        conn_file_name = ""

        kafka_conn_config = {
            "kafka_options_object": {
                "metadata.broker.list": constants.KAFKA_ENDPOINT,
            },
            "notification_protocol": "kafka",
            "topic": topic,
            "name": conn_name,
        }

        with tempfile.NamedTemporaryFile(
            mode="w+", prefix="kafka_conn_", suffix=".json"
        ) as conn_file:
            conn_file_name = os.path.basename(conn_file.name)
            conn_file.write(json.dumps(kafka_conn_config))
            conn_file.flush()  # Ensure that the data is written
            OCP().exec_oc_cmd(
                f"create secret generic {secret_name} --from-file={conn_file.name} -n {self.namespace}"
            )

        secret_ocp_obj = OCP(
            kind="secret",
            namespace=self.namespace,
            resource_name=secret_name,
        )
        self.conn_secrets.append(secret_ocp_obj)

        # MCG stores the connection config file in a directory named after the secret
        conn_config_path = os.path.join(secret_name, conn_file_name)

        return secret_ocp_obj, conn_config_path

    def add_notif_conn_to_noobaa_cr(self, secret, wait=True):
        """
        Add a connection secret to list of bucket notifications
        connections in the NooBaa CR.

        Args:
            secret(ocs_ci.ocs.ocp.OCP): OCP instance of the secret to add
            wait(bool): Whether to wait for the NooBaa resources to be ready
        """
        conn_data = {
            "name": secret.resource_name,
            "namespace": secret.namespace,
        }
        patch_path = os.path.join(NOTIFS_YAML_PATH_NB_CR, "connections")
        add_op = [{"op": "add", "path": f"{patch_path}/-", "value": conn_data}]
        self.nb_config_resource.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(add_op),
            format_type="json",
        )
        if wait:
            MCG.wait_for_ready_status()

    def create_and_register_kafka_topic_with_noobaa(self, topic_name="", wait=True):
        """
        Create a Kafka topic and register it with NooBaa via a connection secret

        Args:
            topic_name(str|optional): Name of the Kafka topic
            wait(bool): Whether to wait for the NooBaa resources to be ready

        Returns:
            tuple: Kafka topic name and MCG's Path to the connection config file
        """
        topic = self.create_kafka_topic(topic_name)
        secret, conn_config_path = self.create_kafka_conn_secret(topic)
        self.add_notif_conn_to_noobaa_cr(secret, wait)

        return topic, conn_config_path

    def put_bucket_notification(
        self, awscli_pod, mcg_obj, bucket, events, conn_config_path, wait=True
    ):
        """
        Configure bucket notifications on a bucket using the AWS CLI

        Args:
            awscli_pod(Pod): Pod instance of the AWS CLI pod
            mcg_obj(MCG): MCG object
            bucket(str): Name of the bucket
            events(list): List of events to trigger notifications
            conn_config_path(str): MCG's Path to the connection config file
            wait(bool): Whether to wait for the notification to propagate
        """
        rand_id = create_unique_resource_name(
            resource_description="notif", resource_type="id"
        )
        notif_config = {
            "TopicConfiguration": [
                {
                    "Id": rand_id,
                    "Events": events,
                    "TopicArn": conn_config_path,
                }
            ]
        }
        notif_config_json = json.dumps(notif_config).replace('"', '\\"')
        awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(
                f"put-bucket-notification-configuration --bucket {bucket} "
                f"--notification-configuration '{notif_config_json}'",
                mcg_obj=mcg_obj,
                api=True,
            )
        )
        if wait:
            logger.info("Waiting for put-bucket-notification to propagate")
            sleep(60)

    def get_bucket_notification_configuration(self, awscli_pod, mcg_obj, bucket):
        """
        Get the bucket notification configuration of a bucket

        Args:
            awscli_pod(Pod): Pod instance of the AWS CLI pod
            mcg_obj(MCG): MCG object
            bucket(str): Name of the bucket

        Returns:
            dict: Bucket notification configuration
        """
        return awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(
                f"get-bucket-notification-configuration --bucket {bucket}",
                mcg_obj=mcg_obj,
                api=True,
            )
        )

    def get_events(self, topic, timeout_in_ms=10000):
        """
        Query a Kafka topic for events

        Args:
            topic(str): Name of the Kafka topic
            timeout_in_ms(int): How long to wait for events

        Returns:
            list: List of event dictionaries
        """
        # Query the Kafka topic via the Kafka consumer script on any of the Kafka pods
        kafka_pods = [
            Pod(**pod_info)
            for pod_info in get_pods_having_label(
                namespace=constants.AMQ_NAMESPACE, label=constants.KAFKA_PODS_LABEL
            )
        ]
        for kafka_pod in kafka_pods:
            cmd = (
                f"bin/kafka-console-consumer.sh --bootstrap-server {constants.KAFKA_ENDPOINT} "
                f"--topic {topic} --from-beginning --timeout-ms {timeout_in_ms}"
            )

            # for some test involving node shutdown, we make sure we get the events
            # from available kafka pods
            try:
                raw_resp = kafka_pod.exec_cmd_on_pod(command=cmd, out_yaml_format=False)
            except CommandFailed as err:
                if "connect: no route to host" in err.args[0]:
                    continue
                raise err
            break

        # Parse the raw response into a list of event dictionaries
        events = []
        for line in raw_resp.split("\n"):
            if line:
                parsed_event = json.loads(line)
                # Every event is nested in a single-element list
                # which is nested in a dict with the key "Records"
                if isinstance(parsed_event, dict) and "Records" in parsed_event:
                    event_dict = parsed_event["Records"][0]
                    events.append(event_dict)
        logger.info(events)
        return events

    def cleanup(self):
        """
        Clean up the resources created by the BucketNotificationsManager
        1. Disable bucket notifications on the NooBaa CR
        2. Delete connection secrets
        3. Delete Kafka topics
        4. Clean up the AMQ cluster
        """
        self.disable_bucket_logging_on_cr()
        for secret in self.conn_secrets:
            secret.delete(resource_name=secret.resource_name)
        for topic in self.kafka_topics:
            topic.delete()

        # Don't cleanup the AMQ cluster in dev-mode for faster re-runs
        if not config.RUN["cli_params"].get("dev_mode"):
            self.amq.cleanup()
