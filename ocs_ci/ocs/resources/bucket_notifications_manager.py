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
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_noobaa_pods,
    get_pods_having_label,
    wait_for_pods_to_be_running,
)

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
        pv_name = pvc_ocp_obj.get()["spec"]["volumeName"]
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
            conn_file_name: Name of the JSON file
        """
        conn_name = create_unique_resource_name(
            resource_description="nb-notif", resource_type="kafka-conn"
        )
        secret_name = conn_name + "-secret"
        conn_file_name = ""

        kafka_conn_config = {
            "metadata.broker.list": constants.KAFKA_ENDPOINT,
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
        return secret_ocp_obj, conn_file_name

    def add_notif_conn_to_noobaa_cr(self, secret):
        """
        Add a connection secret to list of bucket notifications
        connections in the NooBaa CR.

        Args:
            secret(ocs_ci.ocs.ocp.OCP): OCP instance of the secret to add
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

        nb_pods = [pod.name for pod in get_noobaa_pods()]
        wait_for_pods_to_be_running(
            namespace=self.namespace,
            pod_names=nb_pods,
            timeout=60,
            sleep=10,
        )
        CephCluster().wait_for_noobaa_health_ok()

    def put_bucket_notification(self, awscli_pod, mcg_obj, bucket, events, conn_file):
        """
        Configure bucket notifications on a bucket using the AWS CLI

        Args:
            awscli_pod(Pod): Pod instance of the AWS CLI pod
            mcg_obj(MCG): MCG object
            bucket(str): Name of the bucket
            events(list): List of events to trigger notifications
            conn_file(str): Name of the file that NooBaa uses to connect to Kafka
        """
        rand_id = create_unique_resource_name(
            resource_description="notif", resource_type="id"
        )
        notif_config = {
            "TopicConfiguration": {
                "Id": rand_id,
                "Events": events,
                "Topic": conn_file,
            }
        }
        notif_config_json = json.dumps(notif_config).replace('"', '\\"')
        awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(
                f"put-bucket-notification --bucket {bucket} --notification-configuration '{notif_config_json}'",
                mcg_obj=mcg_obj,
                api=True,
            )
        )
        logger.info("Waiting for put-bucket-notification to propogate")
        sleep(60)

    def get_bucket_notification(self, awscli_pod, mcg_obj, bucket):
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
                f"get-bucket-notification --bucket {bucket}",
                mcg_obj=mcg_obj,
                api=True,
            )
        )

    def get_events(self, topic, timeout_in_ms=5000):
        """
        Query a Kafka topic for events

        Args:
            topic(str): Name of the Kafka topic
            timeout_in_ms(int): How long to wait for events

        Returns:
            list: List of event dictionaries
        """
        # Query the Kafka topic via the Kafka consumer script on any of the Kafka pods
        kafka_pod = Pod(
            **get_pods_having_label(
                namespace=constants.AMQ_NAMESPACE, label=constants.KAFKA_PODS_LABEL
            )[0]
        )
        cmd = (
            f"bin/kafka-console-consumer.sh --bootstrap-server {constants.KAFKA_ENDPOINT} "
            f"--topic {topic} --from-beginning --timeout-ms {timeout_in_ms}"
        )
        raw_resp = kafka_pod.exec_cmd_on_pod(command=cmd, out_yaml_format=False)

        # Parse the raw response into a list of event dictionaries
        events = []
        for line in raw_resp.split("\n"):
            if line:
                # Every event is nested in a single-element list
                event_dict = json.loads(line)["Records"][0]
                events.append(event_dict)
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
        self.amq.cleanup()
