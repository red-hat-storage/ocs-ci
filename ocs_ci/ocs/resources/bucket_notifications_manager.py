import json
import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_unique_resource_name, default_storage_class
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.amq import AMQ
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running

logger = logging.getLogger(__name__)

NOTIFS_YAML_PATH_NB_CR = "/spec/bucketNotifications"


class BucketNotificationsManager:
    """
    A class to manage the MCG bucket notifications feature
    """

    @property
    def nb_config_resource(self):
        """
        Return the NooBaa configuration resource
        Note that this might change in the future.

        Returns:
            ocs_ci.ocs.ocp.OCP: OCP instance of the NooBaa configuration resource
        """
        return OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )

    def __init__(self):
        self.amq = AMQ()
        self.kafka_topics = []
        self.conn_secrets = []
        self.cur_logs_pvc = constants.DEFAULT_MCG_BUCKET_NOTIFS_PVC
        self.kafkadrop_pod = self.kafkadrop_svc = self.kafkadrop_route = None

    def setup_kafka(self):
        """
        TODO
        """
        # Get sc
        sc = default_storage_class(interface_type=constants.CEPHBLOCKPOOL)

        # Deploy amq cluster
        self.amq.setup_amq_cluster(sc.name)

        # Create Kafkadrop pod
        (
            self.kafkadrop_pod,
            self.kafkadrop_pod,
            self.kafkadrop_route,
        ) = self.amq.create_kafkadrop()

    def cleanup_kafka(self):
        for topic in self.kafka_topics:
            topic.delete()
        self.kafka_topics = []
        if self.kafkadrop_pod:
            self.kafkadrop_pod.delete()
        if self.kafkadrop_route:
            self.kafkadrop_route.delete()

        self.amq.cleanup()

    def enable_bucket_notifs_on_cr(self, notifs_pvc=None):
        """
        Set the bucket notifications feature on the NooBaa CR

        Args:
            notifs_pvc(str|optional): Name of a provided PVC for MCG to use for
                                      intermediate logging of the events.
            Note:
                If not provided, a PVC will be automatically be created
                by MCG when first enabling the feature.
        """
        logger.info("Enabling bucket notifications on the NooBaa CR")

        # Build a patch command to enable guaranteed bucket logs
        bucket_notifs_dict = {"connections": [], "enabled": True}

        # Add the bucketLoggingPVC field if provided
        if notifs_pvc:
            bucket_notifs_dict["pvc"] = notifs_pvc

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

        self.cur_logs_pvc = (
            notifs_pvc if notifs_pvc else constants.DEFAULT_MCG_BUCKET_NOTIFS_PVC
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
            if "not found" in str(e):
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

    def add_new_notif_conn(self, name=""):
        """
        1. Create a Kafka topic
        2. Create a secret with the Kafka connection details
        3. Add the connection to the NooBaa CR
        """
        topic_name = name or create_unique_resource_name(
            resource_description="nb-notif", resource_type="kafka-topic"
        )
        topic_obj = self.amq.create_kafka_topic(topic_name)
        self.kafka_topics.append(topic_obj)
        secret, conn_file_name = self.create_kafka_connection_secret(topic_name)
        self.add_notif_conn_to_noobaa_cr(secret)

        return conn_file_name

    def create_kafka_connection_secret(self, topic):
        """
        TODO
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        conn_name = create_unique_resource_name(resource_type="kafka-conn")
        secret_name = conn_name + "-secret"
        file_name = ""

        kafka_conn_config = {
            "metadata.broker.list": "my-cluster-kafka-bootstrap.myproject.svc.cluster.local:9092",
            "notification_protocol": "kafka",
            "topic": topic,
            "name": conn_name,
        }

        with tempfile.NamedTemporaryFile(
            mode="w+", prefix="kafka_conn_", suffix=".kv", delete=True
        ) as conn_file:
            file_name = conn_file.name
            for key, value in kafka_conn_config.items():
                conn_file.write(f"{key}={value}\n")

            OCP().exec_oc_cmd(
                f"create secret generic {secret_name} --from-file={conn_file.name} -n {namespace}"
            )

        secret_ocp_obj = OCP(
            kind="secret",
            namespace=namespace,
            resource_name=secret_name,
        )
        self.conn_secrets.append(secret_ocp_obj)

        return secret_ocp_obj, file_name

    def add_notif_conn_to_noobaa_cr(self, secret):
        """
        TODO
        """
        nb_ocp_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa",
        )
        conn_data = {
            "name": secret.name,
            "namespace": secret.namespace,
        }
        patch_path = "/spec/bucketNotification/connections"
        add_op = [{"op": "add", "path": f"{patch_path}/-", "value": conn_data}]
        nb_ocp_obj.patch(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            params=json.dumps(add_op),
            format_type="json",
        )

        wait_for_pods_to_be_running(
            pod_names=[constants.NOOBAA_CORE_POD],
            timeout=60,
            sleep=10,
        )

    def get_events(self, topic):
        """
        TODO
        """
        pass

    def cleanup(self):
        """
        TODO
        """
        self.disable_bucket_logging_on_cr()
        for secret in self.conn_secrets:
            secret.delete()
        self.cleanup_kafka()
