import json
import logging
import uuid

import pytest
import botocore.exceptions as boto3exception

from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_creds_are_missing,
    flowtests,
    skipif_openshift_dedicated,
)
from ocs_ci.framework.testlib import E2ETest, skipif_ocs_version
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    put_bucket_policy,
    get_bucket_policy,
    s3_put_object,
    s3_get_object,
    namespace_bucket_update,
    rm_object_recursive,
    setup_base_objects,
    compare_directory,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.node import drain_nodes, wait_for_nodes_status, schedule_nodes
from ocs_ci.ocs.resources.bucket_policy import (
    NoobaaAccount,
    gen_bucket_policy,
    HttpResponseParser,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs.resources.pod import wait_for_storage_pods

logger = logging.getLogger(__name__)

MCG_NS_RESULT_DIR = "/result"
MCG_NS_ORIGINAL_DIR = "/original"


@skipif_openshift_dedicated
@skipif_aws_creds_are_missing
@skipif_ocs_version("!=4.6")
class TestMcgNamespaceDisruptionsRpc(E2ETest):
    """
    Test MCG namespace disruption flow

    """

    labels_map = {
        "noobaa_core": constants.NOOBAA_CORE_POD_LABEL,
        "noobaa_endpoint": constants.NOOBAA_ENDPOINT_POD_LABEL,
        "noobaa_operator": constants.NOOBAA_OPERATOR_POD_LABEL,
    }

    @pytest.mark.polarion_id("OCS-2297")
    @flowtests
    def test_mcg_namespace_disruptions_rpc(
        self,
        mcg_obj,
        cld_mgr,
        awscli_pod,
        ns_resource_factory,
        bucket_factory,
        node_drain_teardown,
    ):
        """
        Test MCG namespace disruption flow

        1. Create NS resources using rpc calls
        2. Create NS bucket using rpc calls
        3. Upload to NS bucket
        4. Delete noobaa related pods and verify integrity of objects
        5. Create public access policy on NS bucket and verify Get op
        6. Drain nodes containing noobaa pods and verify integrity of objects
        7. Perform put operation to validate public access denial
        7. Edit/verify and remove objects on NS bucket

        """
        data = "Sample string content to write to a S3 object"
        object_key = "ObjKey-" + str(uuid.uuid4().hex)
        awscli_node_name = awscli_pod.get()["spec"]["nodeName"]

        aws_s3_creds = {
            "access_key_id": cld_mgr.aws_client.access_key,
            "access_key": cld_mgr.aws_client.secret_key,
            "endpoint": constants.MCG_NS_AWS_ENDPOINT,
            "region": config.ENV_DATA["region"],
        }

        # S3 account details
        user_name = "nb-user" + str(uuid.uuid4().hex)
        email = user_name + "@mail.com"

        logger.info("Setting up test files for upload, to the bucket/resources")
        setup_base_objects(awscli_pod, MCG_NS_ORIGINAL_DIR, MCG_NS_RESULT_DIR, amount=3)

        # Create the namespace resource and verify health
        aws_res = ns_resource_factory()

        # Create the namespace bucket on top of the namespace resources
        ns_bucket = bucket_factory(
            amount=1,
            interface="mcg-namespace",
            write_ns_resource=aws_res[1],
            read_ns_resources=[aws_res[1]],
        )[0].name
        logger.info(f"Namespace bucket: {ns_bucket} created")

        logger.info(f"Uploading objects to ns bucket: {ns_bucket}")
        sync_object_directory(
            awscli_pod,
            src=MCG_NS_ORIGINAL_DIR,
            target=f"s3://{ns_bucket}",
            s3_obj=mcg_obj,
        )

        for pod_to_respin in self.labels_map:
            logger.info(f"Re-spinning mcg resource: {self.labels_map[pod_to_respin]}")
            pod_obj = pod.Pod(
                **pod.get_pods_having_label(
                    label=self.labels_map[pod_to_respin],
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
            )

            pod_obj.delete(force=True)

            assert pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=self.labels_map[pod_to_respin],
                resource_count=1,
                timeout=300,
            )

            logger.info(
                f"Downloading objects from ns bucket: {ns_bucket} "
                f"after re-spinning: {self.labels_map[pod_to_respin]}"
            )
            sync_object_directory(
                awscli_pod,
                src=f"s3://{ns_bucket}",
                target=MCG_NS_RESULT_DIR,
                s3_obj=mcg_obj,
            )

            logger.info(
                f"Verifying integrity of objects "
                f"after re-spinning: {self.labels_map[pod_to_respin]}"
            )
            compare_directory(
                awscli_pod, MCG_NS_ORIGINAL_DIR, MCG_NS_RESULT_DIR, amount=3
            )

        # S3 account
        user = NoobaaAccount(mcg_obj, name=user_name, email=email, buckets=[ns_bucket])
        logger.info(f"Noobaa account: {user.email_id} with S3 access created")

        # Admin sets Public access policy(*)
        bucket_policy_generated = gen_bucket_policy(
            user_list=["*"],
            actions_list=["GetObject"],
            resources_list=[f'{ns_bucket}/{"*"}'],
        )
        bucket_policy = json.dumps(bucket_policy_generated)

        logger.info(
            f"Creating bucket policy on bucket: {ns_bucket} with wildcard (*) Principal"
        )
        put_policy = put_bucket_policy(mcg_obj, ns_bucket, bucket_policy)
        logger.info(f"Put bucket policy response from Admin: {put_policy}")

        logger.info(f"Getting bucket policy on bucket: {ns_bucket}")
        get_policy = get_bucket_policy(mcg_obj, ns_bucket)
        logger.info(f"Got bucket policy: {get_policy['Policy']}")

        # MCG admin writes an object to bucket
        logger.info(f"Writing object on bucket: {ns_bucket} by admin")
        assert s3_put_object(mcg_obj, ns_bucket, object_key, data), "Failed: PutObject"

        # Verifying whether Get operation is allowed to any S3 user
        logger.info(
            f"Get object action on namespace bucket: {ns_bucket} "
            f"with user: {user.email_id}"
        )
        assert s3_get_object(user, ns_bucket, object_key), "Failed: GetObject"

        # Upload files to NS target
        logger.info(f"Uploading objects directly to ns resource target: {aws_res[0]}")
        sync_object_directory(
            awscli_pod,
            src=MCG_NS_ORIGINAL_DIR,
            target=f"s3://{aws_res[0]}",
            signed_request_creds=aws_s3_creds,
        )

        for pod_to_drain in self.labels_map:
            pod_obj = pod.Pod(
                **pod.get_pods_having_label(
                    label=self.labels_map[pod_to_drain],
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
            )

            # Retrieve the node name on which the pod resides
            node_name = pod_obj.get()["spec"]["nodeName"]

            if awscli_node_name == node_name:
                logger.info(
                    f"Skipping node drain since aws cli pod node: "
                    f"{awscli_node_name} is same as {pod_to_drain} "
                    f"pod node: {node_name}"
                )
                continue

            # Drain the node
            drain_nodes([node_name])
            wait_for_nodes_status(
                [node_name], status=constants.NODE_READY_SCHEDULING_DISABLED
            )
            schedule_nodes([node_name])
            wait_for_nodes_status(timeout=300)

            # Retrieve the new pod
            pod_obj = pod.Pod(
                **pod.get_pods_having_label(
                    label=self.labels_map[pod_to_drain],
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )[0]
            )
            wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=120)

            # Verify all storage pods are running
            wait_for_storage_pods()

            logger.info(
                f"Downloading objects from ns bucket: {ns_bucket} "
                f"after draining node: {node_name} with pod {pod_to_drain}"
            )
            sync_object_directory(
                awscli_pod,
                src=f"s3://{ns_bucket}",
                target=MCG_NS_RESULT_DIR,
                s3_obj=mcg_obj,
            )

            logger.info(
                f"Verifying integrity of objects "
                f"after draining node with pod: {pod_to_drain}"
            )
            compare_directory(
                awscli_pod, MCG_NS_ORIGINAL_DIR, MCG_NS_RESULT_DIR, amount=3
            )

        logger.info(f"Editing the namespace resource bucket: {ns_bucket}")
        namespace_bucket_update(
            mcg_obj,
            bucket_name=ns_bucket,
            read_resource=[aws_res[1]],
            write_resource=aws_res[1],
        )

        logger.info(f"Verifying object download after edit on ns bucket: {ns_bucket}")
        sync_object_directory(
            awscli_pod,
            src=f"s3://{ns_bucket}",
            target=MCG_NS_RESULT_DIR,
            s3_obj=mcg_obj,
        )

        # Verifying whether Put object action is denied
        logger.info(
            f"Verifying whether user: {user.email_id} has only public read access"
        )
        try:
            s3_put_object(
                s3_obj=user, bucketname=ns_bucket, object_key=object_key, data=data
            )
        except boto3exception.ClientError as e:
            logger.info(e.response)
            response = HttpResponseParser(e.response)
            if response.error["Code"] == "AccessDenied":
                logger.info("Put object action has been denied access")
            else:
                raise UnexpectedBehaviour(
                    f"{e.response} received invalid error code "
                    f"{response.error['Code']}"
                )
        else:
            assert (
                False
            ), "Put object operation was granted access, when it should have denied"

        logger.info(f"Removing objects from ns bucket: {ns_bucket}")
        rm_object_recursive(awscli_pod, target=ns_bucket, mcg_obj=mcg_obj)
