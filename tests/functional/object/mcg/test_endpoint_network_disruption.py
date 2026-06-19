import logging
import os
import subprocess
import tempfile
import threading
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    runs_on_provider,
    skipif_managed_service,
    skipif_proxy_cluster,
)
from ocs_ci.framework.testlib import (
    MCGTest,
    skipif_disconnected_cluster,
    tier4c,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.bucket_utils import craft_s3_command
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources import pod
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


@mcg
@red_squad
@runs_on_provider
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
class TestEndpointCloudNetworkDisruption(MCGTest):
    """
    Test class for verifying noobaa-endpoint pod resilience when
    cloud storage connections are severed mid-stream.
    """

    LARGE_FILE_SIZE_MB = 2048
    NETWORK_POLICY_NAME = "block-cloud-egress"

    @tier4c
    @pytest.mark.parametrize(
        argnames=["disruption_during", "bucketclass_dict"],
        argvalues=[
            pytest.param(
                "download",
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
                marks=pytest.mark.polarion_id("OCS-7901"),
                id="download-namespacestore-aws",
            ),
            pytest.param(
                "download",
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
                marks=pytest.mark.polarion_id("OCS-7902"),
                id="download-namespacestore-azure",
            ),
            pytest.param(
                "download",
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"ibmcos": [(1, None)]},
                    },
                },
                marks=pytest.mark.polarion_id("OCS-7903"),
                id="download-namespacestore-ibmcos",
            ),
            pytest.param(
                "download",
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, None)]},
                },
                marks=pytest.mark.polarion_id("OCS-7904"),
                id="download-backingstore-aws",
            ),
            pytest.param(
                "download",
                {
                    "interface": "OC",
                    "backingstore_dict": {"azure": [(1, None)]},
                },
                marks=pytest.mark.polarion_id("OCS-7905"),
                id="download-backingstore-azure",
            ),
            pytest.param(
                "download",
                {
                    "interface": "OC",
                    "backingstore_dict": {"gcp": [(1, None)]},
                },
                marks=pytest.mark.polarion_id("OCS-7906"),
                id="download-backingstore-gcp",
            ),
            pytest.param(
                "download",
                {
                    "interface": "OC",
                    "backingstore_dict": {"ibmcos": [(1, None)]},
                },
                marks=pytest.mark.polarion_id("OCS-7907"),
                id="download-backingstore-ibmcos",
            ),
            pytest.param(
                "upload",
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"aws": [(1, None)]},
                    },
                },
                marks=pytest.mark.polarion_id("OCS-7908"),
                id="upload-namespacestore-aws",
            ),
            pytest.param(
                "upload",
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"azure": [(1, None)]},
                    },
                },
                marks=pytest.mark.polarion_id("OCS-7909"),
                id="upload-namespacestore-azure",
            ),
            pytest.param(
                "upload",
                {
                    "interface": "OC",
                    "namespace_policy_dict": {
                        "type": "Single",
                        "namespacestore_dict": {"ibmcos": [(1, None)]},
                    },
                },
                marks=pytest.mark.polarion_id("OCS-7910"),
                id="upload-namespacestore-ibmcos",
            ),
            pytest.param(
                "upload",
                {
                    "interface": "OC",
                    "backingstore_dict": {"aws": [(1, None)]},
                },
                marks=pytest.mark.polarion_id("OCS-7911"),
                id="upload-backingstore-aws",
            ),
            pytest.param(
                "upload",
                {
                    "interface": "OC",
                    "backingstore_dict": {"azure": [(1, None)]},
                },
                marks=pytest.mark.polarion_id("OCS-7912"),
                id="upload-backingstore-azure",
            ),
            pytest.param(
                "upload",
                {
                    "interface": "OC",
                    "backingstore_dict": {"gcp": [(1, None)]},
                },
                marks=pytest.mark.polarion_id("OCS-7913"),
                id="upload-backingstore-gcp",
            ),
            pytest.param(
                "upload",
                {
                    "interface": "OC",
                    "backingstore_dict": {"ibmcos": [(1, None)]},
                },
                marks=pytest.mark.polarion_id("OCS-7914"),
                id="upload-backingstore-ibmcos",
            ),
        ],
    )
    def test_endpoint_survives_cloud_connection_severed(
        self,
        request,
        disruption_during,
        bucketclass_dict,
        mcg_obj,
        awscli_pod_session,
        bucket_factory,
    ):
        """
        Verify that noobaa-endpoint pods survive when TCP connections
        to cloud storage are severed mid-stream, rather than crashing
        with an unhandled exception.

        Test steps:
            1. Create a bucket backed by cloud storage (namespacestore
               or backingstore depending on parametrization)
            2. Generate a large file on the awscli pod
            3. For download disruption: upload the file first
            4. Record endpoint pod restart counts
            5. Start a large upload or download in a background thread
            6. Apply a NetworkPolicy to block all external egress
               from noobaa-endpoint pods only
            7. Verify endpoint pods did not crash or restart
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        awscli_pod = awscli_pod_session

        store_dict = bucketclass_dict.get(
            "backingstore_dict",
            bucketclass_dict.get("namespace_policy_dict", {}).get(
                "namespacestore_dict", {}
            ),
        )
        platform = next(iter(store_dict))

        # Step 1: Create bucket with parametrized bucketclass
        logger.info(f"Creating bucket backed by {platform} cloud storage")
        bucket = bucket_factory(
            amount=1,
            interface=bucketclass_dict["interface"],
            bucketclass=bucketclass_dict,
        )[0]
        logger.info(f"Created bucket: {bucket.name}")

        # Step 2: Generate a large file
        logger.info(f"Generating {self.LARGE_FILE_SIZE_MB} MB file")
        awscli_pod.exec_sh_cmd_on_pod(
            f"dd if=/dev/urandom of=/tmp/bigfile "
            f"bs=1M count={self.LARGE_FILE_SIZE_MB} status=none",
            timeout=300,
        )
        request.addfinalizer(
            lambda: awscli_pod.exec_sh_cmd_on_pod(
                "rm -f /tmp/bigfile /tmp/bigfile_download"
            )
        )

        # Step 3: For download disruption, upload the file first
        if disruption_during == "download":
            logger.info("Uploading file before download disruption test")
            upload_cmd = craft_s3_command(
                f"cp /tmp/bigfile s3://{bucket.name}/bigfile",
                mcg_obj=mcg_obj,
            )
            awscli_pod.exec_cmd_on_pod(upload_cmd, out_yaml_format=False, timeout=600)
            logger.info("Upload complete")

        # Step 4: Record endpoint pod state before disruption
        endpoint_pods = pod.get_noobaa_endpoint_pods()
        assert endpoint_pods, "No noobaa-endpoint pods found"
        restart_counts_before = {p.name: p.restart_count for p in endpoint_pods}
        logger.info(f"Endpoint pod restart counts before: {restart_counts_before}")

        # Step 5: Register NetworkPolicy cleanup finalizer
        def _cleanup_network_policy():
            try:
                ocp.OCP(kind=constants.NETWORK_POLICY, namespace=namespace).delete(
                    resource_name=self.NETWORK_POLICY_NAME
                )
                logger.info(f"Deleted NetworkPolicy {self.NETWORK_POLICY_NAME}")
            except CommandFailed:
                logger.warning(
                    f"NetworkPolicy {self.NETWORK_POLICY_NAME} already deleted"
                )

        request.addfinalizer(_cleanup_network_policy)

        # Step 6: Start operation in background, then apply NetworkPolicy
        logger.info(f"Starting large {disruption_during} in background thread")
        operation_disrupted = threading.Event()

        if disruption_during == "download":
            s3_cmd = craft_s3_command(
                f"cp s3://{bucket.name}/bigfile /tmp/bigfile_download",
                mcg_obj=mcg_obj,
                max_attempts=2,
            )
        else:
            s3_cmd = craft_s3_command(
                f"cp /tmp/bigfile s3://{bucket.name}/bigfile",
                mcg_obj=mcg_obj,
                max_attempts=2,
            )

        def _s3_operation():
            try:
                awscli_pod.exec_cmd_on_pod(s3_cmd, out_yaml_format=False, timeout=120)
                logger.info(
                    f"{disruption_during.capitalize()} completed before "
                    f"network disruption took effect"
                )
            except (CommandFailed, TimeoutError, subprocess.TimeoutExpired):
                operation_disrupted.set()
                logger.info(
                    f"{disruption_during.capitalize()} failed as expected "
                    f"due to network disruption"
                )

        operation_thread = threading.Thread(target=_s3_operation, daemon=True)
        operation_thread.start()
        time.sleep(2)

        logger.info(
            "Applying NetworkPolicy to block external egress "
            "from noobaa-endpoint pods only"
        )
        network_policy_data = templating.load_yaml(
            constants.TEMPLATE_BLOCK_NB_EGRESS_NETWORK_POLICY
        )
        network_policy_data["metadata"]["namespace"] = namespace
        temp_yaml = tempfile.NamedTemporaryFile(
            mode="w+",
            prefix="network_policy_",
            suffix=".yaml",
            delete=False,
        )
        templating.dump_data_to_temp_yaml(network_policy_data, temp_yaml.name)
        ocp_obj = ocp.OCP(kind=constants.NETWORK_POLICY, namespace=namespace)
        ocp_obj.exec_oc_cmd(f"apply -f {temp_yaml.name}")
        os.unlink(temp_yaml.name)

        # NetworkPolicy has no STATUS; wait on NAME to confirm creation
        ocp_obj.wait_for_resource(
            condition=self.NETWORK_POLICY_NAME,
            column="NAME",
            resource_name=self.NETWORK_POLICY_NAME,
            timeout=30,
        )
        logger.info(
            "NetworkPolicy applied - external egress blocked "
            "for noobaa-endpoint pods"
        )

        operation_thread.join(timeout=120)
        assert operation_disrupted.is_set(), (
            f"{disruption_during.capitalize()} completed before the "
            f"NetworkPolicy disrupted it. Increase LARGE_FILE_SIZE_MB "
            f"to ensure the {disruption_during} is still in progress "
            f"when the network is severed."
        )

        # Step 7: Verify endpoint pods survived
        logger.info("Waiting 30 seconds for any crash to manifest, then verifying")
        time.sleep(30)

        endpoint_pods_after = pod.get_noobaa_endpoint_pods()
        for ep in endpoint_pods_after:
            pod_status = ep.get()["status"]["phase"]
            assert pod_status == constants.STATUS_RUNNING, (
                f"Endpoint pod {ep.name} is in {pod_status} state "
                f"instead of Running"
            )

            if ep.name in restart_counts_before:
                current_restarts = ep.restart_count
                assert current_restarts == restart_counts_before[ep.name], (
                    f"Endpoint pod {ep.name} restarted: "
                    f"before={restart_counts_before[ep.name]}, "
                    f"after={current_restarts}"
                )

            panics = pod.search_pattern_in_pod_logs(
                pod_name=ep.name,
                pattern="PANIC.*uncaughtException",
                container="endpoint",
                since="5m",
            )
            assert (
                not panics
            ), f"Found PANIC/uncaughtException in endpoint pod {ep.name}: {panics}"

        logger.info(
            f"All endpoint pods survived the cloud connection "
            f"disruption during {disruption_during}"
        )
