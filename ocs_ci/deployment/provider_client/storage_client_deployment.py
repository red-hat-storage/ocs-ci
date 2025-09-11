"""
This module provides installation of ODF and native storage-client creation in provider mode
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.rados_utils import (
    verify_cephblockpool_status,
    check_phase_of_rados_namespace,
)
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster
from ocs_ci.ocs.bucket_utils import check_pv_backingstore_type
from ocs_ci.ocs.resources import pod
from ocs_ci.helpers.helpers import verify_block_pool_exists
from ocs_ci.ocs.resources.storageconsumer import verify_storage_consumer_resources

log = logging.getLogger(__name__)


def verify_provider_mode_deployment():
    """
    This method verifies provider mode deployment

    """

    pod_obj = ocp.OCP(kind="Pod", namespace=config.ENV_DATA["cluster_namespace"])
    # Check ux server pod, ocs-provider server pod and rgw pods are up and running
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.UX_BACKEND_SERVER_LABEL,
        resource_count=1,
        timeout=180,
    )
    # Check nooba db pod is up and running
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.NOOBAA_APP_LABEL,
        resource_count=1,
        timeout=300,
    )
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.RGW_APP_LABEL,
        resource_count=1,
        timeout=300,
    )
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.PROVIDER_SERVER_LABEL,
        resource_count=1,
        timeout=300,
    )
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.RGW_APP_LABEL,
        resource_count=1,
        timeout=300,
    )

    # Check ocs-storagecluster is in 'Ready' status
    log.info("Verify storagecluster on Ready state")
    verify_storage_cluster()

    # Check backing storage is s3-compatible
    backingstore_type = check_pv_backingstore_type()
    log.info(f"backingstore value: {backingstore_type}")
    assert backingstore_type == constants.BACKINGSTORE_TYPE_S3_COMP

    # Verify rgw pod restart count is 0
    rgw_restart_count = pod.fetch_rgw_pod_restart_count()
    assert (
        rgw_restart_count == 0
    ), f"Error rgw pod has restarted {rgw_restart_count} times"

    Deployment().wait_for_csv(
        defaults.OCS_CLIENT_OPERATOR_NAME, config.ENV_DATA["cluster_namespace"]
    )
    assert verify_block_pool_exists(
        constants.DEFAULT_BLOCKPOOL
    ), f"{constants.DEFAULT_BLOCKPOOL} is not created"
    assert verify_cephblockpool_status(), "the cephblockpool is not in Ready phase"
    assert check_phase_of_rados_namespace(), "The radosnamespace is not in Ready phase"
    verify_storage_consumer_resources(constants.INTERNAL_STORAGE_CONSUMER_NAME)
