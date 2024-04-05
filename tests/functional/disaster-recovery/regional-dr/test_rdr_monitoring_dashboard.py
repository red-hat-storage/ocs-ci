import logging
import pytest
import yaml

from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    rdr_ui_failover_config_required,
    rdr_ui_relocate_config_required,
)
from ocs_ci.framework.testlib import tier1, skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import turquoise_squad
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.helpers.dr_helpers_ui import (
    dr_submariner_validation_from_ui,
    check_cluster_status_on_acm_console,
    failover_relocate_ui,
)
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)


@tier1
@turquoise_squad
@skipif_ocs_version("<4.14")
class TestRDRMonitoringDashboard:
    """
    Test to enable ACM observability and validate DR monitoring dashboard for RDR on the RHACM console.

    """

    @rdr_ui_failover_config_required
    @pytest.mark.polarion_id("OCS-XXXX")
    def test_rdr_monitoring_dashboard(
        self,
        setup_acm_ui,
        dr_workload,
        workload_type,
    ):
        """
        Test to enable ACM observability and validate DR monitoring dashboard for RDR on the RHACM console.

        """

        # acm_obj = AcmAddClusters()
        # workload_type = [constants.SUBSCRIPTION, constants.APPLICATION_SET]
        # for workload in workload_type:
        # workload == constants.SUBSCRIPTION:
        rdr_workload = dr_workload(num_of_subscription=2, num_of_appset=2)
        # drpc_obj = DRPC(namespace=rdr_workload.workload_namespace)

        # drpc_obj = DRPC(
        #     namespace=constants.GITOPS_CLUSTER_NAMESPACE,
        #     resource_name=f"{rdr_workload.appset_placement_name}-drpc",
        # )

        logger.info("Enable ACM MultiClusterObservability")
        run_cmd(f"oc create -f {multiclusterobservability.yaml}")

        def build_bucket_name(acm_indexes):
            """
            Create backupname from cluster names
            Args:
                acm_indexes (list): List of acm indexes
            """
            bucket_name = ""
            for index in acm_indexes:
                bucket_name += config.clusters[index].ENV_DATA["cluster_name"]
            return bucket_name

        # Configuring s3 bucket
        self.meta_obj.get_meta_access_secret_keys()

        endpoint_url = ("https://s3.amazonaws.com",)

        # bucket name formed like '{acm_active_cluster}-{acm_passive_cluster}'
        self.meta_obj.bucket_name = build_bucket_name(acm_indexes, observability)
        # create s3 bucket
        create_s3_bucket(
            self.meta_obj.access_key,
            self.meta_obj.secret_key,
            self.meta_obj.bucket_name,
        )
        # Label the hub cluster to enable VolumeSynchronizationDelayAlert
        run_cmd(
            "oc label namespace openshift-operators openshift.io/cluster-monitoring='true'"
        )

        # oc get MultiClusterObservability observability -o jsonpath='{.status.conditions[1].status}'

        def create_s3_bucket(access_key, secret_key, bucket_name):
            """
            Create s3 bucket
            Args:
                access_key (str): S3 access key
                secret_key (str): S3 secret key
                acm_indexes (list): List of acm indexes
            """
            client = boto3.resource(
                "s3",
                verify=True,
                endpoint_url="https://s3.amazonaws.com",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
            try:
                client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={
                        "LocationConstraint": constants.AWS_REGION
                    },
                )
                logger.info(f"Successfully created backup bucket: {bucket_name}")
            except BotoCoreError as e:
                logger.error(f"Failed to create s3 bucket {e}")
                raise
