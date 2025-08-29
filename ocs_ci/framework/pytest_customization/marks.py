"""
In this pytest plugin we will keep all our pytest marks used in our tests and
all related hooks/plugins to markers.
"""

import os
import sys

import pytest
from funcy import compose

from ocs_ci.ocs.exceptions import ClusterNotFoundException
from ocs_ci.framework import config
from ocs_ci.ocs.constants import (
    ORDER_BEFORE_OCS_UPGRADE,
    ORDER_BEFORE_OCP_UPGRADE,
    ORDER_BEFORE_UPGRADE,
    ORDER_OCP_UPGRADE,
    ORDER_MCO_UPGRADE,
    ORDER_DR_HUB_UPGRADE,
    ORDER_ACM_UPGRADE,
    ORDER_OCS_UPGRADE,
    ORDER_AFTER_OCP_UPGRADE,
    ORDER_AFTER_OCS_UPGRADE,
    ORDER_AFTER_UPGRADE,
    CLOUD_PLATFORMS,
    ON_PREM_PLATFORMS,
    IBM_POWER_PLATFORM,
    IBMCLOUD_PLATFORM,
    ROSA_PLATFORM,
    OPENSHIFT_DEDICATED_PLATFORM,
    MANAGED_SERVICE_PLATFORMS,
    HPCS_KMS_PROVIDER,
    HCI_PROVIDER_CLIENT_PLATFORMS,
    HCI_PC_OR_MS_PLATFORM,
    HCI_CLIENT,
    MS_CONSUMER_TYPE,
    HCI_PROVIDER,
    BAREMETAL_PLATFORMS,
    AZURE_KV_PROVIDER_NAME,
    ROSA_HCP_PLATFORM,
    VAULT_KMS_PROVIDER,
    NFS_OUTCLUSTER_TEST_PLATFORMS,
    DUTY_USE_EXISTING_HOSTED_CLUSTERS_PUSH_MISSING_CONFIG,
)
from ocs_ci.utility import version
from ocs_ci.utility.aws import update_config_from_s3
from ocs_ci.utility.utils import load_auth_config
from ocs_ci.deployment.hosted_cluster import hypershift_cluster_factory

# tier marks

tier1 = pytest.mark.tier1(value=1)
tier2 = pytest.mark.tier2(value=2)
tier3 = pytest.mark.tier3(value=3)
tier4 = pytest.mark.tier4(value=4)
tier4a = compose(tier4, pytest.mark.tier4a)
tier4b = compose(tier4, pytest.mark.tier4b)
tier4c = compose(tier4, pytest.mark.tier4c)
tier_after_upgrade = pytest.mark.tier_after_upgrade(value=5)


# build acceptance
acceptance = pytest.mark.acceptance

# team marks

e2e = pytest.mark.e2e
ecosystem = pytest.mark.ecosystem
manage = pytest.mark.manage
libtest = pytest.mark.libtest

team_marks = [manage, ecosystem, e2e]

# components  and other markers
ocp = pytest.mark.ocp
rook = pytest.mark.rook
mcg = pytest.mark.mcg
rgw = pytest.mark.rgw
csi = pytest.mark.csi
monitoring = pytest.mark.monitoring
workloads = pytest.mark.workloads
flowtests = pytest.mark.flowtests
system_test = pytest.mark.system_test
performance = pytest.mark.performance
performance_a = pytest.mark.performance_a
performance_b = pytest.mark.performance_b
performance_c = pytest.mark.performance_c
performance_extended = pytest.mark.performance_extended
scale = pytest.mark.scale
scale_long_run = pytest.mark.scale_long_run
scale_changed_layout = pytest.mark.scale_changed_layout
deployment = pytest.mark.deployment
polarion_id = pytest.mark.polarion_id
jira = pytest.mark.jira
acm_import = pytest.mark.acm_import
rdr = pytest.mark.rdr
mdr = pytest.mark.mdr
resiliency = pytest.mark.resiliency

tier_marks = [
    tier1,
    tier2,
    tier3,
    tier4,
    tier4a,
    tier4b,
    tier4c,
    tier_after_upgrade,
    performance,
    performance_a,
    performance_b,
    performance_c,
    scale,
    scale_long_run,
    scale_changed_layout,
    workloads,
    resiliency,
]

# upgrade related markers
# Requires pytest ordering plugin installed
# Use only one of those marker on one test case!
order_pre_upgrade = pytest.mark.order(ORDER_BEFORE_UPGRADE)
order_pre_ocp_upgrade = pytest.mark.order(ORDER_BEFORE_OCP_UPGRADE)
order_pre_ocs_upgrade = pytest.mark.order(ORDER_BEFORE_OCS_UPGRADE)
order_ocp_upgrade = pytest.mark.order(ORDER_OCP_UPGRADE)
order_mco_upgrade = pytest.mark.order(ORDER_MCO_UPGRADE)
order_dr_hub_upgrade = pytest.mark.order(ORDER_DR_HUB_UPGRADE)
# dr cluster operator order is same as hub operator order except that
# it's applicable only on the managed clusters
order_dr_cluster_operator_upgrade = pytest.mark.order(ORDER_DR_HUB_UPGRADE)
order_acm_upgrade = pytest.mark.order(ORDER_ACM_UPGRADE)
order_ocs_upgrade = pytest.mark.order(ORDER_OCS_UPGRADE)
order_post_upgrade = pytest.mark.order(ORDER_AFTER_UPGRADE)
order_post_ocp_upgrade = pytest.mark.order(ORDER_AFTER_OCP_UPGRADE)
order_post_ocs_upgrade = pytest.mark.order(ORDER_AFTER_OCS_UPGRADE)
ocp_upgrade = compose(order_ocp_upgrade, pytest.mark.ocp_upgrade)

# multicluster orchestrator
mco_upgrade = compose(order_mco_upgrade, pytest.mark.mco_upgrade)
# dr hub operator
dr_hub_upgrade = compose(order_dr_hub_upgrade, pytest.mark.dr_hub_upgrade)
dr_cluster_operator_upgrade = compose(
    order_dr_cluster_operator_upgrade, pytest.mark.dr_cluster_operator_upgrade
)
# acm operator
acm_upgrade = compose(order_acm_upgrade, pytest.mark.acm_upgrade)
ocs_upgrade = compose(order_ocs_upgrade, pytest.mark.ocs_upgrade)

# provider operator upgrade
provider_operator_upgrade = compose(
    order_ocs_upgrade, pytest.mark.provider_operator_upgrade
)

# pre_*_upgrade markers
pre_upgrade = compose(order_pre_upgrade, pytest.mark.pre_upgrade)
pre_ocp_upgrade = compose(
    order_pre_ocp_upgrade,
    pytest.mark.pre_ocp_upgrade,
)
pre_ocs_upgrade = compose(
    order_pre_ocs_upgrade,
    pytest.mark.pre_ocs_upgrade,
)
# post_*_upgrade markers
post_upgrade = compose(order_post_upgrade, pytest.mark.post_upgrade)
post_ocp_upgrade = compose(order_post_ocp_upgrade, pytest.mark.post_ocp_upgrade)
post_ocs_upgrade = compose(order_post_ocs_upgrade, pytest.mark.post_ocs_upgrade)

upgrade_marks = [
    ocp_upgrade,
    mco_upgrade,
    dr_hub_upgrade,
    acm_upgrade,
    ocs_upgrade,
    pre_upgrade,
    pre_ocp_upgrade,
    pre_ocs_upgrade,
    post_upgrade,
    post_ocp_upgrade,
    post_ocs_upgrade,
]

# mark the test class with marker below to ignore leftover check
ignore_leftovers = pytest.mark.ignore_leftovers

# Mark the test class with marker below to ignore leftover of resources having
# the app labels specified
ignore_leftover_label = pytest.mark.ignore_leftover_label

# ignore resource_not_found error such as when deleting a resource that was already deleted
# useful for cleanup in teardown when resource might be deleted during the test
ignore_resource_not_found_error_label = (
    pytest.mark.ignore_resource_not_found_error_label
)

# testing marker this is just for testing purpose if you want to run some test
# under development, you can mark it with @run_this and run pytest -m run_this
run_this = pytest.mark.run_this

# Skip marks
skip_inconsistent = pytest.mark.skip(
    reason="Currently the reduction is too inconsistent leading to inconsistent test results"
)

skipif_more_than_three_workers = pytest.mark.skipif(
    config.ENV_DATA["worker_replicas"] > 3,
    reason="This test cannot run on setup having more than three worker nodes",
)

# Skipif marks
skipif_aws_creds_are_missing = pytest.mark.skipif(
    (
        load_auth_config().get("AUTH", {}).get("AWS", {}).get("AWS_ACCESS_KEY_ID")
        is None
        and "AWS_ACCESS_KEY_ID" not in os.environ
        and update_config_from_s3() is None
    ),
    reason=(
        "AWS credentials weren't found in the local auth.yaml "
        "and couldn't be fetched from the cloud"
    ),
)

skipif_mcg_only = pytest.mark.skipif(
    config.ENV_DATA["mcg_only_deployment"],
    reason="This test cannot run on MCG-Only deployments",
)

mcg_only_required = pytest.mark.skipif(
    config.ENV_DATA.get("mcg_only_deployment", "") is not True,
    reason="This test runs only on MCG-only deployments",
)

skipif_fips_enabled = pytest.mark.skipif(
    config.ENV_DATA.get("fips") == "true",
    reason="This test cannot run on FIPS enabled cluster",
)

skipif_fips_enabled_on_ibm_cloud = pytest.mark.skipif(
    (
        config.ENV_DATA.get("fips") == "true"
        and config.ENV_DATA["platform"].lower() == "ibm_cloud"
    ),
    reason="This test cannot run on FIPS enabled IBM cluster",
)

fips_required = pytest.mark.skipif(
    config.ENV_DATA.get("fips") != "true",
    reason="Test runs only on FIPS enabled cluster",
)

stretchcluster_required_skipif = pytest.mark.skipif(
    config.DEPLOYMENT.get("arbiter_deployment") is False,
    reason="Test runs only on Stretch cluster with arbiter deployments",
)

stretchcluster_required = compose(
    stretchcluster_required_skipif, pytest.mark.stretchcluster_required
)

sts_deployment_required = pytest.mark.skipif(
    config.DEPLOYMENT.get("sts_enabled") is False,
    reason="Test runs only on the AWS STS enabled cluster deployments",
)

google_api_required = pytest.mark.skipif(
    not os.path.exists(os.path.expanduser(config.RUN["google_api_secret"])),
    reason="Google API credentials don't exist",
)

aws_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() != "aws",
    reason="Test runs ONLY on AWS deployed cluster",
)

aws_based_platform_required = pytest.mark.skipif(
    (
        config.ENV_DATA["platform"].lower() != "aws"
        and config.ENV_DATA["platform"].lower() != ROSA_PLATFORM
    ),
    reason="Test runs ONLY on AWS based deployed cluster",
)
azure_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() != "azure",
    reason="Test runs ONLY on Azure deployed cluster",
)

gcp_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() != "gcp",
    reason="Test runs ONLY on GCP deployed cluster",
)

cloud_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() not in CLOUD_PLATFORMS,
    reason="Test runs ONLY on cloud based deployed cluster",
)

ibmcloud_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() != IBMCLOUD_PLATFORM,
    reason="Test runs ONLY on IBM cloud",
)

on_prem_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() not in ON_PREM_PLATFORMS,
    reason="Test runs ONLY on on-prem based deployed cluster",
)
nfs_outcluster_test_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() not in NFS_OUTCLUSTER_TEST_PLATFORMS,
    reason="Test runs ONLY on the platforms part of NFS_OUTCLUSTER_TEST_PLATFORMS",
)

rh_internal_lab_required = pytest.mark.skipif(
    (
        config.ENV_DATA["platform"].lower() == "aws"
        or config.ENV_DATA["platform"].lower() == "azure"
    ),
    reason="Tests will not run in AWS or Azure Cloud",
)

vsphere_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() != "vsphere",
    reason="Test runs ONLY on VSPHERE deployed cluster",
)
rhv_platform_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() != "rhv",
    reason="Test runs ONLY on RHV deployed cluster",
)

ipi_deployment_required = pytest.mark.skipif(
    config.ENV_DATA["deployment_type"].lower() != "ipi",
    reason="Test runs ONLY on IPI deployed cluster",
)

managed_service_required = pytest.mark.skipif(
    (config.ENV_DATA["platform"].lower() not in MANAGED_SERVICE_PLATFORMS),
    reason="Test runs ONLY on OSD or ROSA cluster",
)

provider_client_platform_required = pytest.mark.skipif(
    (config.ENV_DATA["platform"].lower() not in HCI_PROVIDER_CLIENT_PLATFORMS),
    reason="Test runs ONLY on cluster with HCI provider-client platform",
)

provider_client_ms_platform_required = pytest.mark.skipif(
    (config.ENV_DATA["platform"].lower() not in HCI_PC_OR_MS_PLATFORM),
    reason="Test runs ONLY on cluster with managed service or HCI provider-client platform",
)

pc_or_ms_provider_required = pytest.mark.skipif(
    not (
        config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == "provider"
        and (
            config.default_cluster_ctx.ENV_DATA["platform"].lower()
            in HCI_PC_OR_MS_PLATFORM
        )
    ),
    reason="Test runs ONLY on managed service provider or provider of HCI provider-client cluster",
)

ms_provider_required = pytest.mark.skipif(
    not (
        config.default_cluster_ctx.ENV_DATA["platform"].lower()
        in MANAGED_SERVICE_PLATFORMS
        and config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == "provider"
    ),
    reason="Test runs ONLY on managed service provider cluster",
)

pc_or_ms_consumer_required = pytest.mark.skipif(
    not (
        config.default_cluster_ctx.ENV_DATA["cluster_type"].lower()
        in [HCI_CLIENT, MS_CONSUMER_TYPE]
        and config.default_cluster_ctx.ENV_DATA["platform"].lower()
        in HCI_PC_OR_MS_PLATFORM
    ),
    reason="Test runs ONLY on managed service provider or provider of HCI provider-client cluster",
)
ms_consumer_required = pytest.mark.skipif(
    not (
        config.default_cluster_ctx.ENV_DATA["platform"].lower()
        in MANAGED_SERVICE_PLATFORMS
        and config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == "consumer"
    ),
    reason="Test runs ONLY on managed service consumer cluster",
)

ms_provider_and_consumer_required = pytest.mark.skipif(
    not (
        config.ENV_DATA["platform"].lower() in MANAGED_SERVICE_PLATFORMS
        and config.is_provider_exist()
        and config.is_consumer_exist()
    ),
    reason="Test runs ONLY on Managed service with provider and consumer clusters",
)

hci_client_required = pytest.mark.skipif(
    not (
        config.default_cluster_ctx.ENV_DATA["platform"].lower()
        in HCI_PROVIDER_CLIENT_PLATFORMS
        and config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == HCI_CLIENT
    ),
    reason="Test runs ONLY on Fusion HCI Client cluster",
)

hci_provider_required = pytest.mark.skipif(
    not (
        config.default_cluster_ctx.ENV_DATA["platform"].lower()
        in HCI_PROVIDER_CLIENT_PLATFORMS
        and config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == HCI_PROVIDER
    ),
    reason="Test runs ONLY on Fusion HCI Provider cluster",
)
hci_provider_and_client_required = pytest.mark.skipif(
    not (
        config.ENV_DATA["platform"].lower() in HCI_PROVIDER_CLIENT_PLATFORMS
        and config.hci_provider_exist()
        and config.hci_client_exist()
    ),
    reason="Test runs ONLY on Fusion HCI provider and client clusters",
)


# when run_on_all_clients marker is used, there needs to be added cluster_index
# parameter to the test to prevent any issues with the test parametrization
def setup_multicluster_marker(marker_base, push_missing_configs=False):
    """
    Set up multicluster marker with parametrization based on client indexes.

    Args:
        marker_base: Base pytest marker to be parametrized
        push_missing_configs: Boolean flag to push missing configs

    Returns:
        Parametrized marker or original marker if setup fails
    """
    try:
        if push_missing_configs:
            # run this only if cluster type is provider and it is part of test execution stage (not deployment or
            # teardown)
            # FIXME: the usage of `sys.argv` here is not correct, but we can't use something like
            # `config.RUN["cli_params"]["deploy"]`, because this setup_multicluster_marker(...) function is called on
            # the module level (see the lines below this function definition) which means that it is actually called
            # immediately when the module is imported and the config object is not fully initialized (especially some of
            # the command line arguments are not processed)
            # the solution will be to move following logic to some fixture (similarly as we have session scope autouse
            # fixture `cluster`, which is responsible for deploying and teardown of the whole cluster (when particular
            # parameters are passed)
            test_stage = not ("--deploy" in sys.argv or "--teardown" in sys.argv)
            if (
                config.default_cluster_ctx.ENV_DATA["cluster_type"].lower()
                == HCI_PROVIDER
                and config.default_cluster_ctx.ENV_DATA["platform"].lower()
                in HCI_PROVIDER_CLIENT_PLATFORMS
            ) and test_stage:
                hypershift_cluster_factory(
                    duty=DUTY_USE_EXISTING_HOSTED_CLUSTERS_PUSH_MISSING_CONFIG,
                )
        client_indexes = [
            pytest.param(*[idx]) for idx in config.get_consumer_indexes_list()
        ]
        if len(client_indexes):
            config.multicluster = True
            return pytest.mark.parametrize(
                argnames=["cluster_index"], argvalues=client_indexes, indirect=True
            )
        return marker_base
    except ClusterNotFoundException:
        return marker_base


run_on_all_clients = setup_multicluster_marker(pytest.mark.run_on_all_clients)
run_on_all_clients_push_missing_configs = setup_multicluster_marker(
    pytest.mark.run_on_all_clients, True
)

kms_config_required = pytest.mark.skipif(
    (
        config.ENV_DATA["KMS_PROVIDER"].lower() != HPCS_KMS_PROVIDER
        and load_auth_config().get("vault", {}).get("VAULT_ADDR") is None
    )
    or (
        not (
            config.ENV_DATA["KMS_PROVIDER"].lower() == HPCS_KMS_PROVIDER
            and version.get_semantic_ocs_version_from_config() >= version.VERSION_4_10
            and load_auth_config().get("hpcs", {}).get("IBM_KP_SERVICE_INSTANCE_ID")
            is not None,
        )
    ),
    reason="KMS config not found in auth.yaml",
)

azure_kv_config_required = pytest.mark.skipif(
    config.ENV_DATA["KMS_PROVIDER"].lower() != AZURE_KV_PROVIDER_NAME,
    reason="Azure KV config required to run the test.",
)

rosa_hcp_required = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() != ROSA_HCP_PLATFORM,
    reason="Test runs ONLY on ROSA HCP cluster",
)

external_mode_required = pytest.mark.skipif(
    config.DEPLOYMENT.get("external_mode") is not True,
    reason="Test will run on External Mode cluster only",
)

skipif_aws_i3 = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == "aws"
    and config.DEPLOYMENT.get("local_storage") is True,
    reason="Test will not run on AWS i3",
)

skipif_bm = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == "baremetal"
    and config.DEPLOYMENT.get("local_storage") is True,
    reason="Test will not run on Bare Metal",
)

skipif_bmpsi = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == "baremetalpsi"
    and config.DEPLOYMENT.get("local_storage") is True,
    reason="Test will not run on Baremetal PSI",
)


skipif_managed_service = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() in MANAGED_SERVICE_PLATFORMS,
    reason="Test will not run on Managed service cluster",
)

skipif_rosa_hcp = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == ROSA_HCP_PLATFORM,
    reason="Test will not run on ROSA HCP cluster",
)

skipif_openshift_dedicated = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == OPENSHIFT_DEDICATED_PLATFORM,
    reason="Test will not run on Openshift dedicated cluster",
)

skipif_ms_provider = pytest.mark.skipif(
    config.default_cluster_ctx.ENV_DATA["platform"].lower() in MANAGED_SERVICE_PLATFORMS
    and config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == "provider",
    reason="Test will not run on Managed service provider cluster",
)

skipif_ms_consumer = pytest.mark.skipif(
    config.default_cluster_ctx.ENV_DATA["platform"].lower() in MANAGED_SERVICE_PLATFORMS
    and config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == "consumer",
    reason="Test will not run on Managed service consumer cluster",
)

skipif_ms_provider_and_consumer = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() in MANAGED_SERVICE_PLATFORMS
    and config.is_provider_exist()
    and config.is_consumer_exist(),
    reason="Test will not run on Managed service with provider and consumer clusters",
)

skipif_hci_provider = pytest.mark.skipif(
    config.default_cluster_ctx.ENV_DATA["platform"].lower()
    in HCI_PROVIDER_CLIENT_PLATFORMS
    and config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == HCI_PROVIDER,
    reason="Test will not run on Fusion HCI provider cluster",
)

skipif_hci_client = pytest.mark.skipif(
    config.default_cluster_ctx.ENV_DATA["platform"].lower()
    in HCI_PROVIDER_CLIENT_PLATFORMS
    and config.default_cluster_ctx.ENV_DATA["cluster_type"].lower() == HCI_CLIENT,
    reason="Test will not run on Fusion HCI client cluster",
)

skipif_hci_provider_and_client = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() in HCI_PROVIDER_CLIENT_PLATFORMS
    and config.hci_provider_exist()
    and config.hci_client_exist(),
    reason="Test will not run on Fusion HCI provider and Client clusters",
)

skipif_hci_provider_or_client = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() in HCI_PROVIDER_CLIENT_PLATFORMS
    or config.hci_provider_exist()
    or config.hci_client_exist(),
    reason="Test will not run on Fusion HCI provider or Client clusters",
)

# Marker for skipping tests for provider clusters based on OCS version
skip_for_provider_if_ocs_version = pytest.mark.skip_for_provider_if_ocs_version

skipif_rosa = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == ROSA_PLATFORM,
    reason="Test will not run on ROSA cluster",
)
skipif_ibm_cloud = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == IBMCLOUD_PLATFORM,
    reason="Test will not run on IBM cloud",
)

skipif_ibm_cloud_managed = pytest.mark.skipif(
    config.ENV_DATA["deployment_type"].lower() == "managed"
    and config.ENV_DATA["platform"].lower() == IBMCLOUD_PLATFORM,
    reason="Test will not run on IBM Cloud aka ROKS (managed deployment type)",
)

skipif_ibm_power = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == IBM_POWER_PLATFORM,
    reason="Test will not run on IBM Power",
)

skipif_disconnected_cluster = pytest.mark.skipif(
    config.DEPLOYMENT.get("disconnected") is True,
    reason="Test will not run on disconnected clusters",
)

skipif_stretch_cluster = pytest.mark.skipif(
    config.DEPLOYMENT.get("arbiter_deployment") is True,
    reason="Test will not run on stretch cluster",
)

skipif_proxy_cluster = pytest.mark.skipif(
    config.DEPLOYMENT.get("proxy") is True,
    reason="Test will not run on proxy clusters",
)

skipif_external_mode = pytest.mark.skipif(
    config.DEPLOYMENT.get("external_mode") is True,
    reason="Test will not run on External Mode cluster",
)

skipif_lso = pytest.mark.skipif(
    config.DEPLOYMENT.get("local_storage") is True,
    reason="Test will not run on LSO deployed cluster",
)

skipif_no_lso = pytest.mark.skipif(
    not config.DEPLOYMENT.get("local_storage"),
    reason="Test run only on LSO deployed cluster",
)

skipif_rhel_os = pytest.mark.skipif(
    (config.ENV_DATA.get("rhel_workers", None) is True)
    or (config.ENV_DATA.get("rhel_user", None) is not None),
    reason="Test will not run on cluster with RHEL OS",
)

skipif_vsphere_ipi = pytest.mark.skipif(
    (
        config.ENV_DATA["platform"].lower() == "vsphere"
        and config.ENV_DATA["deployment_type"].lower() == "ipi"
    ),
    reason="Test will not run on vSphere IPI cluster",
)

skipif_vsphere_platform = pytest.mark.skipif(
    (config.ENV_DATA["platform"].lower() == "vsphere"),
    reason="Test will not run on vSphere cluster",
)

skipif_tainted_nodes = pytest.mark.skipif(
    config.DEPLOYMENT.get("infra_nodes") is True
    or config.DEPLOYMENT.get("ocs_operator_nodes_to_taint") > 0,
    reason="Test will not run if nodes are tainted",
)

skipif_flexy_deployment = pytest.mark.skipif(
    config.ENV_DATA.get("flexy_deployment"),
    reason="This test doesn't work correctly on OCP cluster deployed via Flexy",
)

skipif_noobaa_external_pgsql = pytest.mark.skipif(
    config.ENV_DATA.get("noobaa_external_pgsql") is True,
    reason="This test will not run correctly in external DB deployed cluster.",
)

skipif_compact_mode = pytest.mark.skipif(
    config.ENV_DATA.get("worker_replicas") == 0,
    reason="This test is not supported for compact mode deployment types.",
)

metrics_for_external_mode_required = pytest.mark.skipif(
    version.get_semantic_ocs_version_from_config() < version.VERSION_4_6
    and config.DEPLOYMENT.get("external_mode") is True,
    reason="Metrics is not enabled for external mode OCS <4.6",
)

dr_hub_recovery = pytest.mark.skipif(
    config.nclusters != 4,
    reason="DR hub recovery requires 4th OCP cluster to be available for Passive hub",
)

# Filter warnings
filter_insecure_request_warning = pytest.mark.filterwarnings(
    "ignore::urllib3.exceptions.InsecureRequestWarning"
)

# collect Prometheus metrics if test fails with this mark
# specify Prometheus metric names in argument
gather_metrics_on_fail = pytest.mark.gather_metrics_on_fail

# here is the place to implement some plugins hooks which will process marks
# if some operation needs to be done for some specific marked tests.

# Marker for skipping tests based on OCP version
skipif_ocp_version = pytest.mark.skipif_ocp_version

# Marker for skipping tests based on OCS version
skipif_ocs_version = pytest.mark.skipif_ocs_version

# Marker for skipping tests based on UI
skipif_ui_not_support = pytest.mark.skipif_ui_not_support

# Marker for skipping tests if the cluster is upgraded from a particular
# OCS version
skipif_upgraded_from = pytest.mark.skipif_upgraded_from
skipif_lvm_not_installed = pytest.mark.skipif_lvm_not_installed
# Marker for skipping tests if the cluster doesn't have configured cluster-wide
# encryption with KMS properly
skipif_no_kms = pytest.mark.skipif_no_kms

skipif_ibm_flash = pytest.mark.skipif(
    config.ENV_DATA.get("ibm_flash"),
    reason="This test doesn't work correctly on IBM Flash system",
)

# Skipif intransit encryption is not set.
skipif_intransit_encryption_notset = pytest.mark.skipif(
    not config.ENV_DATA.get("in_transit_encryption"),
    reason="Skipping test due to intransit encryption is not set in config.",
)

# Skip if multus is enabled
skipif_multus_enabled = pytest.mark.skipif(
    config.ENV_DATA.get("is_multus_enabled"),
    reason="This test doesn't work correctly with multus deployments",
)

skipif_gcp_platform = pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() == "gcp",
    reason="Test will not run on GCP deployed cluster",
)

# Squad marks
aqua_squad = pytest.mark.aqua_squad
black_squad = pytest.mark.black_squad
blue_squad = pytest.mark.blue_squad
brown_squad = pytest.mark.brown_squad
green_squad = pytest.mark.green_squad
grey_squad = pytest.mark.grey_squad
magenta_squad = pytest.mark.magenta_squad
orange_squad = pytest.mark.orange_squad
purple_squad = pytest.mark.purple_squad
red_squad = pytest.mark.red_squad
turquoise_squad = pytest.mark.turquoise_squad
yellow_squad = pytest.mark.yellow_squad

# Ignore test during squad decorator check in pytest collection
ignore_owner = pytest.mark.ignore_owner

# Marks to identify tests that only serve as utility for ocs-ci
ocs_ci_utility = pytest.mark.ocs_ci_utility

# Marks to identify the cluster type in which the test case should run
runs_on_provider = pytest.mark.runs_on_provider

# Marks to identify the regression tests for provider-client cluster
provider_mode = pytest.mark.provider_mode

current_test_marks = []


def get_current_test_marks():
    """
    Get the list of the current active marks

    The current_active_marks global is updated by
    ocs_ci/tests/conftest.py::update_current_test_marks_global at the start of each test

    """
    return current_test_marks


baremetal_deployment_required = pytest.mark.skipif(
    (config.ENV_DATA["platform"].lower() not in BAREMETAL_PLATFORMS)
    or (not vsphere_platform_required),
    reason="Test required baremetal or vsphere deployment.",
)

ui_deployment_required = pytest.mark.skipif(
    not config.DEPLOYMENT.get("ui_deployment"),
    reason="UI Deployment required to run the test.",
)


# Marks to identify encryption at rest is configured.
encryption_at_rest_required = pytest.mark.skipif(
    not config.ENV_DATA.get("encryption_at_rest"),
    reason="This test requires encryption at rest to be enabled.",
)

# Mark to identify encryption is configured with KMS.
skipif_kms_deployment = pytest.mark.skipif(
    config.DEPLOYMENT.get("kms_deployment") is True,
    reason="This test is not supported for KMS deployment.",
)

# Mark the test with marker below to allow re-tries in ceph health fixture
# for known issues when waiting in re-balance and flip flop from health OK
# to 1-2 PGs waiting to be Clean
ceph_health_retry = pytest.mark.ceph_health_retry

# Mark for Multicluster upgrade scenarios
config_index = pytest.mark.config_index
multicluster_roles = pytest.mark.multicluster_roles

# Marks to identify if Vault KMS deployment is required
vault_kms_deployment_required = pytest.mark.skipif(
    not config.DEPLOYMENT.get("kms_deployment", False)
    or config.ENV_DATA.get("KMS_PROVIDER", "")
    not in [VAULT_KMS_PROVIDER, HPCS_KMS_PROVIDER],
    reason="This test requires both Vault or HPCS KMS deployment to be enabled and a valid KMS provider.",
)

ui = compose(skipif_ibm_cloud_managed, pytest.mark.ui)
