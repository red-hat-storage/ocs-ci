import types

import pytest
import logging
import random

from ocs_ci.deployment.deployment import validate_acm_hub_install, Deployment
from ocs_ci.deployment.helpers.hypershift_base import (
    get_hosted_cluster_names,
    get_random_hosted_cluster_name,
)
from ocs_ci.deployment.hub_spoke import (
    HypershiftHostedOCP,
    HypershiftAWSHostedOCP,
    HostedODF,
    HostedClients,
)
from ocs_ci.deployment import hub_spoke as hs_module

from ocs_ci.deployment.hub_spoke import deploy_hosted_ocp_clusters
from ocs_ci.framework import config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.framework.pytest_customization.marks import (
    aws_platform_required,
    hci_provider_required,
    libtest,
    purple_squad,
    runs_on_provider,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.catalog_source import get_odf_tag_from_redhat_catsrc
from ocs_ci.utility.utils import (
    get_latest_release_version,
)
from ocs_ci.utility.version import get_ocs_version_from_csv
from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_client import StorageClient
from ocs_ci.helpers.helpers import (
    get_all_storageclass_names,
    verify_block_pool_exists,
)
from ocs_ci.ocs.rados_utils import (
    verify_cephblockpool_status,
    check_phase_of_rados_namespace,
)

logger = logging.getLogger(__name__)


def _ga_resolver_factory(ga_map=None, raise_for=None):
    """
    Returns a function suitable for monkeypatching get_ocp_ga_version.
    - ga_map: dict of 'x.y' -> 'x.y.z'
    - raise_for: set of 'x.y' that should raise Exception
    """
    ga_map = ga_map or {}
    raise_for = set(raise_for or [])

    def _resolver(xy):
        if xy in raise_for:
            raise RuntimeError(f"GA not found for {xy}")
        # default behavior: append '.9' if not specified
        return ga_map.get(xy, f"{xy}.9")

    return _resolver


@libtest
@purple_squad
class TestProviderHosted(object):
    """
    Test provider hosted
    """

    @hci_provider_required
    def test_provider_deploy_OCP_hosted(self):
        """
        Test deploy hosted OCP
        """

        logger.info("Test deploy hosted OCP on provider platform")
        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]

        hypershift_hosted = HypershiftHostedOCP(cluster_name)
        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=True,
            deploy_cnv=True,
            deploy_metallb=True,
            download_hcp_binary=True,
            deploy_hyperconverged=False,
            deploy_mce=False,
        )
        hypershift_hosted.deploy_ocp()

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_skip_cnv_and_lb(self):
        """
        Test deploy hosted OCP on provider platform with cnv and metallb ready beforehand
        """
        logger.info(
            "Test deploy hosted OCP on provider platform with metallb and cnv ready"
        )
        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]
        hypershift_hosted = HypershiftHostedOCP(cluster_name)
        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=True,
            deploy_cnv=False,
            deploy_metallb=False,
            download_hcp_binary=True,
            deploy_hyperconverged=False,
            deploy_mce=False,
        )
        hypershift_hosted.deploy_ocp()

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_skip_cnv(self):
        """
        Test deploy hosted OCP on provider platform with cnv ready beforehand
        """
        logger.info("Test deploy hosted OCP on provider platform with cnv ready")
        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]

        hypershift_hosted = HypershiftHostedOCP(cluster_name)
        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=True,
            deploy_cnv=False,
            deploy_metallb=True,
            download_hcp_binary=True,
            deploy_hyperconverged=False,
            deploy_mce=False,
        )
        HypershiftHostedOCP(cluster_name).deploy_ocp()

    @hci_provider_required
    def test_provider_deploy_OCP_hosted_multiple(self):
        """
        Test deploy hosted OCP on provider platform multiple times
        """
        logger.info("Test deploy hosted OCP on provider platform multiple times")
        deploy_hosted_ocp_clusters()

    @runs_on_provider
    @hci_provider_required
    def test_install_odf_on_hosted_cluster(self):
        """
        Test install ODF on hosted cluster
        """
        logger.info("Test install ODF on hosted cluster")

        HostedClients().download_hosted_clusters_kubeconfig_files()

        hosted_cluster_names = get_hosted_cluster_names()
        cluster_name = random.choice(hosted_cluster_names)

        hosted_odf = HostedODF(cluster_name)
        hosted_odf.do_deploy()

    @runs_on_provider
    @hci_provider_required
    def test_deploy_OCP_and_setup_ODF_client_on_hosted_clusters(self):
        """
        Test install ODF on hosted cluster
        """
        logger.info("Deploy hosted OCP on provider platform multiple times")

        HostedClients().do_deploy()

    @runs_on_provider
    @hci_provider_required
    def test_create_onboarding_key(self):
        """
        Test create onboarding key
        """
        logger.info("Test create onboarding key")
        HostedClients().download_hosted_clusters_kubeconfig_files()

        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]
        assert len(
            HostedODF(cluster_name).get_onboarding_key()
        ), "Failed to get onboarding key"

    @runs_on_provider
    @hci_provider_required
    def test_storage_client_connected(self):
        """
        Test storage client connected
        """
        logger.info("Test storage client connected")
        HostedClients().download_hosted_clusters_kubeconfig_files()

        cluster_names = list(config.ENV_DATA["clusters"].keys())
        assert HostedODF(cluster_names[-1]).get_storage_client_status() == "Connected"

    @runs_on_provider
    @hci_provider_required
    def test_create_hosted_cluster_with_fixture(
        self, create_hypershift_clusters_push_config, destroy_hosted_cluster
    ):
        """
        Test create hosted cluster with fixture
        """
        log_step("Create hosted client")
        cluster_name = get_random_hosted_cluster_name()
        odf_version = str(get_ocs_version_from_csv()).replace(".stable", "")
        if "rhodf" in odf_version:
            odf_version = get_odf_tag_from_redhat_catsrc()
        ocp_version = get_latest_release_version()
        nodepool_replicas = 2

        create_hypershift_clusters_push_config(
            cluster_names=[cluster_name],
            ocp_version=ocp_version,
            odf_version=odf_version,
            setup_storage_client=True,
            nodepool_replicas=nodepool_replicas,
        )

        log_step("Switch to the hosted cluster")
        ocsci_config.switch_to_cluster_by_name(cluster_name)

        server = str(OCP().exec_oc_cmd("whoami --show-server", out_yaml_format=False))

        assert (
            cluster_name in server
        ), f"Failed to switch to cluster '{cluster_name}' and fetch data"

    @runs_on_provider
    @hci_provider_required
    def test_create_destroy_hosted_cluster_with_fixture(
        self, create_hypershift_clusters_push_config, destroy_hosted_cluster
    ):
        """
        Test create hosted cluster with fixture and destroy cluster abruptly
        Important that ceph resources associate with the cluster will not be cleaned up
        """
        log_step("Create hosted client")
        cluster_name = get_random_hosted_cluster_name()
        odf_version = str(get_ocs_version_from_csv()).replace(".stable", "")
        if "rhodf" in odf_version:
            odf_version = get_odf_tag_from_redhat_catsrc()

        ocp_version = get_latest_release_version()
        nodepool_replicas = 2

        create_hypershift_clusters_push_config(
            cluster_names=[cluster_name],
            ocp_version=ocp_version,
            odf_version=odf_version,
            setup_storage_client=True,
            nodepool_replicas=nodepool_replicas,
        )

        log_step("Switch to the hosted cluster")
        ocsci_config.switch_to_cluster_by_name(cluster_name)

        server = str(OCP().exec_oc_cmd("whoami --show-server", out_yaml_format=False))

        assert (
            cluster_name in server
        ), f"Failed to switch to cluster '{cluster_name}' and fetch data"

        log_step("Destroy hosted cluster")
        assert destroy_hosted_cluster(cluster_name), "Failed to destroy hosted cluster"

    @runs_on_provider
    @hci_provider_required
    def test_deploy_acm(self):
        """
        Test deploy dependencies
        """
        logger.info("Test deploy dependencies ACM")
        HypershiftHostedOCP("dummy").deploy_dependencies(
            deploy_acm_hub=True,
            deploy_cnv=False,
            deploy_metallb=False,
            download_hcp_binary=False,
            deploy_hyperconverged=False,
            deploy_mce=False,
        )
        assert validate_acm_hub_install(), "ACM not installed or MCE not configured"

    @runs_on_provider
    @hci_provider_required
    def test_deploy_cnv(self):
        """
        Test deploy dependencies
        """
        logger.info("Test deploy dependencies CNV")
        hypershift_hosted = HypershiftHostedOCP("dummy")
        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=False,
            deploy_cnv=True,
            deploy_metallb=False,
            download_hcp_binary=False,
            deploy_hyperconverged=False,
            deploy_mce=False,
        )
        assert hypershift_hosted.cnv_hyperconverged_installed(), "CNV not installed"

    @runs_on_provider
    @hci_provider_required
    def test_deploy_metallb(self):
        """
        Test deploy dependencies
        """
        logger.info("Test deploy dependencies Metallb")
        hypershift_hosted = HypershiftHostedOCP("dummy")
        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=False,
            deploy_cnv=False,
            deploy_metallb=True,
            download_hcp_binary=False,
            deploy_hyperconverged=False,
            deploy_mce=False,
        )
        assert hypershift_hosted.metallb_instance_created(), "Metallb not installed"

    @runs_on_provider
    @hci_provider_required
    def test_download_hcp(self):
        """
        Test deploy dependencies
        """
        logger.info("Test deploy dependencies HCP binary")
        hypershift_hosted = HypershiftHostedOCP("dummy")
        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=False,
            deploy_cnv=False,
            deploy_metallb=False,
            download_hcp_binary=True,
            deploy_hyperconverged=False,
            deploy_mce=False,
        )
        assert hypershift_hosted.hcp_binary_exists(), "HCP binary not downloaded"

    @runs_on_provider
    def test_mch_status_running(self):
        """
        Get MCH status
        """
        logger.info("Get MCH status")
        depl = Deployment()
        assert depl.muliclusterhub_running(), "MCH not running"

    @runs_on_provider
    def test_verify_native_storage(self):
        """
        Verify native storage client
        """
        logger.info("Verify native storage client")
        storage_client = StorageClient()
        storage_client.verify_native_storageclient()
        assert verify_block_pool_exists(
            constants.DEFAULT_BLOCKPOOL
        ), f"{constants.DEFAULT_BLOCKPOOL} is not created"
        assert verify_cephblockpool_status(), "the cephblockpool is not in Ready phase"

        # Validate radosnamespace created and in 'Ready' status
        assert (
            check_phase_of_rados_namespace()
        ), "The radosnamespace is not in Ready phase"

        # Validate storageclasses created
        storage_class_classes = get_all_storageclass_names()
        storage_class_claims = [
            constants.CEPHBLOCKPOOL_SC,
            constants.CEPHFILESYSTEM_SC,
        ]
        for storage_class in storage_class_claims:
            assert (
                storage_class in storage_class_classes
            ), "Storage classes ae not created as expected"

    def test_deploy_mce(self):
        """
        Test deploy mce without installting acm
        """
        logger.info("Test deploy mce without deploying ACM")
        hypershift_hosted = HypershiftHostedOCP("dummy")
        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=False,
            deploy_cnv=False,
            deploy_metallb=False,
            download_hcp_binary=False,
            deploy_mce=True,
            deploy_hyperconverged=False,
        )

    @hci_provider_required
    def test_provider_deploy_ocp_hosted_skip_acm(self):
        """
        Test deploy hosted OCP on provider platform with cnv ready beforehand
        ! Suitable for released version of OCP only (no mce and hyperconverged)
        """
        logger.info("Test deploy hosted OCP on provider platform with cnv ready")
        cluster_name = list(config.ENV_DATA["clusters"].keys())[-1]
        hypershift_hosted = HypershiftHostedOCP(cluster_name)

        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=False,
            deploy_cnv=True,
            deploy_metallb=True,
            download_hcp_binary=True,
            deploy_mce=False,
            deploy_hyperconverged=False,
        )

        HypershiftHostedOCP(cluster_name).deploy_ocp()

    def test_deploy_hyperconverged(self):
        """
        Test deploy hyperconverged operator
        ! Hyperconverged is used instead of unreleased CNV, to overcome catalogsource limitations on client clusters
        """
        logger.info("Test deploy hyperconverged operator")
        hypershift_hosted = HypershiftHostedOCP("dummy")
        hypershift_hosted.deploy_dependencies(
            deploy_acm_hub=False,
            deploy_cnv=False,
            deploy_metallb=False,
            download_hcp_binary=False,
            deploy_mce=False,
            deploy_hyperconverged=True,
        )

    @runs_on_provider
    def test_acm_upgrade(self):
        """
        Verify acm upgrade
        """
        logger.info("Verify acm upgrade")
        from ocs_ci.ocs.acm_upgrade import ACMUpgrade

        acm_hub_upgrade_obj = ACMUpgrade()
        acm_hub_upgrade_obj.run_upgrade()

    @runs_on_provider
    def test_cnv_upgrade(self):
        """
        Verify cnv upgrade
        """
        logger.info("Verify cnv upgrade")
        from ocs_ci.deployment.cnv import CNVInstaller

        cnv_installer_obj = CNVInstaller()
        assert cnv_installer_obj.upgrade_cnv(), "CNV operator upgrade not successful"

    @runs_on_provider
    def test_metallb_upgrade(self):
        """
        Verify metallb upgrade
        """
        logger.info("Verify metallb upgrade")
        from ocs_ci.deployment.metallb import MetalLBInstaller

        metallb_installer_obj = MetalLBInstaller()
        assert (
            metallb_installer_obj.upgrade_metallb()
        ), "Metallb operator upgrade not successful"

    @pytest.mark.parametrize(
        "case",
        [
            # 1: No configured version, provider GA -> QUAY:provider-x86_64
            dict(
                name="no_config_ga_provider",
                cfg_ocp=None,
                provider="4.19.9",
                hosted="4.18.8",
                ga_map=None,
                raise_for=None,
                expected=lambda: f"{constants.QUAY_REGISTRY_SVC}:4.19.9-x86_64",
            ),
            # 2: No configured version, provider nightly -> REG:provider
            dict(
                name="no_config_nightly_provider",
                cfg_ocp=None,
                provider="4.19.0-0.nightly-2024-09-10-123456",
                hosted="4.18.9",
                ga_map=None,
                raise_for=None,
                expected=lambda: f"{constants.REGISTRY_SVC}:4.19.0-0.nightly-2024-09-10-123456",
            ),
            # 3: x.y resolves to GA, < provider, hosted < desired -> use desired GA
            dict(
                name="xy_lower_than_provider_use_desired",
                cfg_ocp="4.18",
                provider="4.19.3",
                hosted="4.17.9",
                ga_map={"4.18": "4.18.9"},
                raise_for=None,
                expected=lambda: f"{constants.QUAY_REGISTRY_SVC}:4.18.9-x86_64",
            ),
            # 4: x.y resolves to GA equal to provider -> None
            dict(
                name="xy_equal_provider_return_none",
                cfg_ocp="4.18",
                provider="4.18.9",
                hosted="4.18.0",
                ga_map={"4.18": "4.18.9"},
                raise_for=None,
                expected=lambda: None,
            ),
            # 5: desired xyz but hosted >= desired -> fallback to provider GA
            dict(
                name="xyz_hosted_greater_fallback_provider",
                cfg_ocp="4.18.9",
                provider="4.19.3",
                hosted="4.18.10",
                ga_map=None,
                raise_for=None,
                expected=lambda: f"{constants.QUAY_REGISTRY_SVC}:4.19.3-x86_64",
            ),
            # 6: desired xyz and hosted < desired -> use desired
            dict(
                name="xyz_hosted_lower_use_desired",
                cfg_ocp="4.18.9",
                provider="4.19.3",
                hosted="4.18.8",
                ga_map=None,
                raise_for=None,
                expected=lambda: f"{constants.QUAY_REGISTRY_SVC}:4.18.9-x86_64",
            ),
            # 7: GA resolver raises and desired (x.y) > provider -> None
            dict(
                name="ga_resolve_fails_desired_gt_provider_none",
                cfg_ocp="4.99",
                provider="4.19.3",
                hosted="4.19.2",
                ga_map=None,
                raise_for={"4.99"},
                expected=lambda: None,
            ),
            # 8: empty string -> use provider GA
            dict(
                name="empty_string_use_provider",
                cfg_ocp="",
                provider="4.19.3",
                hosted="4.18.9",
                ga_map=None,
                raise_for=None,
                expected=lambda: f"{constants.QUAY_REGISTRY_SVC}:4.19.3-x86_64",
            ),
            # 9: whitespace -> use provider GA
            dict(
                name="whitespace_use_provider",
                cfg_ocp="   ",
                provider="4.19.3",
                hosted="4.18.9",
                ga_map=None,
                raise_for=None,
                expected=lambda: f"{constants.QUAY_REGISTRY_SVC}:4.19.3-x86_64",
            ),
            # 10: None again -> use provider GA
            dict(
                name="none_use_provider_again",
                cfg_ocp=None,
                provider="4.19.3",
                hosted="4.18.9",
                ga_map=None,
                raise_for=None,
                expected=lambda: f"{constants.QUAY_REGISTRY_SVC}:4.19.3-x86_64",
            ),
        ],
    )
    def test_compute_target_release_image(self, monkeypatch, case):
        """
        Unit test for HypershiftHostedOCP._compute_target_release_image.

        It:
        - patches provider version (get_server_version)
        - patches GA resolver (get_ocp_ga_version)
        - injects the hosted cluster version by replacing:
          hypershift_hosted_ocp.get_hosted_cluster_ocp_version = lambda: "<ver>"
        - sets config.ENV_DATA["clusters"][cluster]["ocp_version"]
        """
        cluster = "cl-418-a"

        # Prepare ENV_DATA
        # Keep global config intact; only set the nested key needed for the test
        clusters = ocsci_config.ENV_DATA.setdefault("clusters", {})
        cluster_cfg = clusters.setdefault(cluster, {})
        cluster_cfg.setdefault("cluster_type", "hci_client")
        monkeypatch.setitem(cluster_cfg, "ocp_version", case["cfg_ocp"])

        # Patch provider version
        monkeypatch.setattr(hs_module, "get_server_version", lambda: case["provider"])

        # Patch provider and GA resolver
        monkeypatch.setattr(hs_module, "get_server_version", lambda: case["provider"])
        ga_resolver = _ga_resolver_factory(
            ga_map=case["ga_map"], raise_for=case["raise_for"]
        )
        monkeypatch.setattr(hs_module, "get_ocp_ga_version", ga_resolver)

        self_stub = types.SimpleNamespace()
        self_stub.name = cluster
        self_stub.get_hosted_cluster_ocp_version = lambda: case["hosted"]

        result = hs_module.HypershiftHostedOCP.compute_target_release_image(self_stub)
        assert result == case["expected"]()

    @runs_on_provider
    def test_mce_upgrade(self):
        """
        Verify mce upgrade
        """
        logger.info("Verify mce upgrade")
        from ocs_ci.deployment.mce import MCEInstaller

        mce_installer_obj = MCEInstaller()
        assert mce_installer_obj.upgrade_mce(), "MCE operator upgrade not successful"


def _get_aws_hcp_cluster_names():
    """
    Get AWS HCP cluster names from configuration.

    Returns:
        list: List of AWS HCP cluster names
    """
    clusters = config.ENV_DATA.get("clusters", {})
    return [
        name
        for name, cfg in clusters.items()
        if cfg.get("hosted_cluster_platform") == "aws"
    ]


def _get_aws_hcp_instance(cluster_name=None):
    """
    Get HypershiftAWSHostedOCP instance for testing.

    Args:
        cluster_name (str): Optional cluster name. If not provided, uses first AWS HCP cluster.

    Returns:
        HypershiftAWSHostedOCP: Instance for AWS operations or None if not available
    """
    if not cluster_name:
        aws_hcp_clusters = _get_aws_hcp_cluster_names()
        if not aws_hcp_clusters:
            return None
        cluster_name = aws_hcp_clusters[0]

    return HypershiftAWSHostedOCP(cluster_name)


@libtest
@purple_squad
class TestAWSHCPNetworkRouting:
    """
    Test class for AWS HCP VPC peering and network routing functionality.

    These tests verify the network routing setup between AWS HCP client clusters
    and management clusters. Tests check if VPC peering is already established
    and either verify the existing setup or establish new routing.
    """

    @aws_platform_required
    def test_network_setup_exists_or_establish(self):
        """
        Test that network setup (VPC peering, routing, security groups) exists
        between client and management clusters.

        If network is not established and clusters are available, set it up.
        If clusters are not available, skip the test.
        """
        aws_hcp_clusters = _get_aws_hcp_cluster_names()
        if not aws_hcp_clusters:
            pytest.skip("No AWS HCP clusters configured")

        # Get the management cluster name (provider cluster)
        mgmt_cluster_name = config.ENV_DATA.get("cluster_name")
        if not mgmt_cluster_name:
            pytest.skip("Management cluster name not configured")

        aws_hcp = _get_aws_hcp_instance()
        client_cluster_name = aws_hcp_clusters[0]

        try:
            client_vpc_id = aws_hcp.get_vpc_id_for_cluster(client_cluster_name)
            mgmt_vpc_id = aws_hcp.get_mgmt_vpc_id()
        except ValueError as e:
            pytest.skip(f"VPCs not found for clusters: {e}")

        # Check if peering already exists and is active
        existing_peerings = aws_hcp.ec2_client.describe_vpc_peering_connections(
            Filters=[
                {"Name": "requester-vpc-info.vpc-id", "Values": [client_vpc_id]},
                {"Name": "accepter-vpc-info.vpc-id", "Values": [mgmt_vpc_id]},
                {"Name": "status-code", "Values": ["active"]},
            ]
        )

        if existing_peerings.get("VpcPeeringConnections"):
            pcx_id = existing_peerings["VpcPeeringConnections"][0][
                "VpcPeeringConnectionId"
            ]
            logger.info(f"VPC peering already active: {pcx_id}")
            # Verify security groups have Ceph ports
            # This is informational - the main setup is already done
            return

        # Network not established - set it up using setup_network_for_client_cluster
        logger.info(
            "Network not established, setting up VPC peering, routing, and security groups"
        )

        # Find a running instance in management VPC to get security group
        instances = aws_hcp.ec2_client.describe_instances(
            Filters=[
                {"Name": "vpc-id", "Values": [mgmt_vpc_id]},
                {"Name": "instance-state-name", "Values": ["running"]},
            ]
        )

        if not instances.get("Reservations"):
            pytest.skip(f"No running instances found in management VPC {mgmt_vpc_id}")

        mgmt_instance_id = instances["Reservations"][0]["Instances"][0]["InstanceId"]

        # Setup complete network
        result = aws_hcp.setup_network_for_client_cluster(
            client_cluster_name=client_cluster_name,
            mgmt_cluster_name=mgmt_cluster_name,
            mgmt_instance_id=mgmt_instance_id,
            nodeport=constants.NODEPORT,
        )

        logger.info(
            f"Network setup completed:\n"
            f"  VPC Peering: {result['pcx_id']}\n"
            f"  Security Group: {result['mgmt_sg_id']}\n"
            f"  Client CIDR allowed: {result['client_vpc_cidr']}"
        )

    @aws_platform_required
    def test_security_group_ceph_ports(self):
        """
        Test that Ceph ports are open in security groups for AWS HCP clusters.

        Checks if required Ceph ports are already configured. If not and
        management instance is available, adds the rules.
        """
        aws_hcp_clusters = _get_aws_hcp_cluster_names()
        if not aws_hcp_clusters:
            pytest.skip("No AWS HCP clusters configured")

        aws_hcp = _get_aws_hcp_instance()
        if not aws_hcp:
            pytest.skip("Could not create AWS HCP instance")

        client_cluster_name = aws_hcp_clusters[0]

        # Get client VPC CIDR
        try:
            client_vpc_id = aws_hcp.get_vpc_id_for_cluster(client_cluster_name)
            client_vpc_cidr = aws_hcp.get_vpc_cidr_by_vpc_id(client_vpc_id)
        except ValueError as e:
            pytest.skip(f"Could not get client VPC info: {e}")

        # Get management cluster info
        mgmt_cluster_name = config.ENV_DATA.get("cluster_name")
        if not mgmt_cluster_name:
            pytest.skip("Management cluster name not configured")

        try:
            mgmt_vpc_id = aws_hcp.get_mgmt_vpc_id()
        except ValueError as e:
            pytest.skip(f"Could not get management VPC: {e}")

        # Find security groups in management VPC
        sgs = aws_hcp.ec2_client.describe_security_groups(
            Filters=[{"Name": "vpc-id", "Values": [mgmt_vpc_id]}]
        )

        if not sgs.get("SecurityGroups"):
            pytest.skip("No security groups found in management VPC")

        # Check first security group for Ceph ports
        sg_id = sgs["SecurityGroups"][0]["GroupId"]
        ip_permissions = sgs["SecurityGroups"][0].get("IpPermissions", [])

        ceph_ports = {
            constants.CEPH_MON_MSGR2_PORT,
            constants.CEPH_MON_LEGACY_PORT,
            constants.CEPH_EXPORTER_PORT,
        }
        existing_ports = set()
        for perm in ip_permissions:
            from_port = perm.get("FromPort")
            to_port = perm.get("ToPort")
            if from_port and to_port and from_port == to_port:
                existing_ports.add(from_port)

        missing_ports = ceph_ports - existing_ports
        if missing_ports:
            logger.info(f"Missing Ceph ports: {missing_ports}, adding them")
            aws_hcp.add_ceph_ports_to_security_group(
                security_group_id=sg_id,
                source_cidr=client_vpc_cidr,
            )
            logger.info("Ceph ports added to security group")
        else:
            logger.info("All Ceph ports already configured in security group")

    @aws_platform_required
    def test_network_connectivity_to_management(self):
        """
        Test network connectivity from client cluster to management cluster.

        Uses oc debug to ping the management cluster node from client cluster.
        Skips if clusters are not deployed or kubeconfig is not available.
        """
        aws_hcp_clusters = _get_aws_hcp_cluster_names()
        if not aws_hcp_clusters:
            pytest.skip("No AWS HCP clusters configured")

        aws_hcp = _get_aws_hcp_instance()
        if not aws_hcp:
            pytest.skip("Could not create AWS HCP instance")

        # Check if cluster kubeconfig exists
        if not aws_hcp.cluster_kubeconfig:
            pytest.skip("Cluster kubeconfig not available")

        import os

        if not os.path.exists(aws_hcp.cluster_kubeconfig):
            pytest.skip(f"Kubeconfig not found: {aws_hcp.cluster_kubeconfig}")

        # Get management cluster node IP
        mgmt_cluster_name = config.ENV_DATA.get("cluster_name")
        if not mgmt_cluster_name:
            pytest.skip("Management cluster name not configured")

        # Get management node private IP
        try:
            mgmt_node_ip = aws_hcp.get_node_private_ip()
        except (ValueError, CommandFailed) as e:
            pytest.skip(f"Could not get management node IP: {e}")

        try:
            result = aws_hcp.verify_network_connectivity(
                target_ip=mgmt_node_ip,
                timeout=10,
            )
            assert result, f"Network connectivity to {mgmt_node_ip} failed"
            logger.info(
                f"Network connectivity to management cluster ({mgmt_node_ip}) verified"
            )
        except CommandFailed as e:
            pytest.fail(f"Network connectivity test failed: {e}")

    @aws_platform_required
    def test_setup_complete_network_and_verify(self):
        """
        Set up complete network connectivity between AWS HCP client and
        management clusters, then verify it works.

        Skips if prerequisites are not met (no clusters, no VPCs, no kubeconfig).
        """
        aws_hcp_clusters = _get_aws_hcp_cluster_names()
        if not aws_hcp_clusters:
            pytest.skip("No AWS HCP clusters configured")

        client_cluster_name = aws_hcp_clusters[0]
        aws_hcp = _get_aws_hcp_instance(client_cluster_name)
        if not aws_hcp:
            pytest.skip("Could not create AWS HCP instance")

        aws_hcp.setup_and_verify_network(nodeport=constants.CEPH_NODE_PORT)
