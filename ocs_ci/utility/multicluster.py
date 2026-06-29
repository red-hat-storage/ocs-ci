"""
All multicluster specific utility functions and classes can be here

"""

import logging
import tempfile

from ocs_ci.framework import config as ocsci_config, config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.utils import (
    get_non_acm_cluster_indexes,
    get_passive_acm_index,
    get_primary_cluster_index,
    get_active_acm_index,
    get_all_acm_indexes,
)
from ocs_ci.ocs.constants import (
    MDR_ROLES,
    RDR_ROLES,
    RDR_PROVIDER_ROLES,
    ACM_RANK,
    MANAGED_CLUSTER_RANK,
    HOSTED_CLIENT_RANK,
)
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    run_cmd,
    wait_for_machineconfigpool_status,
    get_acm_mce_build_tag,
)

log = logging.getLogger(__name__)


class MultiClusterUpgradeParametrize(object):
    """
    This base class abstracts upgrade parametrization for multicluster scenarios: MDR, RDR and Managed service

    """

    MULTICLUSTER_UPGRADE_MARKERS = [
        "pre_upgrade",
        "pre_ocp_upgrade",
        "ocp_upgrade",
        "post_ocp_upgrade",
        "mco_upgrade",
        "mce_upgrade",
        "kubevirt_cluster_upgrade",
        "dr_hub_upgrade",
        "dr_cluster_operator_upgrade",
        "acm_upgrade",
        "pre_ocs_upgrade",
        "ocs_upgrade",
        "post_ocs_upgrade",
        "post_upgrade",
    ]

    def __init__(self):
        self.roles = []
        # List of zones which are participating in this multicluster setup
        self.zones = self.get_zone_info()
        self.zones.sort()
        self.zone_base_rank = 100
        # Each zone will be assigned with a rank
        # This rank comes handy when we have to order the tests
        self.roles_to_param_tuples = dict()
        self.roles_to_config_index_map = dict()
        self.zone_role_map = dict()
        self.zone_ranks = {}

    def generate_zone_ranks(self):
        """
        For each zone we would be generating the ranks, we will add the zone's respective indexes
        to the base rank values which keeps the zone ranks apart and create spaces (for ranks)
        in between to accomodate other tests

        """
        for i in range(len(self.zones)):
            self.zone_ranks[f"{self.zones[i]}"] = (
                self.zone_base_rank + i * self.zone_base_rank
            )
        log.info(f"zone ranks = {self.zone_ranks}")

    def generate_config_index_map(self):
        """
        Generate config indexes for all the MDRs/RDR cluster roles
        ex: {"ActiveACM": 0, "PassiveACM": 2, "PrimaryODF": 1, "SecondaryODF": 3}

        """
        active_acm_index = get_active_acm_index()
        primary_index = get_primary_cluster_index()

        for cluster in ocsci_config.clusters:
            cluster_index = cluster.MULTICLUSTER["multicluster_index"]
            if cluster_index == active_acm_index:
                self.roles_to_config_index_map["ActiveACM"] = cluster_index
            elif cluster_index == primary_index:
                self.roles_to_config_index_map["PrimaryODF"] = cluster_index
            elif cluster_index == get_passive_acm_index():
                # We would have already ruled out the ActiveACM in the first 'if'
                self.roles_to_config_index_map["PassiveACM"] = cluster_index
            else:
                # Only option left is secondary odf
                self.roles_to_config_index_map["SecondaryODF"] = cluster_index

    def generate_role_ranks(self):
        """
        Based on current roles for MDR/RDR : ActiveACM:1, PassiveACM:1, PrimaryODF:2, SecondaryODF: 2
        In case of RDR few runs might consider only one ACM where as in MDR we might have 2 ACMs.
        We will be adjusting the roles for RDR dynamically in the child class based on what type of run (
        1 ACM OR 2 ACMs)the user triggers

        """
        # For now we will stick to this convention
        self.role_ranks = {
            "ActiveACM": ACM_RANK,
            "PassiveACM": ACM_RANK,
            "PrimaryODF": MANAGED_CLUSTER_RANK,
            "SecondaryODF": MANAGED_CLUSTER_RANK,
            "HostedClient": HOSTED_CLIENT_RANK,
        }

    def get_zone_info(self):
        """
        Get the list of participating zones

        """
        zones = set()
        for c in ocsci_config.clusters:
            zones.add(c.ENV_DATA["zone"])
        return list(zones)

    def config_init(self):
        self.generate_zone_ranks()
        self.generate_role_ranks()
        self.generate_config_index_map()
        # Reverse mapping of cluster's index to its role
        self.index_to_role = {
            index: role for role, index in self.roles_to_config_index_map.items()
        }
        self.generate_zone_role_map()
        self.generate_role_to_param_tuple_map()

    def generate_zone_role_map(self):
        """
        Generate a map of Cluster's role vs zone in which clusters are located
        ex: {"ActiveACM": 'a', "PassiveACM": 'b', "PrimaryODF": 'a'}
        """
        for crole, cindex in self.roles_to_config_index_map.items():
            czone = ocsci_config.clusters[cindex].ENV_DATA.get("zone")
            if czone:
                self.zone_role_map[crole] = czone

    def generate_role_to_param_tuple_map(self):
        """
        For each of the MDRs applicable roles store a tuple (zone_rank, role_rank, config_index)
        ex: {"ActiveACM": (1, 1, 0), "PassiveACM": (2, 1, 2), "PrimaryODF": (1, 2, 1), "SecondarODF": (2, 2, 3)}

        """
        for role in self.all_roles:
            self.roles_to_param_tuples[role] = (
                self.zone_ranks[self.zone_role_map[role]],
                self.role_ranks[role],
                self.roles_to_config_index_map[role],
            )

    def get_pytest_params_tuple(self, role):
        """
        Get a tuple of parameters applicable to the given role
        For ex: if role is 'ActiveACM', then get a tuple which is applicable to
        that role. If the role is 'all' then we will get tuples of parameter
        for all the roles applicable
        Parmeter tuples looks like (zone_rank, role_rank, config_index) for a given role

        """
        param_list = None
        if role.startswith(f"{self.dr_type}-all"):
            param_list = self.get_dr_all_param_tuples(role)
        else:
            param_list = [self.roles_to_param_tuples[role]]
        return param_list

    def get_dr_all_param_tuples(self, role):
        if f"{self.dr_type}-all-ocp" in role:
            return self.get_all_roles_to_param_tuples()
        elif f"{self.dr_type}-all-odf" in role:
            return self.get_all_odf_roles_to_param_tuple()
        elif f"{self.dr_type}-all-acm" in role:
            return self.get_all_acm_roles_to_param_tuple()

    def get_all_acm_roles_to_param_tuple(self):
        params_list = list()
        for i in get_all_acm_indexes():
            params_list.append(self.roles_to_param_tuples[self.index_to_role[i]])
        return params_list

    def get_all_odf_roles_to_param_tuple(self):
        params_list = list()
        for i in get_non_acm_cluster_indexes():
            params_list.append(self.roles_to_param_tuples[self.index_to_role[i]])
        return params_list

    def get_all_roles_to_param_tuples(self):
        param_list = list()
        for t in self.roles_to_param_tuples.values():
            param_list.append(t)
        return param_list

    def get_roles(self, metafunc):
        # Return a list of roles applicable to the current test
        for marker in metafunc.definition.iter_markers():
            if marker.name == "multicluster_roles":
                return marker.args[0]

    def generate_pytest_parameters(self, metafunc, roles):
        """
        We will have to parametrize the test based on the MDR roles to which the test is applicable to,
        Parameters will be a tuple of (zone_rank, role_rank, config_index)

        """
        pytest_params = []
        for role in roles:
            # A role marker may have mdr, rdr etc markers, we need to pick
            # the role marker only if its applicable to the current DR scenario
            if role.startswith(f"{self.dr_type}"):
                pytest_params.extend(self.get_pytest_params_tuple(role))
        return pytest_params


class MDRClusterUpgradeParametrize(MultiClusterUpgradeParametrize):
    """
    This child class handles MDR upgrade scenario specific pytest parametrization

    """

    def __init__(self):
        self.dr_type = "mdr"
        super().__init__()
        self.all_roles = MDR_ROLES

    def config_init(self):
        super().config_init()


class RDRClusterUpgradeParametrize(MultiClusterUpgradeParametrize):
    """
    This child class handles RDR upgrade scenario specific pytest parametrization

    """

    # RDR upgrade specific order according to the doc
    UPGRADE_TEST_ORDER = {
        constants.ORDER_OCP_UPGRADE: 1,
        constants.ORDER_OCS_UPGRADE: 2,
        constants.ORDER_MCO_UPGRADE: 3,
        constants.ORDER_DR_HUB_UPGRADE: 4,
        constants.ORDER_ACM_UPGRADE: 5,
    }

    def __init__(self):
        self.dr_type = "rdr"
        super().__init__()
        self.all_roles = RDR_ROLES
        # If the current run includes PassiveACM then we need to add
        # it to the list as by default RDR Roles list will not have PassiveACM
        if get_passive_acm_index():
            self.all_roles.append("PassiveACM")

    def config_init(self):
        super().config_init()

    def reeval_upgrade_order(self, phase_order, zrank, role_rank):
        """
        Args:
            phase_order: The component order which is under upgrade for ex: ORDER_OCP_UPGRADE etc
            zrank: zone in which the cluster is present
            role_rank: Rank of the cluster based on role

        Returns:
            neworder (int): New value with which the test need to be order marked so that RDR specific
                upgrade test order is preserved

        """
        # We will take a simple approach of scaling the order number for the test
        # based on its ranking from UPGRADE_TEST_ORDER, higher the scale value
        # later the test gets scheduled
        # For example: consider the following tests with zonerank+role_rank+phase_order values
        # 1.[ACM ocp upgrade test] test_ocp_upgrade(100,1,30),
        # 2.[Primary managed cluster OCP upgrade] test_ocp_upgrade(200,2,30)
        # 3.[Secondary managed cluster OCP upgrade] test_ocp_upgrade(300,2,30)
        # 4.[primary mc ODF upgrade ] test_ocs_upgrade(200,2,60)
        # 5.[secondary mc ODF upgrade] test_ocs_upgrade(300,2,60)
        # 6.[ACM cluster's MCO upgrade] test_mco_upgrade(100,1,42)
        # if we run the tests with above values where order marker gets sum of the values mentioned in the
        # tuple then we can't follow the RDR upgrade sequence hence it needs resequencing.
        # RDR Upgrade tests order:
        # 1. ACM OCP upgrade
        # 2. Primary cluster OCP upgrade
        # 3. Secondary cluster OCP upgrade
        # 4. Primary ODF upgrade
        # 5. Secondary ODF upgrade
        # 6. ACM MCO operator upgrade
        # 7. ACM DR Hub operator upgrade
        # 8. Primary/Secondary DR cluster operator upgrade (automatic once hub is upgraded)
        # 9. ACM upgrade (if test is selected)
        neworder = phase_order + zrank + role_rank
        scaling = self.UPGRADE_TEST_ORDER.get(phase_order, 10)
        neworder = neworder * (10**scaling)
        return neworder


class RDRProviderClusterUpgradeParametrize(MultiClusterUpgradeParametrize):
    """
    Upgrade parametrization for the RDR Provider multicluster scenario.

    Topology
    --------
    - 1 Active ACM cluster (hub); optional Passive ACM cluster
    - 2 managed Provider clusters (PrimaryODF, SecondaryODF) running ODF in
      provider mode
    - N hosted client clusters (cluster_type == "hci_client") attached to each
      provider.  Clients on the same provider share that provider's zone so
      they are bucketed together in the ordering.

    Role ranks
    ----------
    ActiveACM / PassiveACM  -> ACM_RANK  (1)
    PrimaryODF / SecondaryODF -> MANAGED_CLUSTER_RANK  (2)
    HostedClient              -> HOSTED_CLIENT_RANK    (3)

    Each hosted client cluster gets its own param tuple keyed as
    ``HostedClient-<config_index>``.  The config_index acts as the tie-breaker
    so that every client receives a unique ordering slot within its zone bucket.

    Upgrade sequence
    ----------------
    Phase                         Scale   Clusters
    ─────────────────────────────────────────────────────────────────────────
    1. OCP upgrade                  1     ACM → PrimaryODF → SecondaryODF
    2. MCE upgrade                  2     PrimaryODF → SecondaryODF
    3. OCP-on-kubevirt upgrade      3     clients of Provider1, then Provider2
    4. OCS upgrade                  4     PrimaryODF → SecondaryODF
    5. MCO upgrade                  5     ACM
    6. DR Hub upgrade               6     ACM
    7. ACM upgrade                  7     ACM
    ─────────────────────────────────────────────────────────────────────────

    The exponential scaling (10 ** scale) ensures phase boundaries never
    overlap regardless of zone-rank or role-rank values.
    """

    # Maps each phase-order constant -> scale exponent.
    # Higher scale == later in the overall test run.
    UPGRADE_TEST_ORDER = {
        constants.ORDER_OCP_UPGRADE: 1,
        constants.ORDER_MCE_UPGRADE: 2,
        constants.ORDER_OCP_ON_KUBEVIRT_UPGRADE: 3,
        constants.ORDER_OCS_UPGRADE: 4,
        constants.ORDER_MCO_UPGRADE: 5,
        constants.ORDER_DR_HUB_UPGRADE: 6,
        constants.ORDER_ACM_UPGRADE: 7,
    }

    def __init__(self):
        self.dr_type = "rdr-provider"
        super().__init__()
        # Start from the static role list; dynamic entries are appended below
        self.all_roles = list(RDR_PROVIDER_ROLES)

        # Optional secondary ACM
        if get_passive_acm_index():
            self.all_roles.append("PassiveACM")

        # One "HostedClient" entry per hci_client cluster.  They all share the
        # same role string; the embedded config_index in the param tuple
        # provides the unique ordering within the zone.
        self._hosted_client_indexes = self._get_hosted_client_indexes()
        for _ in self._hosted_client_indexes:
            self.all_roles.append("HostedClient")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_hosted_client_indexes(self):
        """Return sorted config indexes for every hci_client cluster."""
        return [
            i
            for i, cluster in enumerate(ocsci_config.clusters)
            if cluster.ENV_DATA.get("cluster_type") == "hci_client"
        ]

    def _get_provider_index_for_client(self, client_index):
        """
        Return the config index of the provider cluster that hosts the client
        at *client_index*.  Resolves via ``provider_cluster_name`` in the
        client's ENV_DATA; falls back to the first provider cluster found.
        """
        client_cluster = ocsci_config.clusters[client_index]
        provider_name = client_cluster.ENV_DATA.get("provider_cluster_name")
        if provider_name:
            for i, cluster in enumerate(ocsci_config.clusters):
                if cluster.ENV_DATA.get("cluster_name") == provider_name:
                    return i
        provider_indexes = ocsci_config.get_provider_cluster_indexes()
        return provider_indexes[0] if provider_indexes else None

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def generate_config_index_map(self):
        """
        Build roles_to_config_index_map for the RDR-Provider topology.

        Static roles (ActiveACM, PassiveACM, PrimaryODF, SecondaryODF) are
        resolved here.  hci_client clusters are intentionally omitted because
        multiple clusters share the same ``HostedClient`` role string; they are
        handled individually in generate_zone_role_map and
        generate_role_to_param_tuple_map.
        """
        active_acm_index = get_active_acm_index()
        primary_index = get_primary_cluster_index()
        passive_acm_index = get_passive_acm_index()
        provider_indexes = ocsci_config.get_provider_cluster_indexes()

        for cluster in ocsci_config.clusters:
            cluster_index = cluster.MULTICLUSTER["multicluster_index"]
            if cluster_index == active_acm_index:
                self.roles_to_config_index_map["ActiveACM"] = cluster_index
            elif passive_acm_index and cluster_index == passive_acm_index:
                self.roles_to_config_index_map["PassiveACM"] = cluster_index
            elif cluster_index == primary_index:
                self.roles_to_config_index_map["PrimaryODF"] = cluster_index
            elif cluster_index in provider_indexes:
                # Second provider cluster that is not the primary
                self.roles_to_config_index_map["SecondaryODF"] = cluster_index
            # hci_client clusters are handled separately

    def generate_zone_role_map(self):
        """
        Build zone_role_map for static roles, then record the zone for each
        hosted client.

        Clients inherit their provider's zone so that all clients of the same
        provider form a single zone bucket.  This ensures the ordering is:
            Provider1 clients (zone A) ... Provider2 clients (zone B) ...
        """
        # Populate static roles (ACM + providers) first
        super().generate_zone_role_map()

        # Map each hosted client index -> its provider's zone
        self._client_zone_map = {}
        for client_index in self._hosted_client_indexes:
            provider_index = self._get_provider_index_for_client(client_index)
            if provider_index is not None:
                zone = ocsci_config.clusters[provider_index].ENV_DATA.get("zone")
            else:
                zone = ocsci_config.clusters[client_index].ENV_DATA.get("zone")
            self._client_zone_map[client_index] = zone

    def generate_role_to_param_tuple_map(self):
        """
        Build (zone_rank, role_rank, config_index) tuples for every role.

        Static roles (ACM + providers) each get one tuple keyed by role name.
        Every hosted client gets its own tuple keyed as
        ``HostedClient-<config_index>`` so that multiple clients within the
        same zone are still ordered by ascending config_index.
        """
        # Static roles
        for role in self.roles_to_config_index_map:
            self.roles_to_param_tuples[role] = (
                self.zone_ranks[self.zone_role_map[role]],
                self.role_ranks[role],
                self.roles_to_config_index_map[role],
            )

        # One tuple per hosted client, keyed as HostedClient-<config_index>
        for client_index in self._hosted_client_indexes:
            zone = self._client_zone_map.get(client_index)
            if zone and zone in self.zone_ranks:
                zrank = self.zone_ranks[zone]
            else:
                # Fallback: place beyond all defined zones
                zrank = self.zone_base_rank * (len(self.zones) + 1)
            self.roles_to_param_tuples[f"HostedClient-{client_index}"] = (
                zrank,
                self.role_ranks["HostedClient"],
                client_index,
            )

    def config_init(self):
        super().config_init()
        # Extend index_to_role to cover hosted clients
        for client_index in self._hosted_client_indexes:
            self.index_to_role[client_index] = f"HostedClient-{client_index}"

    def get_pytest_params_tuple(self, role):
        """
        Extend base behaviour with two rdr-provider-specific compound roles:

        ``rdr-provider-all-clients``
            Returns param tuples for every hosted client cluster, ordered by
            zone (provider affinity) then by config_index within the zone.

        ``rdr-provider-all-providers``
            Returns param tuples for PrimaryODF and SecondaryODF only.
        """
        if role == f"{self.dr_type}-all-clients":
            return [
                self.roles_to_param_tuples[f"HostedClient-{i}"]
                for i in self._hosted_client_indexes
            ]
        if role == f"{self.dr_type}-all-providers":
            return [
                self.roles_to_param_tuples[r]
                for r in ("PrimaryODF", "SecondaryODF")
                if r in self.roles_to_param_tuples
            ]
        return super().get_pytest_params_tuple(role)

    def reeval_upgrade_order(self, phase_order, zrank, role_rank):
        """
        Compute a unique, globally-ordered test-order number that honours the
        RDR-Provider upgrade sequence.

        The base value ``phase_order + zrank + role_rank`` is multiplied by
        ``10 ** scale`` where *scale* comes from UPGRADE_TEST_ORDER.  Because
        the scale grows by at least 1 per phase, no two phases can ever produce
        overlapping order numbers regardless of how many clusters exist.

        Concrete example (zone_base_rank=100, 2 zones a=100/b=200):

        Phase                      scale  cluster          base  final order
        ─────────────────────────────────────────────────────────────────────
        OCP upgrade (30)             1    ACM  (z=100,r=1)  131  1_310
                                          PriODF(z=100,r=2) 132  1_320
                                          SecODF(z=200,r=2) 232  2_320
        MCE upgrade (46)             2    PriODF(z=100,r=2) 148  14_800
                                          SecODF(z=200,r=2) 248  24_800
        OCP-kubevirt (48)            3    Client1(z=100,r=3,i=3) 151 151_000
                                          Client2(z=200,r=3,i=4) 252 252_000
        OCS upgrade (60)             4    PriODF(z=100,r=2) 162  1_620_000
        MCO upgrade (42)             5    ACM  (z=100,r=1)  143  14_300_000
        DR Hub upgrade (44)          6    ACM  (z=100,r=1)  145  145_000_000
        ACM upgrade (46)             7    ACM  (z=100,r=1)  147  1_470_000_000
        ─────────────────────────────────────────────────────────────────────
        """
        neworder = phase_order + zrank + role_rank
        scaling = self.UPGRADE_TEST_ORDER.get(phase_order, 10)
        neworder = neworder * (10**scaling)
        return neworder


multicluster_upgrade_parametrizer = {
    "metro-dr": MDRClusterUpgradeParametrize,
    "regional-dr": RDRClusterUpgradeParametrize,
    "rdr-provider": RDRProviderClusterUpgradeParametrize,
}


def get_multicluster_upgrade_parametrizer():
    return multicluster_upgrade_parametrizer[
        ocsci_config.MULTICLUSTER["multicluster_mode"]
    ]()


def create_mce_catsrc():
    """
    Create MCE CatalogSource in the marketplace namespace
    1. Create ImageDigestMirrorSet for ACM Deployment if not present
    2. Create Konflux Catalogsource for MCE
    3. Wait for Catalogsource to be in READY state

    """
    # if idms were not created during acm deployment, create it now
    log.info("Creating ImageDigestMirrorSet for ACM Deployment if not present")
    try:
        # attempt to apply the IDMS from the template. `oc apply` is idempotent so
        # if the resource already exists it will be updated/unchanged instead of erroring.
        run_cmd(f"oc apply -f {constants.ACM_BREW_IDMS_YAML}")
        wait_for_machineconfigpool_status(node_type="all")
        log.info("ACM Brew ImageDigestMirrorSet applied (or already present)")
    except Exception as ex:
        # If application failed for any reason, log and continue to create catalogsource
        log.warning(f"Failed to apply ACM Brew ImageDigestMirrorSet: {ex}")

    log.info("Creating Konflux Catalogsource for MCE ")
    mce_konflux_catsrc_yaml_data = templating.load_yaml(
        constants.MCE_CATALOGSOURCE_YAML
    )
    if not config.ENV_DATA.get("mce_unreleased_image"):
        mce_image_tag = get_acm_mce_build_tag(
            constants.MCE_CATSRC_IMAGE, config.ENV_DATA.get("mce_version")
        )
    else:
        mce_image_tag = config.ENV_DATA.get("mce_unreleased_image")

    mce_konflux_catsrc_yaml_data["spec"][
        "image"
    ] = f"{constants.MCE_CATSRC_IMAGE}:{mce_image_tag}"

    mce_konflux_catsrc_yaml_data_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="mce_konflux_catsrc_yaml_data_manifest", delete=False
    )
    templating.dump_data_to_temp_yaml(
        mce_konflux_catsrc_yaml_data, mce_konflux_catsrc_yaml_data_manifest.name
    )
    run_cmd(f"oc create -f {mce_konflux_catsrc_yaml_data_manifest.name}")
    mce_operator_catsrc = CatalogSource(
        resource_name=constants.MCE_DEV_CATALOG_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    mce_operator_catsrc.wait_for_state("READY")
