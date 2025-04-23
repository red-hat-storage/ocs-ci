"""
All multicluster specific utility functions and classes can be here

"""

from abc import ABC, abstractmethod
import logging

from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs.utils import (
    get_non_acm_cluster_indexes,
    get_primary_cluster_index,
    get_active_acm_index,
    get_all_acm_indexes,
)
from ocs_ci.ocs.constants import MDR_ROLES, RDR_ROLES

log = logging.getLogger(__name__)


class MultiClusterUpgradeParametrize(ABC):
    """
    This base class abstracts upgrade parametrization for multicluster scenarios: MDR, RDR and Managed service

    """

    MULTICLUSTER_UPGRADE_MARKERS = [
        "pre_upgrade",
        "pre_ocp_upgrade",
        "ocp_upgrade",
        "post_ocp_upgrade",
        "mco_upgrade",
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
        for cluster in ocsci_config.clusters:
            cluster_index = cluster.MULTICLUSTER["multicluster_index"]
            if cluster_index == get_active_acm_index():
                self.roles_to_config_index_map["ActiveACM"] = cluster_index
            elif cluster_index == get_primary_cluster_index():
                self.roles_to_config_index_map["PrimaryODF"] = cluster_index
            elif (
                cluster_index in get_all_acm_indexes()
                and cluster_index != get_active_acm_index()
            ):
                # We would have already ruled out the ActiveACM in the first 'if'
                self.roles_to_config_index_map["PassiveACM"] = cluster_index
            else:
                # Only option left is secondary odf
                self.roles_to_config_index_map["SecondaryODF"] = cluster_index

    @abstractmethod
    def generate_role_ranks(self):
        """
        Based on the multicluster scenario, child class should generate the corresponding
        role ranks. Roles are specific to multicluster scenarios

        """
        pass

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

    def generate_role_ranks(self):
        """
        Based on current roles for MDR : ActiveACM:1, PassiveACM:1, PrimaryODF:2, SecondaryODF: 2

        """
        # For now we will stick to this convention
        self.role_ranks = {
            "ActiveACM": 1,
            "PassiveACM": 1,
            "PrimaryODF": 2,
            "SecondaryODF": 2,
        }


class RDRClusterUpgradeParametrize(MultiClusterUpgradeParametrize):
    """
    This child class handles RDR upgrade scenario specific pytest parametrization

    """

    def __init__(self):
        self.dr_type = "rdr"
        super().__init__()
        self.all_roles = RDR_ROLES

    def config_init(self):
        super().config_init()

    def generate_role_ranks(self):
        """
        Based on the current roles for RDR : ActiveACM:1, PrimaryODF:2, SecondaryODF: 2

        """
        self.role_ranks = {
            "ActiveACM": 1,
            "PrimaryODF": 2,
            "SecondaryODF": 2,
        }


multicluster_upgrade_parametrizer = {
    "metro-dr": MDRClusterUpgradeParametrize,
    "regional-dr": RDRClusterUpgradeParametrize,
}


def get_multicluster_upgrade_parametrizer():
    return multicluster_upgrade_parametrizer[
        ocsci_config.MULTICLUSTER["multicluster_mode"]
    ]()
