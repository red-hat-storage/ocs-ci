"""
All multicluster specific utility functions and classes can be here

"""

from ocs_ci.ocs.constants import mdr_roles
from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs.utils import (
    get_primary_cluster_index,
    get_active_acm_index,
    get_all_acm_indexes,
)


class MutliClusterUpgradeParametrize(object):
    """
    This base class abstracts upgrade parametrization for multicluster scenarios: MDR, RDR and Managed service

    """

    MULTICLUSTER_UPGRADE_MARKERS = [
        "pre_upgrade",
        "pre_ocp_upgrade",
        "ocp_upgrade",
        "post_ocp_upgrade",
        "pre_ocs_upgrade",
        "ocs_upgrade",
        "post_ocs_upgrade",
        "post_upgrade",
    ]

    def __init__(self):
        self.roles = []
        # List of zones which are participating in this multicluster setup
        self.zones = []
        self.zone_base_rank = 100
        # Each zone will be assigned with a rank
        # This rank comes handy when we have to order the tests
        self.zone_ranks = {}

    def get_roles(self, metafunc):
        """
        should be overridden in the child class
        Look for specific role markers based on multicluster scenario

        Args:
            metafunc: Pytest metafunc fixture object
        """
        pass

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

    def generate_role_ranks(self):
        """
        Based on the multicluster scenario, child class should generate the corresponding
        role ranks. Roles are specific to multicluster scenarios

        """
        pass

    def generate_pytest_parameters(self, metafunc, roles):
        """
        should be overridden in the child class.
        This will be called for every testcase parametrization

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


class MDRClusterUpgradeParametrize(MutliClusterUpgradeParametrize):
    """
    This child class handles MDR upgrade scenario specific pytest parametrization

    """

    def __init__(self):
        super().__init__()
        self.roles_to_param_tuples = dict()
        self.roles_to_config_index_map = dict()
        self.zone_role_map = dict()

        self.zones = self.get_zone_info()
        self.mdr_roles = self.get_mdr_roles()
        self.generate_zone_ranks()
        self.generate_role_ranks()
        self.generate_role_to_param_tuple_map()
        self.generate_config_index_map()
        self.generate_zone_role_map()

    # In ocs-ci we need to build this based on the config provided
    def get_config_index_map(self):
        # TO BE built before pytest_generate_test
        if not self.roles_to_config_index_map:
            self.generate_config_index_map()
        return self.roles_to_config_index_map

    def generate_config_index_map(self):
        for cluster in ocsci_config:
            cluster_index = cluster.MULTICLUSTER["multicluster_index"]
            if cluster_index == get_active_acm_index():
                self.roles_to_config_index_map["ActiveACM"] = cluster_index
            elif cluster_index == get_primary_cluster_index():
                self.roles_to_config_index_map["Primary_odf"] = cluster_index
            elif cluster_index in get_all_acm_indexes():
                # We would have already ruled out the ActiveACM in the first 'if'
                self.roles_to_config_index_map["PassiveACM"] = cluster_index
            else:
                # Only option left is secondary odf
                self.roles_to_config_index_map["Secondary_odf"] = cluster_index

    def get_mdr_roles(self):
        """
        All MDR applicable roles

        """
        return mdr_roles

    def generate_role_ranks(self):
        """
        Based on current roles for MDR : ActiveACM:1, PassiceACM:1, Primary:2, Secondary: 2

        """
        # For now we will stick to this convention
        self.role_ranks = {
            "ActiveACM": 1,
            "PassiveACM": 1,
            "Primary_odf": 2,
            "Secondary_odf": 2,
        }

    def generate_zone_role_map(self):
        """
        Generate a map of Cluster's role vs zone in which clusters are located

        """
        for crole, cindex in self.roles_to_config_index_map.items():
            czone = ocsci_config.clusters[cindex].ENV_DATA.get("zone")
            if czone:
                self.zone_role_map[crole] = czone

    def generate_role_to_param_tuple_map(self):
        """
        For each of the MDRs applicable roles store a tuple (zone_rank, role_rank, config_index)

        """
        for role in self.mdr_roles:
            self.roles_to_param_tuples[role] = (
                self.zone_ranks[self.zone_role_map[role]],
                self.role_ranks[role],
                self.get_config_index_map()[role],
            )

    def get_pytest_params_tuple(self, role):
        """
        Generate a tuple of parameters applicable to the given role
        For ex: if role is 'ActiveACM', then generate a tuple which is applicable to
        that role. If the role is 'all' then we will generate tuple of parameter
        for each of the role applicable from MDRs perspective.
        Parmeter tuples looks like (zone_rank, role_rank, config_index)
        """
        param_list = list()
        if role == "all":
            for t in self.roles_to_param_tuples.values():
                param_list.append(t)
            param_list
        else:
            param_list.append(self.roles_to_param_tuples[role])
        return param_list

    def get_roles(self, metafunc):
        # Return a list of roles applicable to the current test
        for marker in metafunc.definition.iter_markers():
            if marker.name == "mdr_roles":
                return marker.args[0]

    def generate_pytest_parameters(self, metafunc, roles):
        """
        We will have to parametrize the test based on the MDR roles to which the test is applicable to,
        Parameters will be a tuple of (zone_rank, role_rank, config_index)

        """
        pytest_params = []
        for role in roles:
            pytest_params.extend(self.get_pytest_params_tuple(role))
        return pytest_params


multicluster_upgrade_parametrizer = {"metro-dr": MDRClusterUpgradeParametrize}


def get_multicluster_upgrade_parametrizer():
    return multicluster_upgrade_parametrizer["metro-dr"]()
