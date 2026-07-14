# -*- coding: utf8 -*-

# See also:
# https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/


from ocs_ci.ocs import constants


# well known topologies
ZONE_LABEL = constants.ZONE_LABEL
REGION_LABEL = constants.REGION_LABEL


def drop_topology_constraint(spec_dict, topology_key):
    """
    Removes constraints for given topology key from given
    dict spec with topologySpreadConstraints. If the constraint
    is not present, the spec dict is not changed.

    Args:
        tsc_dict (dict): topologySpreadConstraints spec
        topology_key (string): name of the topology key
    """
    tsc_list = spec_dict.get("topologySpreadConstraints")
    if tsc_list is None:
        return
    for cons in tsc_list[:]:
        if cons["topologyKey"] == topology_key:
            tsc_list.remove(cons)
