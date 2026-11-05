"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/dr_helpers.py
Insertion point: after function get_drpolicy_conditions
Description: Get the parsed Ramen hub operator config from its ConfigMap

Review and merge this into ocs_ci/helpers/dr_helpers.py before running the test.
"""

def get_ramen_hub_config(namespace):
    """
    Retrieve and parse the Ramen hub operator configuration from its ConfigMap.

    Tries 'ramen-hub-operator-config' first, then falls back to
    'ramen-dr-cluster-operator-config'.

    Args:
        namespace (str): Namespace where the Ramen ConfigMap resides

    Returns:
        tuple: (OCP configmap object, dict of parsed ramen config data)

    Raises:
        Exception: If neither ConfigMap can be found
    """
    import logging
    import yaml
    from ocs_ci.ocs import ocp, constants

    logger = logging.getLogger(__name__)

    configmap_names = [
        constants.RAMEN_HUB_OPERATOR_CONFIG,
        constants.RAMEN_DR_CLUSTER_OPERATOR_CONFIG,
    ]

    for cm_name in configmap_names:
        try:
            cm_obj = ocp.OCP(
                kind="ConfigMap",
                namespace=namespace,
                resource_name=cm_name,
            )
            cm_data = cm_obj.get()
            ramen_config = yaml.safe_load(
                cm_data["data"].get(constants.RAMEN_MANAGER_CONFIG_KEY, "{}")
            )
            logger.info(f"Found Ramen config in ConfigMap: {cm_name}")
            return cm_obj, ramen_config
        except Exception:
            logger.debug(f"ConfigMap {cm_name} not found, trying next")
            continue

    raise Exception(
        f"Could not find Ramen hub operator ConfigMap in namespace {namespace}. "
        f"Tried: {configmap_names}"
    )
