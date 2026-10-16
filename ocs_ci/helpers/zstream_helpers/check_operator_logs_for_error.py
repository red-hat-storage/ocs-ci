"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/helpers.py
Insertion point: after function verify_clusterrole_has_verb_for_resource
Description: Check operator pod logs for a specific error string on a given cluster

Review and merge this into ocs_ci/helpers/helpers.py before running the test.
"""

def check_operator_logs_for_error(cluster_index, operator_name, error_string, tail_lines=500):
    """
    Check operator pod logs for a specific error string.

    Searches for pods matching the operator name and checks their logs
    for the given error string.

    Args:
        cluster_index (int): The config index for the target cluster.
        operator_name (str): Name (or partial name) of the operator pod to search.
        error_string (str): The error string to search for in logs.
        tail_lines (int): Number of log lines to tail. Default 500.

    Returns:
        bool: True if the error string is found in any matching pod's logs, False otherwise.
    """
    from ocs_ci.framework import config
    from ocs_ci.ocs import ocp

    with config.RunWithConfigContext(cluster_index):
        pod_ocp = ocp.OCP(
            kind="Pod",
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        pods = []
        for selector_key in ["app.kubernetes.io/name", "name"]:
            try:
                result = pod_ocp.get(selector=f"{selector_key}={operator_name}").get("items", [])
                if result:
                    pods = result
                    break
            except Exception:
                continue

        if not pods:
            all_pods = pod_ocp.get().get("items", [])
            pods = [
                p for p in all_pods
                if operator_name in p.get("metadata", {}).get("name", "")
            ]

        for pod_item in pods:
            pod_name = pod_item["metadata"]["name"]
            for container_args in [f"--tail={tail_lines} -c manager", f"--tail={tail_lines}"]:
                try:
                    log_output = pod_ocp.exec_oc_cmd(
                        f"logs {pod_name} {container_args}",
                        out_yaml_format=False,
                    )
                    if error_string in log_output:
                        logger.warning(f"Found error '{error_string}' in pod {pod_name} logs")
                        return True
                    break
                except Exception as e:
                    logger.debug(f"Could not get logs from pod {pod_name} with args '{container_args}': {e}")

        logger.info(f"No '{error_string}' errors found in operator logs")
        return False
