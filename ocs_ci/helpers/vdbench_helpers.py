import logging
import yaml
import tempfile

from ocs_ci.utility.utils import run_cmd

log = logging.getLogger(__name__)


def create_vdbench_config_from_dict(vdbench_dict):
    """
    Create a Vdbench configuration file from a dictionary.

    Args:
        vdbench_dict: Dictionary containing Vdbench configuration

    Returns:
        str: Path to created configuration file
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(vdbench_dict, f, default_flow_style=False)
        config_file = f.name

    log.info(f"Created Vdbench config file: {config_file}")
    return config_file


def validate_vdbench_config(config_dict):
    """
    Validate Vdbench configuration dictionary.

    Args:
        config_dict: Vdbench configuration to validate

    Returns:
        bool: True if configuration is valid

    Raises:
        ValueError: If configuration is invalid
    """
    required_sections = [
        "storage_definitions",
        "workload_definitions",
        "run_definitions",
    ]

    for section in required_sections:
        if section not in config_dict:
            raise ValueError(f"Missing required section: {section}")

    # Validate storage definitions
    for sd in config_dict["storage_definitions"]:
        if "id" not in sd or "lun" not in sd:
            raise ValueError("Storage definition missing required fields: id, lun")

    # Validate workload definitions
    for wd in config_dict["workload_definitions"]:
        if "id" not in wd or "sd_id" not in wd:
            raise ValueError("Workload definition missing required fields: id, sd_id")

    # Validate run definitions
    for rd in config_dict["run_definitions"]:
        if "id" not in rd or "wd_id" not in rd:
            raise ValueError("Run definition missing required fields: id, wd_id")

    log.info("Vdbench configuration is valid")
    return True


def get_default_vdbench_configs():
    """
    Get predefined Vdbench configurations for common use cases.

    Returns:
        dict: Dictionary of configuration name to config dict
    """
    configs = {
        "basic": {
            "storage_definitions": [
                {"id": 1, "lun": "/vdbench-data/testfile", "size": "1g", "threads": 1}
            ],
            "workload_definitions": [
                {"id": 1, "sd_id": 1, "rdpct": 50, "seekpct": 100, "xfersize": "4k"}
            ],
            "run_definitions": [
                {"id": 1, "wd_id": 1, "elapsed": 60, "interval": 5, "iorate": "max"}
            ],
        },
        "performance": {
            "storage_definitions": [
                {"id": 1, "lun": "/vdbench-data/perftest", "size": "10g", "threads": 4}
            ],
            "workload_definitions": [
                {"id": 1, "sd_id": 1, "rdpct": 70, "seekpct": 100, "xfersize": "64k"},
                {"id": 2, "sd_id": 1, "rdpct": 0, "seekpct": 100, "xfersize": "1m"},
            ],
            "run_definitions": [
                {"id": 1, "wd_id": 1, "elapsed": 300, "interval": 10, "iorate": "1000"},
                {"id": 2, "wd_id": 2, "elapsed": 180, "interval": 10, "iorate": "max"},
            ],
        },
        "block": {
            "storage_definitions": [
                {"id": 1, "lun": "/dev/vdbench-device", "size": "100%", "threads": 2}
            ],
            "workload_definitions": [
                {"id": 1, "sd_id": 1, "rdpct": 50, "seekpct": 100, "xfersize": "8k"}
            ],
            "run_definitions": [
                {"id": 1, "wd_id": 1, "elapsed": 120, "interval": 5, "iorate": "max"}
            ],
        },
        "stress": {
            "storage_definitions": [
                {"id": 1, "lun": "/vdbench-data/stress1", "size": "5g", "threads": 4},
                {"id": 2, "lun": "/vdbench-data/stress2", "size": "5g", "threads": 4},
            ],
            "workload_definitions": [
                {"id": 1, "sd_id": 1, "rdpct": 30, "seekpct": 100, "xfersize": "4k"},
                {"id": 2, "sd_id": 2, "rdpct": 70, "seekpct": 100, "xfersize": "64k"},
            ],
            "run_definitions": [
                {"id": 1, "wd_id": 1, "elapsed": 1800, "interval": 30, "iorate": "max"},
                {"id": 2, "wd_id": 2, "elapsed": 1800, "interval": 30, "iorate": "max"},
            ],
        },
        "filesystem": {
            "storage_definitions": [
                {
                    "id": 1,
                    "fsd": True,
                    "anchor": "/vdbench-data/fs-test",
                    "depth": 2,
                    "width": 4,
                    "files": 10,
                    "size": "1g",
                }
            ],
            "workload_definitions": [
                {
                    "id": 1,
                    "sd_id": 1,
                    "rdpct": 50,
                    "xfersize": "8k",
                    "threads": 2,
                }
            ],
            "run_definitions": [
                {
                    "id": 1,
                    "wd_id": 1,
                    "elapsed": 120,
                    "interval": 5,
                    "iorate": "max",
                }
            ],
        },
    }

    return configs


def get_vdbench_pods(namespace, deployment_name):
    """
    Get list of Vdbench pod names for a deployment.

    Args:
        namespace: Kubernetes namespace
        deployment_name: Name of the deployment

    Returns:
        list: List of pod names
    """
    try:
        cmd = f"oc get pods -n {namespace} -l app={deployment_name} -o name"
        result = run_cmd(cmd)
        pods = [pod.replace("pod/", "") for pod in result.strip().split("\n") if pod]
        return pods
    except Exception as e:
        log.error(f"Failed to get Vdbench pods: {e}")
        return []


def get_vdbench_logs(namespace, deployment_name, container="vdbench-container"):
    """
    Get logs from all Vdbench pods in a deployment.

    Args:
        namespace: Kubernetes namespace
        deployment_name: Name of the deployment
        container: Container name to get logs from

    Returns:
        dict: Dictionary mapping pod name to logs
    """
    pods = get_vdbench_pods(namespace, deployment_name)
    logs_dict = {}

    for pod in pods:
        try:
            cmd = f"oc logs -n {namespace} {pod} -c {container}"
            logs = run_cmd(cmd)
            logs_dict[pod] = logs
        except Exception as e:
            log.warning(f"Failed to get logs from pod {pod}: {e}")
            logs_dict[pod] = f"Error getting logs: {e}"

    return logs_dict


def parse_vdbench_output(logs):
    """
    Parse Vdbench output logs to extract performance metrics.

    Args:
        logs: Vdbench output logs

    Returns:
        dict or None: Parsed metrics or None if parsing fails
    """
    metrics = {}

    try:
        lines = logs.split("\n")
        for line in lines:
            line = line.strip()

            # Parse summary statistics
            if "avg_rate" in line and "resp" in line:
                # Example line: "avg_rate=1234.5 resp=5.67"
                parts = line.split()
                for part in parts:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        try:
                            metrics[key] = float(value)
                        except ValueError:
                            metrics[key] = value

            # Parse other relevant metrics
            if "Total" in line and "rate=" in line:
                # Parse total statistics
                if "rate=" in line:
                    rate_part = line.split("rate=")[1].split()[0]
                    try:
                        metrics["total_rate"] = float(rate_part)
                    except ValueError:
                        pass

        return metrics if metrics else None

    except Exception as e:
        log.error(f"Failed to parse Vdbench output: {e}")
        return None


def monitor_vdbench_workload(workload, interval=30, duration=300):
    """
    Monitor a running Vdbench workload and collect metrics.

    Args:
        workload: VdbenchWorkload instance
        interval: Monitoring interval in seconds
        duration: Total monitoring duration in seconds

    Returns:
        list: List of metric snapshots
    """
    import time

    metrics_history = []
    start_time = time.time()

    while (time.time() - start_time) < duration:
        try:
            # Get current status
            status = workload.get_workload_status()

            # Get logs and parse metrics
            logs_dict = get_vdbench_logs(workload.namespace, workload.deployment_name)

            snapshot = {"timestamp": time.time(), "status": status, "metrics": {}}

            # Parse metrics from each pod
            for pod_name, logs in logs_dict.items():
                pod_metrics = parse_vdbench_output(logs)
                if pod_metrics:
                    snapshot["metrics"][pod_name] = pod_metrics

            metrics_history.append(snapshot)

            log.info(f"Collected metrics snapshot {len(metrics_history)}")
            time.sleep(interval)

        except Exception as e:
            log.error(f"Error during monitoring: {e}")
            break

    return metrics_history


def create_vdbench_performance_report(metrics_history, output_file=None):
    """
    Create a performance report from collected metrics.

    Args:
        metrics_history: Metrics history from monitoring
        output_file: Path to save report file

    Returns:
        dict: Performance report summary
    """
    if not metrics_history:
        return {}

    report = {
        "summary": {
            "total_snapshots": len(metrics_history),
            "duration": metrics_history[-1]["timestamp"]
            - metrics_history[0]["timestamp"],
            "start_time": metrics_history[0]["timestamp"],
            "end_time": metrics_history[-1]["timestamp"],
        },
        "performance": {
            "avg_rate": [],
            "peak_rate": 0,
            "min_rate": float("inf"),
            "response_times": [],
        },
        "details": metrics_history,
    }

    # Aggregate performance metrics
    for snapshot in metrics_history:
        for pod_name, pod_metrics in snapshot.get("metrics", {}).items():
            if "avg_rate" in pod_metrics:
                rate = pod_metrics["avg_rate"]
                report["performance"]["avg_rate"].append(rate)
                report["performance"]["peak_rate"] = max(
                    report["performance"]["peak_rate"], rate
                )
                report["performance"]["min_rate"] = min(
                    report["performance"]["min_rate"], rate
                )

            if "resp" in pod_metrics:
                report["performance"]["response_times"].append(pod_metrics["resp"])

    # Calculate averages
    if report["performance"]["avg_rate"]:
        report["performance"]["average_rate"] = sum(
            report["performance"]["avg_rate"]
        ) / len(report["performance"]["avg_rate"])

    if report["performance"]["response_times"]:
        report["performance"]["average_response_time"] = sum(
            report["performance"]["response_times"]
        ) / len(report["performance"]["response_times"])

    # Save to file if requested
    if output_file:
        try:
            with open(output_file, "w") as f:
                yaml.dump(report, f, default_flow_style=False)
            log.info(f"Performance report saved to: {output_file}")
        except Exception as e:
            log.error(f"Failed to save report: {e}")

    return report


def cleanup_vdbench_resources(namespace, deployment_name):
    """
    Clean up Vdbench resources manually.

    Args:
        namespace: Kubernetes namespace
        deployment_name: Name of the deployment to clean up
    """
    resources = [f"deployment/{deployment_name}", f"configmap/{deployment_name}-config"]

    for resource in resources:
        try:
            run_cmd(f"oc delete {resource} -n {namespace} --ignore-not-found=true")
            log.info(f"Deleted {resource}")
        except Exception as e:
            log.warning(f"Failed to delete {resource}: {e}")


def wait_for_vdbench_pods_ready(namespace, deployment_name, timeout=300):
    """
    Wait for Vdbench pods to be ready.

    Args:
        namespace: Kubernetes namespace
        deployment_name: Name of the deployment
        timeout: Timeout in seconds

    Returns:
        bool: True if pods are ready, False if timeout
    """
    from ocs_ci.utility.utils import TimeoutSampler

    try:
        for sample in TimeoutSampler(
            timeout, 10, _check_pods_ready, namespace, deployment_name
        ):
            if sample:
                return True
        return False
    except Exception as e:
        log.error(f"Error waiting for pods: {e}")
        return False


def _check_pods_ready(namespace, deployment_name):
    """
    Check if pods are ready.

    Args:
        namespace: Kubernetes namespace
        deployment_name: Name of the deployment

    Returns:
        bool: True if all pods are ready
    """
    try:
        cmd = (
            f"oc get pods -n {namespace} -l app={deployment_name} "
            f"-o jsonpath='{{.items[*].status.conditions[?(@.type==\"Ready\")].status}}'"
        )
        result = run_cmd(cmd)
        ready_statuses = result.strip().split()
        return (
            all(status == "True" for status in ready_statuses)
            and len(ready_statuses) > 0
        )
    except Exception:
        return False


def validate_vdbench_workload_health(workload, timeout=300):
    """
    Validate that a Vdbench workload is healthy and running.
    Args:
        workload: VdbenchWorkload instance to validate
        timeout: Timeout for validation in seconds
    Returns:
        bool: True if workload is healthy
    Raises:
        AssertionError: If workload is not healthy
    """
    status = workload.get_workload_status()

    assert status["is_running"], f"Workload {workload.deployment_name} is not running"
    assert not status["is_paused"], f"Workload {workload.deployment_name} is paused"
    assert (
        status["current_replicas"] > 0
    ), f"Workload {workload.deployment_name} has no replicas"

    # Check pod phases
    if "pod_phases" in status:
        running_pods = [phase for phase in status["pod_phases"] if phase == "Running"]
        assert len(running_pods) == status["current_replicas"], (
            f"Not all pods are running. Expected: {status['current_replicas']}, "
            f"Running: {len(running_pods)}"
        )

    log.info(f"Vdbench workload {workload.deployment_name} is healthy")
    return True


def create_temp_config_file(vdbench_config):
    """
    Create a temporary YAML configuration file from a dictionary.

    Args:
        vdbench_config (dict): Vdbench configuration dictionary

    Returns:
        str: Path to temporary configuration file
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(vdbench_config, f, default_flow_style=False)
        temp_file = f.name

    log.debug(f"Created temporary config file: {temp_file}")
    return temp_file
