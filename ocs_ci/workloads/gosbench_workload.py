"""
GOSBench workload management module for NooBaa/ODF S3 testing.

This module provides functionality to:
1. Start GOSBench workload
2. Modify workload parameters in ConfigMap
3. Stop workload
4. Monitor workload status and metrics
"""

import logging
import yaml
import base64

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.mcg import MCG
from ocs_ci.ocs.resources.pod import get_pods_having_label, wait_for_pods_to_be_running
from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility import templating

logger = logging.getLogger(__name__)


class GOSBenchWorkload:
    """
    GOSBench workload management class for NooBaa S3 performance testing.
    """

    def __init__(self, namespace=None, workload_name="gosbench"):
        """
        Initialize GOSBench workload manager.

        Args:
            namespace (str): Kubernetes namespace (default: openshift-storage)
            workload_name (str): Name prefix for workload resources
        """
        self.namespace = namespace or config.ENV_DATA["cluster_namespace"]
        self.workload_name = workload_name
        self.ocp = OCP(namespace=self.namespace)

        # Resource names
        self.config_name = f"{workload_name}-config"
        self.secret_name = f"{workload_name}-aws"
        self.server_name = f"{workload_name}-server"
        self.worker_name = f"{workload_name}-worker"
        self.service_name = f"{workload_name}-server"

        logger.info(
            f"Initialized GOSBench workload manager in namespace: {self.namespace}"
        )

    def get_noobaa_s3_endpoint(self):
        """
        Get NooBaa S3 endpoint from route or service.

        Returns:
            str: S3 endpoint URL
        """
        try:
            # Try to get external route first
            route_ocp = OCP(kind=constants.ROUTE, namespace=self.namespace)
            route_obj = route_ocp.get(resource_name="s3")
            endpoint = f"https://{route_obj['spec']['host']}"
            logger.info(f"Found external S3 endpoint: {endpoint}")
            return endpoint
        except CommandFailed:
            # Fallback to internal service endpoint
            endpoint = "https://s3.openshift-storage.svc.cluster.local"
            logger.info(f"Using internal S3 endpoint: {endpoint}")
            return endpoint

    def get_noobaa_credentials(self):
        """
        Get NooBaa admin credentials.

        Returns:
            tuple: (access_key_id, secret_access_key)
        """
        mcg = MCG()
        creds = mcg.get_noobaa_admin_credentials_from_secret()
        return creds["AWS_ACCESS_KEY_ID"], creds["AWS_SECRET_ACCESS_KEY"]

    def create_credentials_secret(self, access_key_id=None, secret_access_key=None):
        """
        Create Kubernetes secret with S3 credentials.

        Args:
            access_key_id (str): AWS access key ID (default: get from NooBaa)
            secret_access_key (str): AWS secret access key (default: get from NooBaa)

        Returns:
            bool: True if secret created successfully
        """
        if not access_key_id or not secret_access_key:
            access_key_id, secret_access_key = self.get_noobaa_credentials()

        secret_data = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": self.secret_name, "namespace": self.namespace},
            "type": "Opaque",
            "data": {
                "AWS_ACCESS_KEY_ID": base64.b64encode(access_key_id.encode()).decode(),
                "AWS_SECRET_ACCESS_KEY": base64.b64encode(
                    secret_access_key.encode()
                ).decode(),
            },
        }

        try:
            # Create temporary YAML file for the secret data
            import tempfile
            from ocs_ci.utility import templating

            secret_temp_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="gosbench_secret", delete=False
            )
            templating.dump_data_to_temp_yaml(secret_data, secret_temp_file.name)

            secret_ocp = OCP(kind="Secret", namespace=self.namespace)
            secret_ocp.create(yaml_file=secret_temp_file.name)
            logger.info(f"Created credentials secret: {self.secret_name}")
            return True
        except CommandFailed as e:
            if "already exists" in str(e):
                logger.info(f"Credentials secret {self.secret_name} already exists")
                return True
            logger.error(f"Failed to create credentials secret: {e}")
            raise

    def create_s3_bucket(self):
        """
        Create S3 bucket for GOSBENCH workload with health verification.

        Returns:
            str: The created bucket name
        """
        from ocs_ci.ocs.resources.mcg import MCG
        from ocs_ci.ocs.bucket_utils import s3_create_bucket
        import time

        try:
            # Get MCG object
            mcg_obj = MCG()

            # Create the bucket
            bucket_name = f"{self.workload_name}-bucket"
            s3_create_bucket(mcg_obj, bucket_name)

            logger.info(f"Created S3 bucket: {bucket_name}")

            # Verify bucket health and readiness
            logger.info(f"Verifying bucket health for: {bucket_name}")
            time.sleep(2)  # Brief initial wait for bucket initialization

            # Wait for bucket to be fully ready
            max_attempts = 10
            for attempt in range(max_attempts):
                try:
                    # Check if bucket exists and is accessible
                    if mcg_obj.s3_verify_bucket_exists(bucket_name):
                        # Try a simple list operation to verify accessibility
                        list(mcg_obj.s3_list_all_objects_in_bucket(bucket_name))
                        logger.info(
                            f"✓ Bucket {bucket_name} verified healthy and ready"
                        )
                        return bucket_name
                except Exception as e:
                    if attempt < max_attempts - 1:
                        logger.debug(
                            f"Bucket not ready yet (attempt {attempt+1}/{max_attempts}): {e}"
                        )
                        time.sleep(3)
                    else:
                        logger.warning(
                            f"Bucket health verification incomplete after {max_attempts} attempts: {e}"
                        )
                        # Return anyway - bucket might still work
                        return bucket_name

            return bucket_name

        except Exception as e:
            # Bucket might already exist, which is fine
            logger.info(f"S3 bucket creation note: {e}")
            bucket_name = f"{self.workload_name}-bucket"

            # Even if bucket exists, verify it's healthy
            try:
                mcg_obj = MCG()
                if mcg_obj.s3_verify_bucket_exists(bucket_name):
                    logger.info(f"✓ Existing bucket {bucket_name} verified")
            except Exception as verify_error:
                logger.warning(f"Could not verify existing bucket: {verify_error}")

            return bucket_name

    def delete_s3_bucket(self):
        """
        Delete S3 bucket created for GOSBENCH workload.

        Returns:
            bool: True if bucket deleted successfully
        """
        from ocs_ci.ocs.resources.mcg import MCG
        from ocs_ci.ocs.bucket_utils import s3_delete_bucket

        try:
            # Get MCG object
            mcg_obj = MCG()

            # Delete the bucket
            bucket_name = f"{self.workload_name}-bucket"
            s3_delete_bucket(mcg_obj, bucket_name)

            logger.info(f"Deleted S3 bucket: {bucket_name}")
            return True

        except Exception as e:
            # Bucket might not exist or already deleted, which is fine
            logger.info(f"S3 bucket deletion note: {e}")
            return True

    def create_workload_config(self, benchmark_config=None):
        """
        Create ConfigMap with GOSBench benchmark configuration.

        Args:
            benchmark_config (dict): Custom benchmark configuration

        Returns:
            bool: True if ConfigMap created successfully
        """
        s3_endpoint = self.get_noobaa_s3_endpoint()

        # Default benchmark configuration
        default_config = {
            "s3": {
                "endpoint": s3_endpoint,
                "region": "us-east-1",
                "access_key": "$AWS_ACCESS_KEY_ID",
                "secret_key": "$AWS_SECRET_ACCESS_KEY",
                "bucket": f"{self.workload_name}-bucket",
                "insecure_tls": False,
            },
            "benchmark": {
                "name": f"{self.workload_name}-mixed",
                "object": {"size": "1MiB", "count": 10000},
                "stages": [
                    {"name": "ramp", "duration": "30s", "op": "none"},
                    {"name": "put", "duration": "2m", "op": "put", "concurrency": 64},
                    {"name": "get", "duration": "2m", "op": "get", "concurrency": 64},
                    {
                        "name": "delete",
                        "duration": "1m",
                        "op": "delete",
                        "concurrency": 64,
                    },
                ],
            },
        }

        # Merge with custom config if provided
        if benchmark_config:
            self._deep_merge(default_config, benchmark_config)

        configmap_data = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": self.config_name, "namespace": self.namespace},
            "data": {
                "gosbench.yaml": yaml.dump(default_config, default_flow_style=False)
            },
        }

        try:
            # Create temporary YAML file for the ConfigMap data
            import tempfile

            cm_temp_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="gosbench_configmap", delete=False
            )
            templating.dump_data_to_temp_yaml(configmap_data, cm_temp_file.name)

            cm_ocp = OCP(kind="ConfigMap", namespace=self.namespace)
            cm_ocp.create(yaml_file=cm_temp_file.name)
            logger.info(f"Created workload ConfigMap: {self.config_name}")
            return True
        except CommandFailed as e:
            if "already exists" in str(e):
                logger.info(f"ConfigMap {self.config_name} already exists, updating...")
                return self.update_workload_config(benchmark_config or default_config)
            logger.error(f"Failed to create ConfigMap: {e}")
            raise

    def update_workload_config(self, benchmark_config):
        """
        Update existing ConfigMap with new benchmark configuration.

        Args:
            benchmark_config (dict): New benchmark configuration

        Returns:
            bool: True if ConfigMap updated successfully
        """
        try:
            cm_ocp = OCP(kind="ConfigMap", namespace=self.namespace)
            existing_cm = cm_ocp.get(resource_name=self.config_name)

            # Parse existing config
            existing_config = yaml.safe_load(existing_cm["data"]["gosbench.yaml"])

            # Merge with new config
            self._deep_merge(existing_config, benchmark_config)

            # Update ConfigMap
            patch_data = {
                "data": {
                    "gosbench.yaml": yaml.dump(
                        existing_config, default_flow_style=False
                    )
                }
            }

            cm_ocp.patch(
                resource_name=self.config_name, params=patch_data, format_type="merge"
            )
            logger.info(f"Updated workload ConfigMap: {self.config_name}")
            return True
        except CommandFailed as e:
            logger.error(f"Failed to update ConfigMap: {e}")
            raise

    def create_server_deployment(self, image=None, resource_limits=None):
        """
        Create GOSBench server deployment and service using templates.

        Args:
            image (str): Container image to use (default: ghcr.io/mulbc/gosbench:latest)
            resource_limits (dict): Resource limits and requests

        Returns:
            bool: True if server created successfully
        """
        s3_endpoint = self.get_noobaa_s3_endpoint()

        # Template variables for service
        service_template_vars = {
            "service_name": self.service_name,
            "namespace": self.namespace,
            "server_name": self.server_name,
            "workload_name": self.workload_name,
            "version": "latest",
            # Optional variables that template checks for
            "custom_labels": None,
            "annotations": None,
            "control_port": 2000,
            "metrics_port": 2112,
            "extra_ports": None,
            "service_type": "ClusterIP",
            "session_affinity": None,
            "session_affinity_config": None,
            "external_traffic_policy": None,
            "load_balancer_ip": None,
            "load_balancer_source_ranges": None,
            "external_ips": None,
        }

        # Template variables for deployment
        deployment_template_vars = {
            "server_name": self.server_name,
            "namespace": self.namespace,
            "s3_endpoint_host": s3_endpoint.replace("https://", ""),
            "secret_name": self.secret_name,
            "config_name": self.config_name,
            "image": image or "ghcr.io/mulbc/gosbench:latest",
            "workload_name": self.workload_name,
            "version": "latest",
            # Optional variables that template might check for
            "custom_labels": None,
            "annotations": None,
            "replicas": 1,
            "restart_policy": "Always",
            "image_pull_policy": "IfNotPresent",
            "service_account": None,
            "security_context": None,
            "pod_security_context": None,
            "node_selector": None,
            "tolerations": None,
            "affinity": None,
            "dns_policy": None,
            "dns_config": None,
        }

        # Add resource limits if provided
        if resource_limits:
            deployment_template_vars.update(resource_limits)

        try:
            # Create service from template using proper Templating class with to_nice_yaml filter - TEMPLATE VARS FIXED
            templating_obj = templating.Templating()
            service_yaml_content = templating_obj.render_template(
                "workloads/gosbench/server-service.yaml.j2", service_template_vars
            )
            logger.debug(f"Rendered service YAML content:\n{service_yaml_content}")
            service_data = yaml.safe_load(service_yaml_content)
            svc_ocp = OCP(kind="Service", namespace=self.namespace)
            # Create temporary YAML file for the Service data
            import tempfile

            svc_temp_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="gosbench_service", delete=False
            )
            templating.dump_data_to_temp_yaml(service_data, svc_temp_file.name)
            svc_ocp.create(yaml_file=svc_temp_file.name)
            logger.info(f"Created server service: {self.service_name}")

            # Create deployment from template using proper Templating class
            deployment_yaml_content = templating_obj.render_template(
                "workloads/gosbench/server-deployment.yaml.j2", deployment_template_vars
            )
            deployment_data = yaml.safe_load(deployment_yaml_content)

            # Fix container command and args for specific images
            container = deployment_data["spec"]["template"]["spec"]["containers"][0]
            current_image = container.get("image", "")

            if "goroom-server" in current_image:
                # For goroom-server image, run as server (no config file)
                # The server listens for HTTP requests from workers
                container["command"] = ["/app/main"]
                container["args"] = []  # No args - server mode
                logger.info(
                    "Fixed server container args for goroom-server image (server mode)"
                )

            deploy_temp_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="gosbench_server_deployment", delete=False
            )
            templating.dump_data_to_temp_yaml(deployment_data, deploy_temp_file.name)
            deploy_ocp = OCP(kind="Deployment", namespace=self.namespace)
            deploy_ocp.create(yaml_file=deploy_temp_file.name)
            logger.info(f"Created server deployment: {self.server_name}")

            return True
        except CommandFailed as e:
            if "already exists" in str(e):
                logger.info("Server resources already exist")
                return True
            logger.error(f"Failed to create server: {e}")
            raise

    def create_worker_deployment(self, replicas=5, image=None, resource_limits=None):
        """
        Create GOSBench worker deployment using template.

        Args:
            replicas (int): Number of worker replicas
            image (str): Container image to use (default: ghcr.io/mulbc/gosbench:latest)
            resource_limits (dict): Resource limits and requests

        Returns:
            bool: True if workers created successfully
        """
        # Template variables for worker deployment
        worker_template_vars = {
            "worker_name": self.worker_name,
            "namespace": self.namespace,
            "replicas": replicas,
            "server_endpoint": f"{self.service_name}.{self.namespace}.svc.cluster.local:2000",
            "image": image or "ghcr.io/mulbc/gosbench:latest",
            "workload_name": self.workload_name,
            "version": "latest",
            # Optional variables that template might check for
            "custom_labels": None,
            "annotations": None,
            "restart_policy": "Always",
            "image_pull_policy": "IfNotPresent",
            "service_account": None,
            "security_context": None,
            "pod_security_context": None,
            "node_selector": None,
            "tolerations": None,
            "affinity": None,
            "dns_policy": None,
            "dns_config": None,
        }

        # Add resource limits if provided
        if resource_limits:
            worker_template_vars.update(resource_limits)

        try:
            # Create deployment from template using proper Templating class
            templating_obj = templating.Templating()
            deployment_yaml_content = templating_obj.render_template(
                "workloads/gosbench/worker-deployment.yaml.j2", worker_template_vars
            )
            deployment_data = yaml.safe_load(deployment_yaml_content)

            # Fix container command and args for specific images
            container = deployment_data["spec"]["template"]["spec"]["containers"][0]
            current_image = container.get("image", "")

            if "goroom-worker" in current_image:
                # For goroom-worker image, use /app/main without worker subcommand
                server_endpoint = worker_template_vars["server_endpoint"]
                # goroom-worker expects pure host:port (NO http:// prefix)
                # The Go net.Dial() function cannot parse URLs with protocols
                worker_port = worker_template_vars.get(
                    "worker_port", 8888
                )  # Default port from help is 8888
                container["command"] = ["/app/main"]
                container["args"] = [
                    "-p",
                    str(worker_port),
                    "-s",
                    server_endpoint,  # Just host:port, no protocol
                ]
                logger.info(
                    f"Fixed worker container args for goroom-worker image: {server_endpoint}"
                )

            # Create temporary YAML file for the Deployment data
            import tempfile

            deploy_temp_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="gosbench_worker_deployment", delete=False
            )
            templating.dump_data_to_temp_yaml(deployment_data, deploy_temp_file.name)

            deploy_ocp = OCP(kind="Deployment", namespace=self.namespace)
            deploy_ocp.create(yaml_file=deploy_temp_file.name)
            logger.info(
                f"Created worker deployment: {self.worker_name} with {replicas} replicas"
            )
            return True
        except CommandFailed as e:
            if "already exists" in str(e):
                logger.info(
                    f"Worker deployment already exists, scaling to {replicas} replicas"
                )
                return self.scale_workers(replicas)
            logger.error(f"Failed to create workers: {e}")
            raise

    def start_workload(
        self,
        benchmark_config=None,
        worker_replicas=5,
        timeout=300,
        image=None,
        server_image=None,
        worker_image=None,
        server_resource_limits=None,
        worker_resource_limits=None,
    ):
        """
        Start the complete GOSBench workload.

        Args:
            benchmark_config (dict): Custom benchmark configuration
            worker_replicas (int): Number of worker replicas
            timeout (int): Timeout in seconds to wait for pods to be ready
            image (str): Container image to use for both server and worker (default: ghcr.io/mulbc/gosbench:latest)
            server_image (str): Container image specifically for server (overrides image if specified)
            worker_image (str): Container image specifically for worker (overrides image if specified)
            server_resource_limits (dict): Resource limits for server pods
            worker_resource_limits (dict): Resource limits for worker pods

        Returns:
            bool: True if workload started successfully
        """
        logger.info(f"Starting GOSBench workload: {self.workload_name}")

        try:
            # 1. Create credentials secret
            self.create_credentials_secret()

            # 2. Create S3 bucket
            self.create_s3_bucket()

            # 3. Create workload configuration
            self.create_workload_config(benchmark_config)

            # 4. Create server deployment and service
            # Use server_image if specified, otherwise fall back to image
            actual_server_image = server_image or image
            self.create_server_deployment(
                image=actual_server_image, resource_limits=server_resource_limits
            )

            # 4.5. Wait for server to be ready before creating workers
            logger.info("Waiting for server to be ready before creating workers...")
            self.wait_for_server_ready(timeout=120)

            # 5. Create worker deployment
            # Use worker_image if specified, otherwise fall back to image
            actual_worker_image = worker_image or image
            self.create_worker_deployment(
                replicas=worker_replicas,
                image=actual_worker_image,
                resource_limits=worker_resource_limits,
            )

            # 6. Wait for pods to be ready
            self.wait_for_workload_ready(timeout)

            logger.info(f"GOSBench workload {self.workload_name} started successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to start workload: {e}")
            raise

    def get_benchmark_results(self):
        """
        Extract and format benchmark results from completed GOSBENCH pods.

        Returns:
            dict: Formatted benchmark statistics
        """
        logger.info("Extracting benchmark results from completed pods...")

        results = {
            "workload_name": self.workload_name,
            "namespace": self.namespace,
            "server_results": {},
            "worker_results": [],
            "summary": {},
        }

        try:
            pod_ocp = OCP(kind="Pod", namespace=self.namespace)

            # Get server pod logs
            server_pods = get_pods_having_label(
                label=f"app={self.server_name}", namespace=self.namespace
            )

            if server_pods:
                server_pod_name = server_pods[0]["metadata"]["name"]

                try:
                    # Get server logs (including previous if pod restarted)
                    server_logs = pod_ocp.exec_oc_cmd(
                        f"logs {server_pod_name} --previous", out_yaml_format=False
                    )
                except CommandFailed:
                    # If no previous logs, get current logs
                    try:
                        server_logs = pod_ocp.exec_oc_cmd(
                            f"logs {server_pod_name}", out_yaml_format=False
                        )
                    except CommandFailed:
                        server_logs = "No server logs available"

                results["server_results"] = self._parse_server_logs(server_logs)

            # Get worker pod logs
            worker_pods = get_pods_having_label(
                label=f"app={self.worker_name}", namespace=self.namespace
            )

            for worker_pod in worker_pods:
                worker_name = worker_pod["metadata"]["name"]
                try:
                    worker_logs = pod_ocp.exec_oc_cmd(
                        f"logs {worker_name} --previous", out_yaml_format=False
                    )
                except CommandFailed:
                    try:
                        worker_logs = pod_ocp.exec_oc_cmd(
                            f"logs {worker_name}", out_yaml_format=False
                        )
                    except CommandFailed:
                        worker_logs = "No logs available"

                worker_result = self._parse_worker_logs(worker_logs, worker_name)
                results["worker_results"].append(worker_result)

            # Generate summary
            results["summary"] = self._generate_summary(results)

        except Exception as e:
            logger.warning(f"Could not extract all benchmark results: {e}")
            results["error"] = str(e)

        return results

    def _parse_server_logs(self, logs):
        """Parse server logs to extract benchmark metrics."""
        metrics = {
            "status": "unknown",
            "start_time": None,
            "end_time": None,
            "duration": None,
            "connections": 0,
            "phases_completed": [],
            "object_stats": {
                "total_operations": 0,
                "put_operations": 0,
                "get_operations": 0,
                "delete_operations": 0,
                "put_rate": 0,
                "get_rate": 0,
                "delete_rate": 0,
                "avg_latency": 0,
                "throughput_mb_s": 0,
                "errors": 0,
                "success_rate": "100%",
            },
            "benchmark_phases": [],
        }

        if not logs or logs == "No server logs available":
            return metrics

        lines = logs.split("\n")
        for line in lines:
            if "Ready to accept connections" in line:
                metrics["status"] = "started"
                metrics["start_time"] = self._extract_timestamp(line)
            elif (
                "All performance tests finished" in line
                or "Benchmark completed" in line
            ):
                metrics["status"] = "completed"
                metrics["end_time"] = self._extract_timestamp(line)
            elif "connected to us" in line:
                metrics["connections"] += 1
            elif "level=info" in line and (
                "put" in line.lower()
                or "get" in line.lower()
                or "delete" in line.lower()
            ):
                metrics["phases_completed"].append(line.strip())

            # Parse object statistics
            if "PUT operations:" in line or "put operations:" in line:
                self._parse_operation_stats(line, "PUT", metrics["object_stats"])
                # Also parse rate from the same line if present
                if "Rate:" in line and "ops/sec" in line:
                    self._parse_rate_stats(line, metrics["object_stats"], "PUT")
            elif "GET operations:" in line or "get operations:" in line:
                self._parse_operation_stats(line, "GET", metrics["object_stats"])
                # Also parse rate from the same line if present
                if "Rate:" in line and "ops/sec" in line:
                    self._parse_rate_stats(line, metrics["object_stats"], "GET")
            elif "DELETE operations:" in line or "delete operations:" in line:
                self._parse_operation_stats(line, "DELETE", metrics["object_stats"])
                # Also parse rate from the same line if present
                if "Rate:" in line and "ops/sec" in line:
                    self._parse_rate_stats(line, metrics["object_stats"], "DELETE")
            elif "Rate:" in line and "ops/sec" in line:
                self._parse_rate_stats(line, metrics["object_stats"])
            elif "Throughput:" in line and ("MB/s" in line or "MiB/s" in line):
                self._parse_throughput_stats(line, metrics["object_stats"])
            elif "Average latency:" in line or "Avg latency:" in line:
                self._parse_latency_stats(line, metrics["object_stats"])
            elif "Errors:" in line and ("/" in line or "%" in line):
                self._parse_error_stats(line, metrics["object_stats"])
            elif "phase started" in line.lower() or "phase:" in line.lower():
                metrics["benchmark_phases"].append(line.strip())

        # Calculate total operations
        stats = metrics["object_stats"]
        stats["total_operations"] = (
            stats["put_operations"]
            + stats["get_operations"]
            + stats["delete_operations"]
        )

        # Calculate duration if we have both timestamps
        if metrics["start_time"] and metrics["end_time"]:
            try:
                from datetime import datetime

                start = datetime.fromisoformat(
                    metrics["start_time"].replace("Z", "+00:00")
                )
                end = datetime.fromisoformat(metrics["end_time"].replace("Z", "+00:00"))
                metrics["duration"] = str(end - start)
            except (ValueError, TypeError):
                pass

        return metrics

    def _parse_operation_stats(self, line, operation_type, stats):
        """Parse operation count from log line."""
        try:
            # Format: "PUT operations: 1000, Rate: 45.5 ops/sec"
            if "operations:" in line:
                parts = line.split("operations:")
                if len(parts) > 1:
                    # Extract the number after "operations:"
                    count_part = parts[1].split(",")[0].strip()
                    count = int(count_part)

                    if operation_type == "PUT":
                        stats["put_operations"] = max(stats["put_operations"], count)
                    elif operation_type == "GET":
                        stats["get_operations"] = max(stats["get_operations"], count)
                    elif operation_type == "DELETE":
                        stats["delete_operations"] = max(
                            stats["delete_operations"], count
                        )
        except (ValueError, IndexError, KeyError):
            pass

    def _parse_rate_stats(self, line, stats, operation_type=None):
        """Parse rate statistics from log line."""
        try:
            # Format: "Rate: 45.5 ops/sec"
            if "Rate:" in line and "ops/sec" in line:
                rate_part = line.split("Rate:")[1].split("ops/sec")[0].strip()
                rate = float(rate_part)

                # Use provided operation type or determine from context
                if operation_type == "PUT":
                    stats["put_rate"] = max(stats["put_rate"], rate)
                elif operation_type == "GET":
                    stats["get_rate"] = max(stats["get_rate"], rate)
                elif operation_type == "DELETE":
                    stats["delete_rate"] = max(stats["delete_rate"], rate)
                elif "PUT" in line.upper() or "put" in line.lower():
                    stats["put_rate"] = max(stats["put_rate"], rate)
                elif "GET" in line.upper() or "get" in line.lower():
                    stats["get_rate"] = max(stats["get_rate"], rate)
                elif "DELETE" in line.upper() or "delete" in line.lower():
                    stats["delete_rate"] = max(stats["delete_rate"], rate)
        except (ValueError, IndexError, KeyError):
            pass

    def _parse_throughput_stats(self, line, stats):
        """Parse throughput statistics from log line."""
        try:
            # Format: "Throughput: 2.3 MB/s" or "Throughput: 2.3 MiB/s"
            if "Throughput:" in line:
                throughput_part = line.split("Throughput:")[1]
                if "MB/s" in throughput_part:
                    throughput = float(throughput_part.split("MB/s")[0].strip())
                elif "MiB/s" in throughput_part:
                    throughput = float(throughput_part.split("MiB/s")[0].strip())
                else:
                    return
                stats["throughput_mb_s"] = max(stats["throughput_mb_s"], throughput)
        except (ValueError, IndexError, KeyError):
            pass

    def _parse_latency_stats(self, line, stats):
        """Parse latency statistics from log line."""
        try:
            # Format: "Average latency: 123ms" or "Avg latency: 123ms"
            if "latency:" in line.lower():
                latency_part = line.lower().split("latency:")[1]
                if "ms" in latency_part:
                    latency = float(latency_part.split("ms")[0].strip())
                    stats["avg_latency"] = max(stats["avg_latency"], latency)
        except (ValueError, IndexError, KeyError):
            pass

    def _parse_error_stats(self, line, stats):
        """Parse error statistics from log line."""
        try:
            # Format: "Errors: 0/1000 (0%)" or "Errors: 5 (0.5%)"
            if "Errors:" in line:
                error_part = line.split("Errors:")[1].strip()
                if "/" in error_part:
                    # Format: "0/1000 (0%)"
                    error_count = int(error_part.split("/")[0].strip())
                    stats["errors"] = max(stats["errors"], error_count)
                    if "(" in error_part and "%" in error_part:
                        success_rate = error_part.split("(")[1].split(")")[0]
                        stats["success_rate"] = success_rate
                elif "(" in error_part and "%" in error_part:
                    # Format: "5 (0.5%)"
                    error_count = int(error_part.split("(")[0].strip())
                    stats["errors"] = max(stats["errors"], error_count)
                    success_rate = error_part.split("(")[1].split(")")[0]
                    stats["success_rate"] = success_rate
        except (ValueError, IndexError, KeyError):
            pass

    def _parse_worker_logs(self, logs, worker_name):
        """Parse worker logs to extract performance metrics."""
        metrics = {
            "worker_name": worker_name,
            "status": "unknown",
            "prometheus_port": None,
            "connections": [],
            "errors": [],
        }

        if not logs or logs == "No logs available":
            return metrics

        lines = logs.split("\n")
        for line in lines:
            if "Starting Prometheus Exporter" in line:
                metrics["status"] = "started"
                if "port" in line:
                    try:
                        port = line.split("port ")[1].split()[0]
                        metrics["prometheus_port"] = port
                    except (IndexError, ValueError):
                        pass
            elif "Could not connect to the server" in line:
                metrics["errors"].append(line.strip())
            elif "connected to" in line or "connection" in line.lower():
                metrics["connections"].append(line.strip())

        # Determine final status
        if "error" in logs.lower() and "Could not connect" in logs:
            metrics["status"] = "connection_failed"
        elif any("completed" in line.lower() for line in lines):
            metrics["status"] = "completed"
        elif metrics["status"] == "started":
            metrics["status"] = "running"

        return metrics

    def _extract_timestamp(self, log_line):
        """Extract timestamp from log line."""
        try:
            # Format: time="2025-09-24T14:27:53Z"
            if 'time="' in log_line:
                start = log_line.find('time="') + 6
                end = log_line.find('"', start)
                return log_line[start:end]
        except (ValueError, IndexError):
            pass
        return None

    def _generate_summary(self, results):
        """Generate a summary of the benchmark results."""
        summary = {
            "total_workers": len(results["worker_results"]),
            "successful_workers": 0,
            "failed_workers": 0,
            "server_status": results["server_results"].get("status", "unknown"),
            "total_connections": results["server_results"].get("connections", 0),
        }

        for worker in results["worker_results"]:
            if worker["status"] == "completed":
                summary["successful_workers"] += 1
            elif "failed" in worker["status"] or "error" in worker["status"]:
                summary["failed_workers"] += 1

        return summary

    def print_benchmark_results(self):
        """
        Extract and print formatted benchmark results.
        """
        logger.info("📊 Extracting and displaying GOSBENCH benchmark results...")

        results = self.get_benchmark_results()

        print("\n" + "=" * 80)
        print("🏆 GOSBENCH BENCHMARK RESULTS")
        print("=" * 80)
        print(f"📋 Workload: {results['workload_name']}")
        print(f"📍 Namespace: {results['namespace']}")
        print()

        # Server Results
        server = results["server_results"]
        print("🖥️  SERVER RESULTS:")
        print(f"   Status: {server.get('status', 'unknown').upper()}")
        if server.get("start_time"):
            print(f"   Start Time: {server['start_time']}")
        if server.get("end_time"):
            print(f"   End Time: {server['end_time']}")
        if server.get("duration"):
            print(f"   Duration: {server['duration']}")
        print(f"   Worker Connections: {server.get('connections', 0)}")

        # Object Statistics
        obj_stats = server.get("object_stats", {})
        if obj_stats.get("total_operations", 0) > 0:
            print("\n📊 OBJECT STATISTICS:")
            print(f"   Total Operations: {obj_stats.get('total_operations', 0):,}")
            if obj_stats.get("put_operations", 0) > 0:
                print(f"   PUT Operations: {obj_stats.get('put_operations', 0):,}")
                if obj_stats.get("put_rate", 0) > 0:
                    print(f"     Rate: {obj_stats.get('put_rate', 0):.2f} ops/sec")
            if obj_stats.get("get_operations", 0) > 0:
                print(f"   GET Operations: {obj_stats.get('get_operations', 0):,}")
                if obj_stats.get("get_rate", 0) > 0:
                    print(f"     Rate: {obj_stats.get('get_rate', 0):.2f} ops/sec")
            if obj_stats.get("delete_operations", 0) > 0:
                print(
                    f"   DELETE Operations: {obj_stats.get('delete_operations', 0):,}"
                )
                if obj_stats.get("delete_rate", 0) > 0:
                    print(f"     Rate: {obj_stats.get('delete_rate', 0):.2f} ops/sec")

            if obj_stats.get("throughput_mb_s", 0) > 0:
                print(f"   Throughput: {obj_stats.get('throughput_mb_s', 0):.2f} MB/s")
            if obj_stats.get("avg_latency", 0) > 0:
                print(f"   Average Latency: {obj_stats.get('avg_latency', 0):.2f} ms")
            if obj_stats.get("errors", 0) > 0:
                print(f"   Errors: {obj_stats.get('errors', 0)}")
            print(f"   Success Rate: {obj_stats.get('success_rate', '100%')}")

        if server.get("benchmark_phases"):
            print("\n   Benchmark Phases:")
            for phase in server["benchmark_phases"]:
                print(f"     • {phase}")
        elif server.get("phases_completed"):
            print("\n   Completed Phases:")
            for phase in server["phases_completed"]:
                print(f"     • {phase}")
        print()

        # Worker Results
        print("👷 WORKER RESULTS:")
        summary = results["summary"]
        print(f"   Total Workers: {summary.get('total_workers', 0)}")
        print(f"   ✅ Successful: {summary.get('successful_workers', 0)}")
        print(f"   ❌ Failed: {summary.get('failed_workers', 0)}")
        print()

        for worker in results["worker_results"]:
            status_emoji = (
                "✅"
                if worker["status"] == "completed"
                else "❌" if "failed" in worker["status"] else "⚠️"
            )
            print(
                f"   {status_emoji} {worker['worker_name']}: {worker['status'].upper()}"
            )
            if worker.get("prometheus_port"):
                print(f"      Metrics Port: {worker['prometheus_port']}")
            if worker.get("errors"):
                print(f"      Errors: {len(worker['errors'])}")

        print()
        print("📈 SUMMARY:")
        print(f"   Server Status: {summary.get('server_status', 'unknown').upper()}")
        print(
            f"   Worker Success Rate: {summary.get('successful_workers', 0)}/{summary.get('total_workers', 0)} workers"
        )
        if summary.get("total_connections", 0) > 0:
            print(f"   Total Connections: {summary['total_connections']}")

        # Add object statistics summary
        obj_stats = server.get("object_stats", {})
        if obj_stats.get("total_operations", 0) > 0:
            print(f"   Total S3 Operations: {obj_stats.get('total_operations', 0):,}")
            # Calculate overall rate if we have individual rates
            total_rate = (
                obj_stats.get("put_rate", 0)
                + obj_stats.get("get_rate", 0)
                + obj_stats.get("delete_rate", 0)
            )
            if total_rate > 0:
                print(f"   Overall Rate: {total_rate:.2f} ops/sec")
            if obj_stats.get("throughput_mb_s", 0) > 0:
                print(
                    f"   Peak Throughput: {obj_stats.get('throughput_mb_s', 0):.2f} MB/s"
                )

        print("=" * 80)
        print()

        return results

    def stop_workload(self, delete_bucket=True, grace_period=5):
        """
        Stop and cleanup the GOSBench workload.

        Args:
            delete_bucket (bool): Whether to delete the S3 bucket (default: True)
            grace_period (int): Seconds to wait before deleting bucket to allow
                               in-flight operations to complete (default: 5)

        Returns:
            bool: True if workload stopped successfully
        """
        logger.info(f"Stopping GOSBench workload: {self.workload_name}")

        try:
            # Extract and print results before cleanup
            try:
                self.print_benchmark_results()
            except Exception as e:
                logger.warning(f"Could not extract benchmark results: {e}")

            # Continue with cleanup
            # Delete deployments
            deploy_ocp = OCP(kind="Deployment", namespace=self.namespace)
            try:
                deploy_ocp.delete(resource_name=self.server_name)
                logger.info(f"Deleted server deployment: {self.server_name}")
            except CommandFailed:
                logger.info(f"Server deployment {self.server_name} not found")

            try:
                deploy_ocp.delete(resource_name=self.worker_name)
                logger.info(f"Deleted worker deployment: {self.worker_name}")
            except CommandFailed:
                logger.info(f"Worker deployment {self.worker_name} not found")

            # Delete service
            svc_ocp = OCP(kind="Service", namespace=self.namespace)
            try:
                svc_ocp.delete(resource_name=self.service_name)
                logger.info(f"Deleted service: {self.service_name}")
            except CommandFailed:
                logger.info(f"Service {self.service_name} not found")

            # Delete ConfigMap
            cm_ocp = OCP(kind="ConfigMap", namespace=self.namespace)
            try:
                cm_ocp.delete(resource_name=self.config_name)
                logger.info(f"Deleted ConfigMap: {self.config_name}")
            except CommandFailed:
                logger.info(f"ConfigMap {self.config_name} not found")

            # Delete secret
            secret_ocp = OCP(kind="Secret", namespace=self.namespace)
            try:
                secret_ocp.delete(resource_name=self.secret_name)
                logger.info(f"Deleted secret: {self.secret_name}")
            except CommandFailed:
                logger.info(f"Secret {self.secret_name} not found")

            # Delete S3 bucket with optional grace period
            if delete_bucket:
                if grace_period > 0:
                    import time

                    logger.info(
                        f"Waiting {grace_period}s grace period before deleting bucket "
                        f"to allow in-flight operations to complete..."
                    )
                    time.sleep(grace_period)
                self.delete_s3_bucket()
            else:
                bucket_name = f"{self.workload_name}-bucket"
                logger.info(
                    f"Skipping bucket deletion (delete_bucket=False). "
                    f"Bucket '{bucket_name}' will remain for inspection."
                )

            logger.info(f"GOSBench workload {self.workload_name} stopped successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to stop workload: {e}")
            raise

    def cleanup_workload(self):
        """
        Clean up all resources associated with the GOSBench workload.

        This method is called by the test framework for additional cleanup
        beyond what stop_workload() does.

        Returns:
            bool: True if cleanup was successful
        """
        logger.info(f"Cleaning up GOSBench workload: {self.workload_name}")

        try:
            # stop_workload() already handles all cleanup, but we can add
            # any additional cleanup logic here if needed in the future
            return True

        except Exception as e:
            logger.error(f"Failed to cleanup workload: {e}")
            return False

    def scale_workers(self, replicas):
        """
        Scale worker deployment to specified number of replicas.

        Args:
            replicas (int): Number of worker replicas

        Returns:
            bool: True if scaling successful
        """
        try:
            deploy_ocp = OCP(kind="Deployment", namespace=self.namespace)
            deploy_ocp.patch(
                resource_name=self.worker_name,
                params={"spec": {"replicas": replicas}},
                format_type="merge",
            )
            logger.info(f"Scaled workers to {replicas} replicas")
            return True
        except CommandFailed as e:
            logger.error(f"Failed to scale workers: {e}")
            raise

    def wait_for_server_ready(self, timeout=120):
        """
        Wait for the GOSBENCH server to be ready.

        Fixed: Replaced OCP.wait_for_resource() which was looking for non-existent
        'STATUS' column in Deployment output, causing ValueError. Now uses proper
        pod-based readiness checks and deployment status verification.

        Args:
            timeout (int): Timeout in seconds

        Returns:
            bool: True if server is ready
        """
        logger.info(f"Waiting for GOSBENCH server {self.server_name} to be ready...")

        try:
            # Wait for server pods to be running using label selector
            import time

            start_time = time.time()
            while time.time() - start_time < timeout:
                server_pods = get_pods_having_label(
                    label=f"app={self.server_name}", namespace=self.namespace
                )

                if server_pods:
                    logger.info(
                        f"Found {len(server_pods)} server pod(s), waiting for them to be running..."
                    )
                    pod_names = [pod["metadata"]["name"] for pod in server_pods]
                    wait_for_pods_to_be_running(
                        namespace=self.namespace, pod_names=pod_names, timeout=60
                    )
                    logger.info("GOSBENCH server pods are running")

                    # Additional check: verify deployment is available
                    deploy_ocp = OCP(kind="Deployment", namespace=self.namespace)
                    deployment_info = deploy_ocp.get(resource_name=self.server_name)

                    available_replicas = deployment_info.get("status", {}).get(
                        "availableReplicas", 0
                    )
                    desired_replicas = deployment_info.get("spec", {}).get(
                        "replicas", 1
                    )

                    if available_replicas >= desired_replicas:
                        logger.info(
                            f"GOSBENCH server deployment is ready: "
                            f"{available_replicas}/{desired_replicas} replicas available"
                        )
                        return True
                    else:
                        logger.info(
                            f"Deployment not fully available yet: "
                            f"{available_replicas}/{desired_replicas} replicas"
                        )
                        time.sleep(5)
                else:
                    logger.info("No server pods found yet, waiting...")
                    time.sleep(5)

            logger.error(f"Server readiness check timed out after {timeout} seconds")
            return False

        except Exception as e:
            logger.error(f"Server readiness check failed: {e}")
            raise

    def wait_for_workload_ready(self, timeout=300):
        """
        Wait for workload pods to be ready.

        Args:
            timeout (int): Timeout in seconds

        Returns:
            bool: True if all pods are ready
        """
        logger.info("Waiting for GOSBench pods to be ready...")

        # Wait for server pod
        server_pods = get_pods_having_label(
            label=f"app={self.server_name}", namespace=self.namespace
        )
        if server_pods:
            wait_for_pods_to_be_running(
                pod_names=[pod["metadata"]["name"] for pod in server_pods],
                namespace=self.namespace,
                timeout=timeout,
            )

        # Wait for worker pods
        worker_pods = get_pods_having_label(
            label=f"app={self.worker_name}", namespace=self.namespace
        )
        if worker_pods:
            wait_for_pods_to_be_running(
                pod_names=[pod["metadata"]["name"] for pod in worker_pods],
                namespace=self.namespace,
                timeout=timeout,
            )

        logger.info("All GOSBench pods are ready")
        return True

    def run_benchmark(self, timeout=3600):
        """
        Execute the benchmark run.

        Args:
            timeout (int): Timeout in seconds for benchmark completion

        Returns:
            str: Benchmark results
        """
        logger.info("Starting benchmark run...")

        try:
            # Get server pod and deployment to check image type
            server_pods = get_pods_having_label(
                label=f"app={self.server_name}", namespace=self.namespace
            )
            if not server_pods:
                raise Exception("No server pods found")

            server_pod_name = server_pods[0]["metadata"]["name"]

            # Get deployment to check image type
            deploy_ocp = OCP(kind="Deployment", namespace=self.namespace)
            deployment = deploy_ocp.get(resource_name=self.server_name)
            container_image = deployment["spec"]["template"]["spec"]["containers"][
                0
            ].get("image", "")

            # Check if using goroom images
            if "goroom-server" in container_image or "goroom-worker" in container_image:
                logger.info("Detected goroom image - triggering benchmark via HTTP API")
                # For goroom images, trigger benchmark via HTTP POST to control API
                return self._trigger_benchmark_http(server_pod_name, timeout)
            else:
                # For standard gosbench image, use client run command
                logger.info(
                    "Using standard gosbench image - triggering via client command"
                )
                cmd = f"client run --server {self.service_name}.{self.namespace}.svc.cluster.local:2000"
                pod_ocp = OCP(kind="Pod", namespace=self.namespace)
                result = pod_ocp.exec_oc_cmd(
                    f"exec {server_pod_name} -- {cmd}",
                    out_yaml_format=False,
                    timeout=timeout,
                )

                logger.info("Benchmark completed successfully")
                return result

        except Exception as e:
            logger.error(f"Benchmark execution failed: {e}")
            raise

    def _trigger_benchmark_http(self, server_pod_name, timeout=3600):
        """
        Trigger benchmark via HTTP POST to goroom-server control API.

        Args:
            server_pod_name (str): Name of the server pod
            timeout (int): Timeout in seconds

        Returns:
            str: Benchmark results or status
        """
        import time

        logger.info(f"Triggering benchmark via HTTP POST to {server_pod_name}")

        try:
            pod_ocp = OCP(kind="Pod", namespace=self.namespace)

            # Trigger benchmark via HTTP POST to /start endpoint
            trigger_cmd = (
                "curl -X POST http://localhost:2000/start "
                "-H 'Content-Type: application/json' "
                "-d '{}'"
            )

            try:
                result = pod_ocp.exec_oc_cmd(
                    f'exec {server_pod_name} -- sh -c "{trigger_cmd}"',
                    out_yaml_format=False,
                    timeout=30,
                )
                logger.info(f"Benchmark trigger response: {result}")
            except Exception as e:
                # If curl or /start endpoint doesn't exist, the benchmark might auto-start
                logger.warning(f"Could not trigger via HTTP POST: {e}")
                logger.info(
                    "Benchmark might auto-start with goroom images - monitoring progress..."
                )

            # Monitor benchmark progress by checking logs
            start_time = time.time()
            last_log_check = 0
            benchmark_started = False
            benchmark_completed = False

            while time.time() - start_time < timeout:
                # Check logs every 10 seconds to avoid spam
                if time.time() - last_log_check < 10:
                    time.sleep(1)
                    continue

                last_log_check = time.time()

                try:
                    # Get recent logs from server pod
                    logs = pod_ocp.exec_oc_cmd(
                        f"logs {server_pod_name} --tail=50",
                        out_yaml_format=False,
                    )

                    # Check for benchmark activity in logs
                    if not benchmark_started:
                        if any(
                            keyword in logs.lower()
                            for keyword in [
                                "benchmark",
                                "starting",
                                "workers connected",
                                "put operation",
                                "get operation",
                                "starting stage",
                            ]
                        ):
                            benchmark_started = True
                            logger.info(
                                "✓ Benchmark has started - monitoring progress..."
                            )

                    # Check for completion indicators
                    if benchmark_started and any(
                        keyword in logs.lower()
                        for keyword in [
                            "completed",
                            "finished",
                            "benchmark done",
                            "all stages complete",
                        ]
                    ):
                        benchmark_completed = True
                        logger.info("✓ Benchmark appears to have completed")
                        break

                    # Check for errors
                    if "error" in logs.lower() or "failed" in logs.lower():
                        logger.warning(
                            "Detected error messages in logs, but continuing to monitor..."
                        )

                except Exception as e:
                    logger.debug(f"Could not check logs: {e}")

                time.sleep(1)

            if benchmark_completed:
                logger.info("Benchmark execution completed successfully")
                return "Benchmark completed (monitored via logs)"
            elif benchmark_started:
                logger.warning(
                    f"Benchmark started but may not have completed within {timeout}s"
                )
                return "Benchmark started (monitoring timed out)"
            else:
                logger.warning(
                    "Could not confirm benchmark started. For goroom images, "
                    "the benchmark should auto-start when workers connect. "
                    "Check server and worker pod logs for details."
                )
                return "Benchmark trigger attempted (status uncertain)"

        except Exception as e:
            logger.error(f"Failed to trigger/monitor benchmark: {e}")
            raise

    def get_workload_status(self):
        """
        Get current status of the workload.

        Returns:
            dict: Status information
        """
        status = {
            "server": {"deployment": "NotFound", "pods": []},
            "workers": {"deployment": "NotFound", "pods": []},
            "config": "NotFound",
            "secret": "NotFound",  # pragma: allowlist secret
        }

        try:
            # Check deployments
            deploy_ocp = OCP(kind="Deployment", namespace=self.namespace)

            try:
                server_deploy = deploy_ocp.get(resource_name=self.server_name)
                status["server"]["deployment"] = server_deploy["status"]
            except CommandFailed:
                pass

            try:
                worker_deploy = deploy_ocp.get(resource_name=self.worker_name)
                status["workers"]["deployment"] = worker_deploy["status"]
            except CommandFailed:
                pass

            # Check pods
            server_pods = get_pods_having_label(
                label=f"app={self.server_name}", namespace=self.namespace
            )
            status["server"]["pods"] = [
                {"name": pod["metadata"]["name"], "status": pod["status"]["phase"]}
                for pod in server_pods
            ]

            worker_pods = get_pods_having_label(
                label=f"app={self.worker_name}", namespace=self.namespace
            )
            status["workers"]["pods"] = [
                {"name": pod["metadata"]["name"], "status": pod["status"]["phase"]}
                for pod in worker_pods
            ]

            # Check ConfigMap
            try:
                cm_ocp = OCP(kind="ConfigMap", namespace=self.namespace)
                cm_ocp.get(resource_name=self.config_name)
                status["config"] = "Found"
            except CommandFailed:
                pass

            # Check Secret
            try:
                secret_ocp = OCP(kind="Secret", namespace=self.namespace)
                secret_ocp.get(resource_name=self.secret_name)
                status["secret"] = "Found"  # pragma: allowlist secret
            except CommandFailed:
                pass

        except Exception as e:
            logger.error(f"Failed to get workload status: {e}")

        # Add convenience fields for validation
        status["server_ready"] = len(status["server"]["pods"]) > 0 and all(
            pod["status"] == "Running" for pod in status["server"]["pods"]
        )
        status["worker_count"] = len(status["workers"]["pods"])

        return status

    def _deep_merge(self, base_dict, update_dict):
        """
        Deep merge two dictionaries.

        Args:
            base_dict (dict): Base dictionary to merge into
            update_dict (dict): Dictionary to merge from
        """
        for key, value in update_dict.items():
            if (
                key in base_dict
                and isinstance(base_dict[key], dict)
                and isinstance(value, dict)
            ):
                self._deep_merge(base_dict[key], value)
            else:
                base_dict[key] = value


# Convenience functions for easy usage
def start_gosbench_workload(
    workload_name="gosbench",
    namespace=None,
    benchmark_config=None,
    worker_replicas=5,
    timeout=300,
    image=None,
    server_resource_limits=None,
    worker_resource_limits=None,
):
    """
    Start a GOSBench workload with default configuration.

    Args:
        workload_name (str): Name for the workload
        namespace (str): Kubernetes namespace
        benchmark_config (dict): Custom benchmark configuration
        worker_replicas (int): Number of worker replicas
        timeout (int): Timeout for pod readiness
        image (str): Container image to use
        server_resource_limits (dict): Resource limits for server pods
        worker_resource_limits (dict): Resource limits for worker pods

    Returns:
        GOSBenchWorkload: Workload instance
    """
    workload = GOSBenchWorkload(namespace=namespace, workload_name=workload_name)
    workload.start_workload(
        benchmark_config=benchmark_config,
        worker_replicas=worker_replicas,
        timeout=timeout,
        image=image,
        server_resource_limits=server_resource_limits,
        worker_resource_limits=worker_resource_limits,
    )
    return workload


def stop_gosbench_workload(workload_name="gosbench", namespace=None):
    """
    Stop a GOSBench workload.

    Args:
        workload_name (str): Name of the workload to stop
        namespace (str): Kubernetes namespace

    Returns:
        bool: True if stopped successfully
    """
    workload = GOSBenchWorkload(namespace=namespace, workload_name=workload_name)
    return workload.stop_workload()


def modify_gosbench_config(
    workload_name="gosbench", namespace=None, benchmark_config=None
):
    """
    Modify GOSBench workload configuration.

    Args:
        workload_name (str): Name of the workload
        namespace (str): Kubernetes namespace
        benchmark_config (dict): New benchmark configuration

    Returns:
        bool: True if configuration updated successfully
    """
    workload = GOSBenchWorkload(namespace=namespace, workload_name=workload_name)
    return workload.update_workload_config(benchmark_config)


def print_gosbench_results(workload_name="gosbench", namespace=None):
    """
    Print benchmark results for an existing GOSBench workload.

    Args:
        workload_name (str): Name of the workload
        namespace (str): Kubernetes namespace

    Returns:
        dict: Benchmark results dictionary
    """
    workload = GOSBenchWorkload(namespace=namespace, workload_name=workload_name)
    return workload.print_benchmark_results()
