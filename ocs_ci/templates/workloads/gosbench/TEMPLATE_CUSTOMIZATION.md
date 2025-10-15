# GOSBench Template Customization Guide

This document explains all the customization options available in the GOSBench Jinja2 templates. The templates are highly flexible and support a wide range of deployment scenarios.

## Table of Contents

- [Overview](#overview)
- [Template Variables](#template-variables)
- [Server Deployment Customization](#server-deployment-customization)
- [Worker Deployment Customization](#worker-deployment-customization)
- [Service Customization](#service-customization)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)

## Overview

The GOSBench templates use Jinja2 templating to provide maximum flexibility. All variables have sensible defaults, so you only need to specify the values you want to customize.

### Template Files

- `server-deployment.yaml.j2` - Server deployment configuration
- `worker-deployment.yaml.j2` - Worker deployment configuration
- `server-service.yaml.j2` - Service configuration
- `configmap.yaml.j2` - Configuration map template
- `secret.yaml.j2` - Credentials secret template

## Template Variables

### Basic Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `workload_name` | `"gosbench"` | Name of the workload |
| `namespace` | Required | Kubernetes namespace |
| `version` | `"latest"` | Version label for resources |
| `image` | `"ghcr.io/mulbc/gosbench:latest"` | Container image |
| `image_pull_policy` | None | Image pull policy (Always, IfNotPresent, Never) |

### Labels and Annotations

| Variable | Default | Description |
|----------|---------|-------------|
| `custom_labels` | None | Additional labels for deployments |
| `custom_pod_labels` | None | Additional labels for pods |
| `annotations` | None | Annotations for deployments |
| `custom_pod_annotations` | None | Annotations for pods |

### Resource Configuration

#### Server Resources
| Variable | Default | Description |
|----------|---------|-------------|
| `server_replicas` | `1` | Number of server replicas |
| `server_memory_request` | `"256Mi"` | Memory request |
| `server_memory_limit` | `"512Mi"` | Memory limit |
| `server_cpu_request` | `"100m"` | CPU request |
| `server_cpu_limit` | `"500m"` | CPU limit |
| `server_ephemeral_storage_request` | None | Ephemeral storage request |
| `server_ephemeral_storage_limit` | None | Ephemeral storage limit |

#### Worker Resources
| Variable | Default | Description |
|----------|---------|-------------|
| `replicas` | `5` | Number of worker replicas |
| `worker_memory_request` | `"128Mi"` | Memory request |
| `worker_memory_limit` | `"256Mi"` | Memory limit |
| `worker_cpu_request` | `"50m"` | CPU request |
| `worker_cpu_limit` | `"200m"` | CPU limit |
| `worker_ephemeral_storage_request` | None | Ephemeral storage request |
| `worker_ephemeral_storage_limit` | None | Ephemeral storage limit |

### Node Placement

| Variable | Default | Description |
|----------|---------|-------------|
| `node_selector` | None | Node selector for pod placement |
| `tolerations` | None | Tolerations for tainted nodes |
| `affinity` | None | Pod affinity/anti-affinity rules |

### Security Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `security_context` | None | Pod security context |
| `container_security_context` | None | Container security context |
| `service_account` | None | Service account name |
| `image_pull_secrets` | None | Image pull secrets |

### Network Configuration

#### Ports
| Variable | Default | Description |
|----------|---------|-------------|
| `control_port` | `2000` | Server control port |
| `metrics_port` | `2112` | Metrics port |
| `worker_port` | `8009` | Worker port |
| `extra_ports` | None | Additional ports |

#### Service Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `service_type` | `"ClusterIP"` | Service type |
| `session_affinity` | None | Session affinity |
| `external_traffic_policy` | None | External traffic policy |
| `load_balancer_ip` | None | Load balancer IP |
| `load_balancer_source_ranges` | None | Load balancer source ranges |
| `external_ips` | None | External IPs |

### Health Checks

| Variable | Default | Description |
|----------|---------|-------------|
| `health_checks` | None | Health check configuration |
| `health_checks.liveness` | None | Liveness probe |
| `health_checks.readiness` | None | Readiness probe |
| `health_checks.startup` | None | Startup probe |

### Storage and Volumes

| Variable | Default | Description |
|----------|---------|-------------|
| `extra_volumes` | None | Additional volumes |
| `extra_volume_mounts` | None | Additional volume mounts |

### Environment and Arguments

| Variable | Default | Description |
|----------|---------|-------------|
| `extra_env_vars` | None | Additional environment variables |
| `server_extra_args` | None | Additional server arguments |
| `worker_extra_args` | None | Additional worker arguments |

### Deployment Strategy

| Variable | Default | Description |
|----------|---------|-------------|
| `deployment_strategy` | None | Deployment strategy |
| `restart_policy` | None | Pod restart policy |

### DNS Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `dns_policy` | None | DNS policy |
| `dns_config` | None | DNS configuration |

## Server Deployment Customization

### Basic Example

```python
server_config = {
    "workload_name": "my-gosbench",
    "version": "v1.0.0",
    "server_memory_request": "512Mi",
    "server_memory_limit": "1Gi",
    "server_cpu_request": "200m",
    "server_cpu_limit": "1000m"
}
```

### Advanced Example with Security

```python
secure_server_config = {
    "workload_name": "secure-gosbench",
    "security_context": {
        "runAsNonRoot": True,
        "runAsUser": 1000,
        "fsGroup": 1000
    },
    "container_security_context": {
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "runAsNonRoot": True,
        "capabilities": {
            "drop": ["ALL"]
        }
    },
    "service_account": "gosbench-secure",
    "health_checks": {
        "liveness": {
            "httpGet": {
                "path": "/health",
                "port": 2112
            },
            "initialDelaySeconds": 30,
            "periodSeconds": 10
        },
        "readiness": {
            "httpGet": {
                "path": "/ready",
                "port": 2112
            },
            "initialDelaySeconds": 5,
            "periodSeconds": 5
        }
    }
}
```

## Worker Deployment Customization

### High-Performance Configuration

```python
high_perf_worker_config = {
    "replicas": 20,
    "worker_memory_request": "512Mi",
    "worker_memory_limit": "1Gi",
    "worker_cpu_request": "200m",
    "worker_cpu_limit": "1000m",
    "node_selector": {
        "node-type": "performance",
        "storage-tier": "nvme"
    },
    "affinity": {
        "podAntiAffinity": {
            "preferredDuringSchedulingIgnoredDuringExecution": [{
                "weight": 100,
                "podAffinityTerm": {
                    "labelSelector": {
                        "matchExpressions": [{
                            "key": "component",
                            "operator": "In",
                            "values": ["gosbench-worker"]
                        }]
                    },
                    "topologyKey": "kubernetes.io/hostname"
                }
            }]
        }
    }
}
```

### Multi-Zone Deployment

```python
multi_zone_config = {
    "replicas": 15,  # 5 per zone
    "affinity": {
        "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [{
                "labelSelector": {
                    "matchExpressions": [{
                        "key": "component",
                        "operator": "In",
                        "values": ["gosbench-worker"]
                    }]
                },
                "topologyKey": "topology.kubernetes.io/zone"
            }]
        }
    },
    "tolerations": [{
        "key": "zone",
        "operator": "Exists",
        "effect": "NoSchedule"
    }]
}
```

## Service Customization

### Load Balancer Service

```python
lb_service_config = {
    "service_type": "LoadBalancer",
    "load_balancer_ip": "10.0.0.100",
    "load_balancer_source_ranges": [
        "10.0.0.0/8",
        "172.16.0.0/12"
    ],
    "external_traffic_policy": "Local",
    "session_affinity": "ClientIP",
    "session_affinity_config": {
        "clientIP": {
            "timeoutSeconds": 3600
        }
    }
}
```

### NodePort Service

```python
nodeport_service_config = {
    "service_type": "NodePort",
    "control_node_port": 32000,
    "metrics_node_port": 32112,
    "extra_ports": [{
        "name": "debug",
        "port": 8080,
        "nodePort": 32080
    }]
}
```

## Usage Examples

### Using Templates in Python Code

```python
from ocs_ci.workloads.gosbench_workload import GOSBenchWorkload

# High-performance configuration
high_perf_config = {
    "workload_name": "perf-test",
    "server_memory_request": "1Gi",
    "server_memory_limit": "2Gi",
    "server_cpu_request": "500m",
    "server_cpu_limit": "2000m",
    "replicas": 20,
    "worker_memory_request": "512Mi",
    "worker_memory_limit": "1Gi",
    "worker_cpu_request": "200m",
    "worker_cpu_limit": "1000m",
    "node_selector": {
        "node-type": "performance"
    },
    "health_checks": {
        "liveness": {
            "httpGet": {"path": "/health", "port": 2112},
            "initialDelaySeconds": 30,
            "periodSeconds": 10
        }
    }
}

workload = GOSBenchWorkload(workload_name="perf-test")
workload.start_workload(
    server_resource_limits=high_perf_config,
    worker_resource_limits=high_perf_config
)
```

### Debug Configuration

```python
debug_config = {
    "image": "ghcr.io/mulbc/gosbench:debug",
    "image_pull_policy": "Always",
    "extra_env_vars": [
        {"name": "LOG_LEVEL", "value": "debug"},
        {"name": "ENABLE_PROFILING", "value": "true"}
    ],
    "extra_ports": [
        {"containerPort": 8080, "name": "debug"},
        {"containerPort": 6060, "name": "pprof"}
    ],
    "server_extra_args": ["--debug", "--profile"],
    "worker_extra_args": ["--verbose", "--debug-worker"],
    "service_type": "NodePort",
    "control_node_port": 32000,
    "metrics_node_port": 32112
}
```

### Resource-Constrained Environment

```python
minimal_config = {
    "server_memory_request": "64Mi",
    "server_memory_limit": "128Mi",
    "server_cpu_request": "50m",
    "server_cpu_limit": "100m",
    "worker_memory_request": "32Mi",
    "worker_memory_limit": "64Mi",
    "worker_cpu_request": "25m",
    "worker_cpu_limit": "50m",
    "replicas": 2,
    "deployment_strategy": {
        "type": "RollingUpdate",
        "rollingUpdate": {
            "maxUnavailable": 0,
            "maxSurge": 1
        }
    }
}
```

## Best Practices

### 1. Resource Planning

- **Start Small**: Begin with default resources and scale up based on testing
- **Monitor Usage**: Use metrics to determine optimal resource allocation
- **Set Limits**: Always set resource limits to prevent resource starvation

### 2. Node Placement

- **Use Node Selectors**: Target appropriate nodes for performance testing
- **Anti-Affinity**: Spread workers across nodes for better distribution
- **Tolerations**: Use tolerations for dedicated testing nodes

### 3. Security

- **Non-Root**: Always run containers as non-root users
- **Read-Only**: Use read-only root filesystems when possible
- **Drop Capabilities**: Drop unnecessary Linux capabilities
- **Service Accounts**: Use dedicated service accounts with minimal permissions

### 4. Health Checks

- **Implement Probes**: Always configure liveness and readiness probes
- **Appropriate Timeouts**: Set realistic timeout values
- **Startup Probes**: Use startup probes for slow-starting containers

### 5. Networking

- **Service Types**: Choose appropriate service types for your environment
- **Load Balancing**: Use session affinity when needed
- **Port Configuration**: Avoid port conflicts with other services

### 6. Storage

- **Ephemeral Storage**: Set limits for ephemeral storage usage
- **Volume Mounts**: Use appropriate volume types for different use cases
- **Persistent Storage**: Consider persistent volumes for long-running tests

### 7. Monitoring

- **Labels**: Use consistent labeling for monitoring and alerting
- **Metrics**: Expose metrics on standard ports
- **Annotations**: Use annotations for additional metadata

## Template Validation

The templates include validation for common configuration errors:

- Required fields are validated
- Resource formats are checked
- Port conflicts are detected
- Label and annotation formats are validated

## Extending Templates

To add new customization options:

1. Add the variable to the template with appropriate defaults
2. Update this documentation
3. Add examples to the advanced configuration file
4. Test with various scenarios

## Troubleshooting

### Common Issues

1. **Resource Limits**: Ensure limits are higher than requests
2. **Node Selection**: Verify nodes match selectors and tolerations
3. **Image Pull**: Check image names and pull policies
4. **Port Conflicts**: Ensure ports don't conflict with existing services
5. **Health Checks**: Verify probe endpoints are accessible

### Debug Tips

- Use `kubectl describe` to check resource creation
- Check pod logs for startup issues
- Verify service endpoints are accessible
- Monitor resource usage during tests

This comprehensive customization system makes GOSBench templates extremely flexible for any testing scenario while maintaining simplicity for basic use cases.
