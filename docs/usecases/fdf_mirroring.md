# FDF Image Mirroring

Guide for mirroring FDF (Fusion Data Foundation) unreleased images from Mgen to a disconnected mirror registry.

## Overview

This guide covers mirroring FDF catalog images and their associated operator images using the `oc-mirror` tool. The process automatically:
- Mirrors the FDF catalog and all related images
- Creates ImageDigestMirrorSet (IDMS) for cluster configuration
- Optionally configures registries.conf for internal image mirrors

## Prerequisites

### 1. Pull Secret (Required)

A pull secret with authentication for both source and destination registries. The pull secret should be located at:
- pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret") (recommended)

The pull secret must include authentication for:

**Source registries** (where images are pulled from):
- `cp.stg.icr.io` - FDF catalog registry
- `registry.redhat.io` - Red Hat base images
- `icr.io/cpopen` - FDF operator images

**Destination registry** (where images are pushed to):
- Your mirror registry

```

### 2. Config File (Optional)

Only needed if you want to override mirror registry credentials. Create a config file (e.g., `mirror_config.yaml`):

```yaml
---
DEPLOYMENT:
  mirror_registry: '<mirror registry>'
  mirror_registry_user: 'your-mirror-username'
  mirror_registry_password: ''
```

**Note:** If your pull secret already contains credentials for the mirror registry, you don't need to provide them in the config file.

### 3. Required Tools

- `oc` CLI tool (OpenShift client)
- `oc-mirror` plugin (automatically installed if not present)
- Access to OCP cluster with kubeconfig

## Quick Start

### CLI Command

**Basic Usage (with all CLI arguments):**
```bash
python -m ocs_ci.framework.fdf_mirror.main \
  --catalog-image <catalog-image-url> \
  --mirror-registry <mirror-registry-url> \
  --mirror-registry-user <username> \
  --mirror-registry-password '<password>' \
  --cluster-path <path-to-cluster-dir> \
  --cluster-name <cluster-name> \
  --configure-registries
```

**With Config File (credentials in config):**
```bash
python -m ocs_ci.framework.fdf_mirror.main \
  --catalog-image <catalog-image-url> \
  --mirror-registry <mirror-registry-url> \
  --cluster-path <path-to-cluster-dir> \
  --ocsci-conf /path/to/config.yaml \
  --configure-registries
```

**Mixed (registry from CLI, credentials from config):**
```bash
python -m ocs_ci.framework.fdf_mirror.main \
  --catalog-image <catalog-image-url> \
  --mirror-registry <mirror-registry-url> \
  --cluster-path <path-to-cluster-dir> \
  --conf /path/to/config.yaml \
  --configure-registries
```

**Required Arguments:**
- `--catalog-image`: FDF catalog image to mirror (required)
- `--mirror-registry`: Target mirror registry URL (required if not in config)
- `--cluster-path`: Path to OCP cluster directory containing auth/kubeconfig (required)

**Optional Arguments:**
- `--cluster-name`: Name of the OCP cluster (optional if metadata.json exists in cluster-path)
- `--mirror-registry-user`: Mirror registry username (optional, can be provided via CLI or config file)
- `--mirror-registry-password`: Mirror registry password (optional, can be provided via CLI or config file)
- `--configure-registries`: Configure /etc/containers/registries.conf for internal FDF images
- `--ocsci-conf` or `--conf`: Path to config file (optional, can be used to provide mirror_registry and credentials). Both arguments are supported and can be used interchangeably or together.
- `--report`: Path for JUnit report output

**Note:** CLI arguments take precedence over config file values. If credentials are not provided via CLI or config, the tool will use credentials from the pull secret.

### Python API

```python
from ocs_ci.deployment.disconnected import mirror_fdf_catalog_via_oc_mirror

# Mirror FDF catalog and images
results = mirror_fdf_catalog_via_oc_mirror(
    catalog_image="cp.stg.icr.io/cp/df/isf-data-foundation-catalog:v4.20",
    mirror_registry="your-registry.com:5000/fdf",
    configure_registries=True  # Optional: configure registries.conf
)
```

**Notes:**
- Use `python -m ocs_ci.framework.fdf_mirror.main` for reliability in Jenkins
- KUBECONFIG must point to cluster kubeconfig
- Pull secret at `${CLUSTER_PATH}/auth/pull-secret` is used automatically
- oc-mirror tool is installed automatically if not present
- IDMS (ImageDigestMirrorSet) is created and applied automatically

## Manual Process

If you prefer to run the mirroring process manually without using the ocs-ci framework:

### 1. Create ImageSetConfiguration File

Create a file named `fdf_isc.yaml`:

```yaml
kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v1alpha2
mirror:
  operators:
    - catalog: <cateloge image>
```

### 2. Configure registries.conf (Optional - For Internal Images)

If you need to mirror internal images, add to `/etc/containers/registries.conf`:

```toml
[[registry]]
location="registry.redhat.io"
[[registry.mirror]]
location="cp.stg.icr.io/cp/df"
mirror-by-digest-only = false
pull-from-mirror = "all"

[[registry]]
location="icr.io/cpopen"
[[registry.mirror]]
location="cp.stg.icr.io/cp/df"
mirror-by-digest-only = false
pull-from-mirror = "all"
short-name-mode = "permissive"

[[registry]]
location="cp.icr.io/cp/df/"
[[registry.mirror]]
location="cp.stg.icr.io/cp/df"
mirror-by-digest-only = false
pull-from-mirror = "all"
short-name-mode = "permissive"
```

### 3. Run oc-mirror Command

```bash
oc mirror \
  --config fdf_isc.yaml \
  docker://<mirror-registry> \
  --workspace file://<workspace-path> \
  --v2 \
  --dest-tls-verify=false \
  --image-timeout 30m
```

**Example:**
```bash
oc mirror \
  --config fdf_isc.yaml \
  <Mirror registry full path> \
  --workspace file://oc-mirror-workspace/results-files \
  --v2 \
  --dest-tls-verify=false \
  --image-timeout 30m
```

**Note:** The `oc-mirror` tool uses `~/.docker/config.json` for authentication. Make sure your pull secret is available at this location.

### 4. Apply ImageDigestMirrorSet

After mirroring completes, apply the generated IDMS to your cluster:

```bash
# Find and apply the generated IDMS
oc apply -f oc-mirror-workspace/results-*/working-dir/cluster-resources/idms-*.yaml

# Wait for MachineConfigPools to be updated
oc wait --for=condition=Updated mcp/worker --timeout=600s
oc wait --for=condition=Updated mcp/master --timeout=600s
```

## FDF Catalog Images by Version

Use the appropriate catalog image for your FDF version:

- **FDF 4.18**: `<registry-path>/isf-data-foundation-catalog:v4.18`
- **FDF 4.19**: `<registry-path>/isf-data-foundation-catalog:v4.19`
- **FDF 4.20**: `<registry-path>/isf-data-foundation-catalog:v4.20`

For specific builds, append the build number:
- Example: `<registry-path>/isf-data-foundation-catalog:v4.18.20-5`

## Templates

The following template files are available in the ocs-ci repository:

- **ImageSetConfiguration**: `ocs_ci/templates/fusion-data-foundation/fdf-imageset-config.yaml`
- **Registries.conf**: `ocs_ci/templates/fusion-data-foundation/registries.conf.template`

These templates are used automatically by the mirroring function.

## Implementation Details

The FDF mirroring functionality is integrated into `ocs_ci/deployment/disconnected.py` and follows the same pattern as existing ODF mirroring:

### Key Features

1. **Automatic oc-mirror Installation**: The tool is automatically downloaded and installed if not present
2. **IDMS Creation**: ImageDigestMirrorSet is automatically created and applied to the cluster
3. **Registry Configuration**: Optionally configures `/etc/containers/registries.conf` for internal images
4. **MachineConfigPool Wait**: Waits for worker and master nodes to be updated after IDMS application
5. **Pull Secret Handling**: Automatically uses pull secret from cluster-path or ~/.docker/config.json

### Function Signature

```python
def mirror_fdf_catalog_via_oc_mirror(
    catalog_image,
    mirror_registry=None,
    configure_registries=False,
):
    """
    Mirror FDF catalog and related images using oc-mirror tool.

    Args:
        catalog_image (str): FDF catalog image URL
        mirror_registry (str): Target mirror registry (optional if in config)
        configure_registries (bool): Configure registries.conf for internal images

    Returns:
        str: Mirrored catalog image URL
    """
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Ensure pull secret contains credentials for all required registries
   - Verify pull secret is at `${CLUSTER_PATH}/auth/pull-secret` or `~/.docker/config.json`

2. **oc-mirror Command Fails**
   - Check network connectivity to source and destination registries
   - Verify mirror registry credentials are correct
   - Increase timeout with `--image-timeout` if images are large

3. **IDMS Not Applied**
   - Check if IDMS was created in `oc-mirror-workspace/results-*/working-dir/cluster-resources/`
   - Manually apply with `oc apply -f idms-*.yaml`
   - Wait for MachineConfigPools to update

4. **Nodes Not Updating**
   - Check MachineConfigPool status: `oc get mcp`
   - View node status: `oc get nodes`
   - Check for errors: `oc describe mcp/worker`

### Debug Mode

For detailed logging, set environment variable:
```bash
export LOG_LEVEL=DEBUG
```
