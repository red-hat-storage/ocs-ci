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
- `--ocsci-conf` or `--conf`: Path to config file (optional, can be used to provide mirror_registry and credentials). Both arguments are supported and can be used interchangeably or together. Multiple config files can be specified and will be merged.
- `--report`: Path for JUnit report output (generates JUnit XML format test results)

### Credential Resolution Order

The tool resolves mirror registry credentials using the following priority (highest to lowest):
1. **CLI arguments** (`--mirror-registry-user`, `--mirror-registry-password`) - highest priority
2. **Config file values** (`mirror_registry_user`, `mirror_registry_password` in DEPLOYMENT section)
3. **Pull secret** (credentials extracted from pull-secret file) - fallback

**Note:** CLI arguments always take precedence over config file values. If credentials are not provided via CLI or config, the tool will automatically extract and use credentials from the pull secret.

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
- IDMS (ImageDigestMirrorSet) is created and applied automatically with unique naming (format: `fdf-{run_id}`)
- The mirroring process includes automatic retry logic (3 attempts with exponential backoff)
- Overall command timeout is 5 hours (18000 seconds) for large catalog operations
- Workspace directory: `oc-mirror-workspace/results-{timestamp}` (automatically uses most recent)

## JUnit Reporting

The tool supports generating JUnit XML format test reports for CI/CD integration:

```bash
python -m ocs_ci.framework.fdf_mirror.main \
  --catalog-image <catalog-image-url> \
  --mirror-registry <mirror-registry-url> \
  --cluster-path <path-to-cluster-dir> \
  --report /path/to/report.xml
```

The report includes:
- Test suite properties (cluster name, OCP version, catalog image, etc.)
- Test case results (success/failure status)
- Execution time and timestamps
- Error details if mirroring fails

This is particularly useful for:
- Jenkins/GitLab CI pipeline integration
- Automated test result tracking
- Failure analysis and debugging

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

If you need to mirror internal images, create a new configuration file in `/etc/containers/registries.conf.d/`:

```bash
# Create OCS-CI specific registry configuration
sudo tee /etc/containers/registries.conf.d/ocs-ci-fdf-mirrors.conf > /dev/null <<EOF
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
EOF
```

**Benefits of using `/etc/containers/registries.conf.d/`:**
- Non-destructive: doesn't modify main system configuration
- Easy to track: OCS-CI specific file with clear naming
- Simple cleanup: `sudo rm /etc/containers/registries.conf.d/ocs-ci-fdf-mirrors.conf`
- Easy updates: just replace the file without backup/restore
- Standard practice on RHEL/Fedora systems
- Automatically used by the `--configure-registries` option

**Note:** The automated tool (`--configure-registries` flag) uses this approach automatically. The above manual steps are only needed if you're not using the ocs-ci framework.

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
# Find the most recent results directory
RESULTS_DIR=$(ls -td oc-mirror-workspace/results-* | head -1)
echo "Using results directory: $RESULTS_DIR"

# Apply the generated IDMS
oc apply -f $RESULTS_DIR/working-dir/cluster-resources/idms-oc-mirror.yaml

# Wait for MachineConfigPools to be updated (this can take 10-30 minutes)
oc wait --for=condition=Updated mcp/worker --timeout=1800s
oc wait --for=condition=Updated mcp/master --timeout=1800s

# Monitor the update progress
oc get mcp -w
```

**Note:** The workspace directory structure is:
```
oc-mirror-workspace/
└── results-{timestamp}/
    └── working-dir/
        ├── cluster-resources/
        │   ├── idms-oc-mirror.yaml
        │   └── cs-*.yaml (CatalogSource)
        └── dry-run/
            └── mapping.txt
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
2. **IDMS Creation**: ImageDigestMirrorSet is automatically created and applied to the cluster with unique naming
   - IDMS name format: `fdf-{run_id}` (e.g., `fdf-1234567890`)
   - Ensures no conflicts with existing IDMS resources
3. **Registry Configuration**: Optionally configures registry mirrors for internal images
   - Creates `/etc/containers/registries.conf.d/ocs-ci-fdf-mirrors.conf` (non-destructive approach)
   - Automatically creates the directory if it doesn't exist
   - Template location: `ocs_ci/templates/fusion-data-foundation/registries.conf.template`
4. **MachineConfigPool Wait**: Waits for worker and master nodes to be updated after IDMS application
5. **Pull Secret Handling**: Automatically uses pull secret from cluster-path or ~/.docker/config.json
6. **Retry Logic**: Automatic retry on failure (3 attempts with exponential backoff: 10s, 20s, 40s delays)
7. **Workspace Management**:
   - Creates timestamped result directories: `oc-mirror-workspace/results-{timestamp}`
   - Automatically selects the most recent results directory
   - Preserves previous runs for debugging
8. **JUnit Reporting**: Optional test result reporting in JUnit XML format for CI/CD integration

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
   - Check credential resolution order: CLI args → config file → pull secret
   - Verify credentials work by testing: `podman login <registry> --authfile <pull-secret-path>`

2. **oc-mirror Command Fails**
   - The tool automatically retries 3 times with exponential backoff (10s, 20s, 40s delays)
   - Check network connectivity to source and destination registries
   - Verify mirror registry credentials are correct
   - Increase timeout with `--image-timeout` if images are large (default: 30m)
   - Overall operation timeout is 5 hours (18000 seconds)
   - Check workspace directory for partial results: `oc-mirror-workspace/results-*/`

3. **IDMS Not Applied**
   - Check if IDMS was created in `oc-mirror-workspace/results-*/working-dir/cluster-resources/`
   - IDMS name format: `fdf-{run_id}` (check with `oc get imagedigestmirrorset`)
   - Manually apply with `oc apply -f idms-oc-mirror.yaml`
   - Wait for MachineConfigPools to update (can take 10-30 minutes)

4. **Nodes Not Updating**
   - Check MachineConfigPool status: `oc get mcp`
   - View node status: `oc get nodes`
   - Check for errors: `oc describe mcp/worker`
   - Monitor progress: `oc get mcp -w`

5. **Workspace Directory Issues**
   - Multiple result directories may exist: `oc-mirror-workspace/results-{timestamp}/`
   - Tool automatically uses the most recent directory
   - Previous runs are preserved for debugging
   - Clean up old workspaces if disk space is limited: `rm -rf oc-mirror-workspace/results-*/`

6. **Registry Configuration Fails**
   - Check if `/etc/containers/registries.conf.d/ocs-ci-fdf-mirrors.conf` was created
   - Remove if needed: `sudo rm /etc/containers/registries.conf.d/ocs-ci-fdf-mirrors.conf`
   - Verify template exists: `ocs_ci/templates/fusion-data-foundation/registries.conf.template`
   - Check sudo permissions for creating files in `/etc/containers/registries.conf.d/`
   - The directory is automatically created if it doesn't exist
   - Verify platform compatibility (RHEL 7.6+, RHEL 8+, all OpenShift nodes)

### Debug Mode

For detailed logging, set environment variable:
```bash
export LOG_LEVEL=DEBUG
```

### Viewing JUnit Reports

If using the `--report` option, view test results:
```bash
# View XML report
cat /path/to/report.xml

# Or use a JUnit viewer tool
```

## Best Practices

### 1. Credential Management
- Store credentials in config files rather than CLI arguments for security
- Use pull secrets with all required registry authentications
- Test credentials before starting long-running mirror operations

### 2. Network and Performance
- Ensure stable network connection (mirroring can take several hours)
- Use `--image-timeout 30m` or higher for large images
- Consider running in a screen/tmux session for long operations
- Monitor disk space in workspace directory

### 3. IDMS Management
- Each run creates a unique IDMS with format `fdf-{run_id}`
- Clean up old IDMS resources if running multiple times: `oc delete imagedigestmirrorset fdf-<old-run-id>`
- Wait for MachineConfigPool updates to complete before testing

### 4. Workspace Cleanup
- Previous runs are preserved in timestamped directories
- Clean up old workspaces periodically to save disk space
- Keep at least one previous run for rollback/debugging

### 5. Registry Configuration
- Only use `--configure-registries` if mirroring internal FDF images
- Uses `/etc/containers/registries.conf.d/ocs-ci-fdf-mirrors.conf` for OCS-CI specific configuration
  - Non-destructive approach that doesn't modify system configuration
  - Easy to remove: `sudo rm /etc/containers/registries.conf.d/ocs-ci-fdf-mirrors.conf`
  - Easy to update: just run the command again to replace the file
  - Directory is automatically created if it doesn't exist
- Available on RHEL 7.6+, RHEL 8+, and all OpenShift nodes

### 6. CI/CD Integration
- Use `--report` option for JUnit XML output
- Set appropriate timeouts in CI pipelines (5+ hours recommended)
- Use `python -m ocs_ci.framework.fdf_mirror.main` for module execution reliability
- Check exit codes for success/failure status

### 7. Troubleshooting
- Enable DEBUG logging for detailed information: `export LOG_LEVEL=DEBUG`
- Check workspace directories for partial results on failure
- Retry logic handles transient failures automatically (3 attempts)
- Review IDMS and CatalogSource YAML files before applying manually
