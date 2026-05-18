# FDF Image Mirroring

Guide for mirroring FDF (Fusion Data Foundation) unreleased images from Mgen.

## Quick Start

### Using the Integrated Function

```python
from ocs_ci.deployment.disconnected import mirror_fdf_catalog_via_oc_mirror

# Mirror FDF catalog and images
results = mirror_fdf_catalog_via_oc_mirror(
    catalog_image="docker-na-public.artifactory.swg-devops.com/hyc-abell-devops-team-dev-docker-local/df/isf-data-foundation-catalog:v4.20",
    mirror_registry="your-registry.com:5000/fdf",
    configure_registries=True  # Optional: configure registries.conf for internal images
)
```

## Manual Process

### 1. Create ImageSetConfiguration File (`fdf_isc.yaml`)

```yaml
kind: ImageSetConfiguration
apiVersion: mirror.openshift.io/v1alpha2
mirror:
  operators:
    - catalog: docker-na-public.artifactory.swg-devops.com/hyc-abell-devops-team-dev-docker-local/df/isf-data-foundation-catalog:v4.20
```

### 2. Configure registries.conf (For Internal Images)

Add to `/etc/containers/registries.conf`:

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

### 3. Run oc mirror Command

```bash
oc mirror \
  --config fdf_isc.yaml \
  docker://<mirror-registry> \
  --workspace file://<workspace-path> \
  --v2 \
  --dest-tls-verify=false \
  --authfile <pull-secret> \
  --image-timeout 30m
```

**Example:**
```bash
oc mirror \
  --config fdf_isc.yaml \
  docker://registry.example.com:5000/fdf \
  --workspace file:///tmp/oc-mirror-workspace \
  --v2 \
  --dest-tls-verify=false \
  --authfile ~/.docker/config.json \
  --image-timeout 30m
```

### 4. Apply ImageDigestMirrorSet

```bash
# Find and apply the generated IDMS
oc apply -f oc-mirror-workspace/results-*/working-dir/cluster-resources/idms-*.yaml

# Wait for nodes to be ready
oc wait --for=condition=Updated mcp/worker --timeout=600s
oc wait --for=condition=Updated mcp/master --timeout=600s
```

## FDF Catalog Images by Version

- **FDF 4.18**: `docker-na-public.artifactory.swg-devops.com/hyc-abell-devops-team-dev-docker-local/df/isf-data-foundation-catalog:v4.18`
- **FDF 4.19**: `docker-na-public.artifactory.swg-devops.com/hyc-abell-devops-team-dev-docker-local/df/isf-data-foundation-catalog:v4.19`
- **FDF 4.20**: `docker-na-public.artifactory.swg-devops.com/hyc-abell-devops-team-dev-docker-local/df/isf-data-foundation-catalog:v4.20`

## Templates

The following template files are available:
- **ImageSetConfiguration**: `ocs_ci/templates/fusion-data-foundation/fdf-imageset-config.yaml`
- **Registries.conf**: `ocs_ci/templates/fusion-data-foundation/registries.conf.template`

## Implementation Details

The FDF mirroring functionality is integrated into `ocs_ci/deployment/disconnected.py` and follows the same pattern as existing ODF mirroring:

- Uses `oc-mirror` tool (automatically installed if not present)
- Creates and applies ImageDigestMirrorSet (IDMS)
- Optionally configures registries.conf for internal images
- Waits for MachineConfigPool to be ready after IDMS application
