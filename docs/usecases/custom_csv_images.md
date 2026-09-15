# Testing with Custom CSV Images

This guide shows how to test ODF/OCS with custom container images by modifying the ClusterServiceVersion (CSV) during deployment.

## Use Cases

- Testing pull requests with custom operator builds
- Testing pre-release or development images
- Testing patches before they're officially released
- Reproducing issues with specific image versions

## Configuration Files

### Single Custom Image (Pattern-Based)

The simplest way to replace an image without needing to know the exact SHA.

**File: `conf/ocsci/custom_ocs_operator.yaml`**

```yaml
DEPLOYMENT:
  # Replace any version of ocs-rhel9-operator with your custom build
  csv_change_from: "registry.redhat.io/odf4/ocs-rhel9-operator"
  csv_change_to: "quay.io/myusername/ocs-rhel9-operator:pr-12345"
```

**Usage:**
```bash
run-ci \
  --ocsci-conf conf/ocsci/production-conf.yaml \
  --ocsci-conf conf/ocsci/custom_ocs_operator.yaml \
  --deploy \
  --cluster-path ~/my-cluster
```

### Multiple Custom Images (Pattern-Based)

Replace multiple operator images at once.

**File: `conf/ocsci/custom_odf_stack.yaml`**

```yaml
DEPLOYMENT:
  csv_change_from:
    - "registry.redhat.io/odf4/ocs-rhel9-operator"
    - "registry.redhat.io/odf4/rook-ceph-rhel9-operator"
    - "registry.redhat.io/rhceph/rhceph-8-rhel9"
    - "registry.redhat.io/odf4/ocs-metrics-exporter-rhel9"
  csv_change_to:
    - "quay.io/myusername/ocs-operator:dev-latest"
    - "quay.io/myusername/rook-ceph:dev-latest"
    - "quay.io/myusername/ceph:dev-latest"
    - "quay.io/myusername/ocs-metrics-exporter:dev-latest"
```

**Usage:**
```bash
run-ci \
  --ocsci-conf conf/deployment/aws/ipi_3az.yaml \
  --ocsci-conf conf/ocsci/custom_odf_stack.yaml \
  --deploy \
  --cluster-path ~/my-cluster
```

### Testing a Specific PR Build

For testing a pull request with custom images from your build system.

**File: `conf/ocsci/pr_12345.yaml`**

```yaml
DEPLOYMENT:
  csv_change_from:
    # Only replace the component you changed
    - "registry.redhat.io/odf4/ocs-rhel9-operator"
  csv_change_to:
    # Use your PR-specific build tag
    - "quay.io/ocs-dev/ocs-rhel9-operator:pr-12345-abc123"
```

### Exact SHA Replacement (Advanced)

Use when you need precise control over which exact image to replace.

**File: `conf/ocsci/exact_sha_replacement.yaml`**

```yaml
DEPLOYMENT:
  # Only replace this specific SHA
  csv_change_from: "registry.redhat.io/odf4/ocs-rhel9-operator@sha256:582e2365862ada1c649adf8fef865c1a61cb02b735a9551b79c219f0e131aa60"
  csv_change_to: "quay.io/myusername/ocs-operator@sha256:abc123def456789..."
```

### Testing OpenShift Proxy Images

Replace OpenShift sidecar images (OAuth proxy, RBAC proxy, etc.).

**File: `conf/ocsci/custom_openshift_sidecars.yaml`**

```yaml
DEPLOYMENT:
  csv_change_from:
    - "registry.redhat.io/openshift4/ose-oauth-proxy-rhel9"
    - "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9"
  csv_change_to:
    - "quay.io/myusername/oauth-proxy:custom"
    - "quay.io/myusername/kube-rbac-proxy:custom"
```

### Development with Local Registry

Testing with images in your local or internal registry.

**File: `conf/ocsci/local_registry.yaml`**

```yaml
DEPLOYMENT:
  csv_change_from:
    - "registry.redhat.io/odf4/ocs-rhel9-operator"
    - "registry.redhat.io/odf4/rook-ceph-rhel9-operator"
  csv_change_to:
    - "my-registry.local:5000/odf/ocs-operator:latest"
    - "my-registry.local:5000/odf/rook-ceph:latest"
```

## Complete Example: Testing a Bug Fix

Scenario: You fixed a bug in the OCS operator and want to test it before the official build.

**Step 1: Build and push your image**
```bash
# Build your custom operator
podman build -t quay.io/myusername/ocs-operator:bug-fix-1234 .

# Push to your registry
podman push quay.io/myusername/ocs-operator:bug-fix-1234
```

**Step 2: Create config file**

**File: `conf/ocsci/test_bug_fix_1234.yaml`**
```yaml
---
DEPLOYMENT:
  csv_change_from: "registry.redhat.io/odf4/ocs-rhel9-operator"
  csv_change_to: "quay.io/myusername/ocs-operator:bug-fix-1234"

# Optional: Add a note about what you're testing
# This is just a comment for documentation
# Testing fix for BZ#1234567 - OCS operator crashes on upgrade
```

**Step 3: Deploy and test**
```bash
run-ci \
  --ocsci-conf conf/deployment/aws/ipi_3az_rhcos.yaml \
  --ocsci-conf conf/ocsci/downstream_config.yaml \
  --ocsci-conf conf/ocsci/test_bug_fix_1234.yaml \
  --deploy \
  --cluster-path ~/test-bug-1234

# After deployment succeeds, run your test
run-ci \
  --ocsci-conf conf/deployment/aws/ipi_3az_rhcos.yaml \
  --ocsci-conf conf/ocsci/downstream_config.yaml \
  --ocsci-conf conf/ocsci/test_bug_fix_1234.yaml \
  --cluster-path ~/test-bug-1234 \
  tests/e2e/test_specific_bug.py::test_bug_1234
```

## Combining with Other Deployment Options

You can combine CSV image replacement with other deployment options.

**File: `conf/ocsci/my_custom_deployment.yaml`**
```yaml
---
ENV_DATA:
  platform: "aws"
  region: "us-east-2"
  worker_replicas: 3

DEPLOYMENT:
  # Use custom images
  csv_change_from:
    - "registry.redhat.io/odf4/ocs-rhel9-operator"
    - "registry.redhat.io/odf4/rook-ceph-rhel9-operator"
  csv_change_to:
    - "quay.io/myrepo/ocs-operator:my-branch"
    - "quay.io/myrepo/rook-ceph:my-branch"

  # Other deployment options
  ui_deployment: false
  live_deployment: false
```

## Verification

After deployment, verify your custom images are running:

```bash
# Check the CSV
oc get csv -n openshift-storage

# Check which images are actually running in pods
oc get pods -n openshift-storage -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{range .spec.containers[*]}  {.image}{"\n"}{end}{"\n"}{end}' | grep -E "ocs-operator|rook-ceph"

# Or use this one-liner to see all unique images
oc get pods -n openshift-storage -o jsonpath='{.items[*].spec.containers[*].image}' | tr ' ' '\n' | sort -u
```

## Troubleshooting

### Image Pull Errors

If you see `ImagePullBackOff` errors:

1. Verify your image exists and is accessible:
   ```bash
   podman pull quay.io/myusername/ocs-operator:my-tag
   ```

2. Check if authentication is needed:
   ```bash
   # Create pull secret if using private registry
   oc create secret docker-registry my-pull-secret \
     --docker-server=quay.io \
     --docker-username=myusername \
     --docker-password=mypassword \
     -n openshift-storage
   ```

### CSV Not Modified

If your images aren't being replaced:

1. Check deployment logs for CSV modification messages:
   ```bash
   grep "CSV.*will be modified" <log-file>
   ```

2. Verify your config was loaded:
   ```bash
   # The run-ci output should show all loaded configs
   ```

3. Make sure both `csv_change_from` and `csv_change_to` are set

### Pattern Not Matching

If pattern-based replacement isn't working:

1. Check the exact image path in the CSV:
   ```bash
   oc get csv -n openshift-storage ocs-operator.v4.19.0 -o yaml | grep "image:"
   ```

2. Make sure your pattern matches exactly (case-sensitive, including registry):
   ```yaml
   # Wrong - missing registry
   csv_change_from: "odf4/ocs-rhel9-operator"

   # Correct
   csv_change_from: "registry.redhat.io/odf4/ocs-rhel9-operator"
   ```

## Best Practices

1. **Use meaningful tags**: Instead of `:latest`, use descriptive tags like `:pr-12345` or `:bug-fix-1234`

2. **Keep configs organized**: Create separate config files for different testing scenarios

3. **Document your changes**: Add comments in your config files explaining what you're testing

4. **Test incrementally**: Start with replacing one image, then expand to multiple if needed

5. **Pattern-based for development**: Use pattern-based replacement for iterative development where you're frequently updating images

6. **Exact for reproduction**: Use exact SHA replacement when reproducing specific issues or testing regression fixes

## See Also

- [CSV Image Modification Examples](../csv_image_modification_examples.md) - Technical details and advanced patterns
- [conf/README.md](../../conf/README.md) - Complete configuration reference
- [Getting Started](../getting_started.md) - Initial setup and prerequisites
