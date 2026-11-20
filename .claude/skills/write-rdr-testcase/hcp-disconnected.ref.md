## HCP (Hosted Control Plane) with RDR

HCP (Hosted Clusters / Hypershift) changes several RDR behaviours. Key config flag: `is_hosted: true` in the cluster's `MULTICLUSTER` block.

### How HCP clusters differ

| Behaviour | Standard cluster | HCP cluster |
|---|---|---|
| `validate_application_odf_cli` | Runs normally | **Auto-skipped** — returns `None` when any `is_hosted=True` |
| `create_ingress_cert_dr` | Builds + applies trust bundle | **Skipped** — HCP shares hosting cluster's ingress CA |
| DRPolicy cluster name | Bare cluster name | `{HYPERSHIFT_ADDON_DISCOVERYPREFIX}-{cluster_name}` |
| `MachineConfigPool` rollout waits | Required | **Skipped** — HCP has no independent MachineConfigPool |
| Node stop/start | Standard IaaS via `nodes_multicluster` | HCP workers managed by hosting cluster — verify platform support |

### Detecting HCP at test time

```python
is_any_hosted = any(
    c.MULTICLUSTER.get("is_hosted", False)
    for c in config.clusters
)
```

> You never need to apply the HCP prefix in test code — `failover` and `relocate` handle it internally. Do not add explicit HCP guards in tests.

### Pre-submit checklist for HCP tests

- [ ] No explicit `validate_application_odf_cli` calls — let `failover`/`relocate` handle it
- [ ] `create_ingress_cert_dr` is NOT called in tests
- [ ] Node operations verified compatible with HCP worker node management
- [ ] `@skipif_ocs_version` gate present if feature requires minimum OCS version

---

## Disconnected Mode

Tests themselves require no disconnected guards — **except** CNV tests that need CDI registry authentication. The `mirror_rdr_images` session fixture handles mirroring automatically.

### CNV tests in disconnected mode — CDI registry auth

```python
from ocs_ci.helpers.dr_helpers import create_cdi_pull_secret, create_cdi_cert_configmap

create_cdi_pull_secret(
    namespace=cnv_workload.workload_namespace,
    secret_name="quayadmin",
)
create_cdi_cert_configmap(
    namespace=cnv_workload.workload_namespace,
    configmap_name="user-ca-bundle",
)
```

> The `cnv_dr_workload` fixture calls these internally when `disconnected=True`. Only call manually when deploying CNV workloads outside the fixture.

### Disconnected pre-submit checklist

- [ ] Test does NOT call `generate_rdr_mirror_images`, `apply_itms_to_managed_clusters`, or `create_ingress_cert_dr`
- [ ] CNV tests deploying outside `cnv_dr_workload` call `create_cdi_pull_secret` and `create_cdi_cert_configmap` before workload creation
- [ ] `wait_for_mirroring_status_ok` passes explicit `timeout` (900–1800s)
- [ ] Both RBD and CephFS workload images exist in the mirror registry when parametrizing on `pvc_interface`
