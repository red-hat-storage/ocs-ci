# ACM and Submariner Auto-Configuration

## Summary

This document describes the automatic configuration mechanism for ACM and Submariner deployment in the `ocs-ci` test framework. The implementation eliminates manual version configuration by automatically determining component version based on the OpenShift Container Platform (OCP) version.

**Related Changes:**

| Repository | Link |
|---|---|
| GitHub PR | [red-hat-storage/ocs-ci#14733](https://github.com/red-hat-storage/ocs-ci/pull/14733) |
| GitLab MR | [ocs/ocs4-jenkins!2472](https://gitlab.cee.redhat.com/ocs/ocs4-jenkins/-/merge_requests/2472) |

**Main Commit:** `d29e833fe` — *Auto-configure ACM and Submariner based on OCP version*
**Backported to:** release-4.16 through release-4.21

---

## Problem

Prior to this change, ACM and Submariner configuration required manual specification of these values:

- `acm_hub_unreleased` — Boolean flag for ACM release status
- `RELEASED_OR_UNRELEASED_ACM — String value (`"released"` or `"unreleased"`)
- `UPSTREAM_OR_DOWNSTREAM_SUBMARINER` — String value (`"Upstream"` or `"Downstream"`)
- `submariner_source` — String value (`"Downstream"`)
- submariner_release_type - String value (`"released"` or `"unreleased"`)

These values were hardcoded in multiple OCP version-specific configuration files, causing:

- Configuration inconsistencies across test runs
- Maintenance overhead every time a new OCP version was introduced
- Risk of version mismatches between OCP and its components

---

## Solution

### Architecture

The solution introduces a version mapping system with two components:

1. **Centralized Version Mappings** — Static dictionaries in `ocs_ci/ocs/defaults.py` mapping OCP versions to component release status
2. **Auto-Configuration Functions** — Runtime logic in `ocs_ci/utility/utils.py` that reads these mappings during `pytest` initialization

### Version Mappings

**File:** `ocs_ci/ocs/defaults.py` (lines 173–197)

```python
ocp_to_acm_unreleased_mapping = {
    "4.14": False,
    "4.15": False,
    "4.16": False,
    "4.17": False,
    "4.18": False,
    "4.19": False,
    "4.20": False,
    "4.21": False,
    "4.22": True,
    "4.23": True,
}

ocp_to_submariner_unreleased_mapping = {
    "4.14": False,
    "4.15": False,
    "4.16": False,
    "4.17": False,
    "4.18": False,
    "4.19": False,
    "4.20": False,
    "4.21": False,
    "4.22": False,
    "4.23": True,
}
```

**Mapping semantics:**
- `False` = Released / GA version
- `True` = Unreleased / development version
- Unmapped versions default to `True` (conservative fallback)

NOTE: In Future, maintainers will only need to create MR changing these values True to False based on the GA'ed versions. E.g 4.23 has to be False once they GA the ACM and Submariner Versions used.

---

### Auto-Configuration Functions

**File:** `ocs_ci/utility/utils.py`

#### `auto_configure_acm()` — Lines 7619–7640

Configures `config.ENV_DATA["acm_hub_unreleased"]` based on OCP version.

Execution order:
1. Skip if `acm_hub_unreleased` is already set (user override respected)
2. Retrieve OCP version via `get_semantic_ocp_version_from_config()`
3. Look up version in `ocp_to_acm_unreleased_mapping`
4. Default to `True` if version is not found
5. Set config value and log the decision

#### `auto_configure_submariner()` — Lines 7643–7677

Configures `config.ENV_DATA["submariner_release_type"]` and `submariner_source`.

Execution order:
1. Set `submariner_source` to `"downstream"` if not already set
2. Skip if `submariner_release_type` is already set (user override respected)
3. Retrieve OCP version via `get_semantic_ocp_version_from_config()`
4. Look up version in `ocp_to_submariner_unreleased_mapping`
5. Default to `True` if version is not found
6. Convert boolean to string: `True` → `"unreleased"`, `False` → `"released"`
7. Set config value and log the decision

---

### Integration Point

**File:** `ocs_ci/framework/pytest_customization/ocscilib.py` (lines 393–395)

```python
def pytest_configure(config):
    # ... existing initialization ...

    if not (config.getoption("--help") or config.getoption("collectonly")):
        process_cluster_cli_params(config)
        auto_configure_acm()           # ACM configuration
        auto_configure_submariner()    # Submariner configuration

        # ... deployment configuration loading ...
```

Both functions execute **after** CLI parameter processing and **before** deployment configuration loading, ensuring the values are available for the entire test session.

---

### Configuration Templates Added

Two new YAML config files support released deployments:

**`conf/ocsci/acm_hub_released_deploy.yaml`**
```yaml
ENV_DATA:
  deploy_acm_hub_cluster: true
  skip_ocs_deployment: true
  acm_hub_unreleased: false
```

**`conf/ocsci/submariner_downstream_released.yaml`**
```yaml
ENV_DATA:
  submariner_source: "downstream"
  submariner_release_type: "released"
  submariner_channel: ""
  subctl_version: "subctl-rhel9:v0.23"
```

---

### Hardcoded Values Removed

| File | Removed Keys |
|---|---|
| `ocp-4.21-config.yaml` | `submariner_source` |
| `ocp-4.22-config.yaml` | `submariner_source`, `submariner_release_type`, `acm_hub_unreleased` |
| `ocp-4.22-ga-config.yaml` | `submariner_source`, `submariner_release_type`, `acm_hub_unreleased` |

---

## Configuration Flow

```
Test Start
    │
    ▼
pytest_configure()
    │
    ▼
process_cluster_cli_params()
    │
    ▼
auto_configure_acm()
    ├─ User override set? → Skip
    ├─ Get OCP version
    ├─ Lookup ocp_to_acm_unreleased_mapping
    └─ Set config.ENV_DATA["acm_hub_unreleased"] + log
    │
    ▼
auto_configure_submariner()
    ├─ Set default source = "downstream"
    ├─ User override set? → Skip
    ├─ Get OCP version
    ├─ Lookup ocp_to_submariner_unreleased_mapping
    └─ Set config.ENV_DATA["submariner_release_type"] + log
    │
    ▼
Load deployment configs
    │
    ▼
Deploy clusters → Execute tests
```

---

## Usage

### Default Behavior

No configuration required. The system automatically detects OCP version and sets the correct values.

Example log output:
```
INFO: OCP 4.21 → ACM: RELEASED
INFO: OCP 4.21 → Submariner: RELEASED
```

### User Overrides

Explicit values in config files are always respected and skip auto-configuration.

```yaml
ENV_DATA:
  acm_hub_unreleased: true
  submariner_release_type: "unreleased"
```

Log output:
```
INFO: ACM explicitly configured: acm_hub_unreleased=True
INFO: Submariner explicitly configured: release_type=unreleased
```

### Unknown OCP Versions

Unmapped versions default to `unreleased` as a safe fallback.

```
INFO: OCP 4.25 not in mapping → ACM: UNRELEASED (safe default)
INFO: OCP 4.25 not in mapping → Submariner: UNRELEASED (safe default)
```

---

## Testing Matrix

| OCP Version | ACM Status | Submariner Status | Source |
|---|---|---|---|
| 4.14 – 4.21 | Released | Released | Auto-configured |
| 4.22 | Unreleased | Released | Auto-configured |
| 4.23+ | Unreleased | Unreleased | Auto-configured |
| Unknown | Unreleased | Unreleased | Default fallback |
| Any | User-specified | User-specified | Explicit override |

---

## Backport Status

| Branch | Commit | Status |
|---|---|---|
| release-4.16 | `fd73d47d6` | Merged |
| release-4.17 | `f68514834` | Merged |
| release-4.18 | `13dcb69b5` | Merged |
| release-4.19 | `19c3ce805` | Merged |
| release-4.20 | `3b718b427` | Merged |
| release-4.21 | `1f91cd511` | Merged |

---


## Maintenance

### Adding a New OCP Version

Update both mappings in `ocs_ci/ocs/defaults.py`:

```python
ocp_to_acm_unreleased_mapping = {
    # ... existing entries ...
    "4.24": True,  # Initially unreleased
}

ocp_to_submariner_unreleased_mapping = {
    # ... existing entries ...
    "4.24": True,  # Initially unreleased
}
```

No changes required in any OCP version-specific config files.

### Marking a Version as Released (GA)

Change the mapping value to `False` when the component reaches GA:

```python
ocp_to_acm_unreleased_mapping = {
    # ... existing entries ...
    "4.23": False,  # Updated when ACM 2.x GA'd
}
```

---

## Troubleshooting

**Auto-configuration not applied**
- Verify no explicit override exists in config files or CLI
- Check pytest init logs for auto-configuration messages
- Confirm `pytest_configure()` is executing

**Wrong component version used**
- Check logs for the detected OCP version
- Confirm the correct mapping entry in `ocs_ci/ocs/defaults.py`
- Update mapping if value is incorrect for the given OCP version

**Unmapped OCP version**
- Log will show: `"not in mapping → UNRELEASED (safe default)"`
- Add the OCP version to both mappings in `defaults.py` with the correct `True`/`False` value
- Submit a PR with the mapping update

---

## Code Reference

| Component | File | Lines |
|---|---|---|
| ACM Version Mapping | `ocs_ci/ocs/defaults.py` | 173–184 |
| Submariner Version Mapping | `ocs_ci/ocs/defaults.py` | 186–197 |
| `auto_configure_acm()` | `ocs_ci/utility/utils.py` | 7619–7640 |
| `auto_configure_submariner()` | `ocs_ci/utility/utils.py` | 7643–7677 |
| pytest integration | `ocs_ci/framework/pytest_customization/ocscilib.py` | 393–395 |

---

## References

- [Red Hat ACM Documentation](https://access.redhat.com/documentation/en-us/red_hat_advanced_cluster_management_for_kubernetes)
- [Submariner Project](https://submariner.io/)
- [ocs-ci Repository](https://github.com/red-hat-storage/ocs-ci)

---

*Author: Deepanjan Acharya — July 2026*
