# MCG Performance Profiles UI Tests

## Overview
Automated UI tests for MCG Performance Profiles (RHSTOR-8629): page layout verification, Core Storage profile selection, and MCG profile selection.

## Test Cases

**UI-1 (OCS-8317)**: Configure Performance page layout — two sections (Core Storage/MCG), inline display, OCS-labeled nodes, no selection.

**UI-2 (OCS-8319)**: Core Storage profile selection regression — select profiles, verify CR updated, re-open and confirm UI shows selection.

**UI-3 (OCS-8318)**: MCG profile selection — select profiles, verify CR updated, re-open and confirm UI shows selection.

## Files

- `tests/functional/ui/test_mcg_performance_profile_ui.py` — Test cases
- `ocs_ci/ocs/ui/page_objects/configure_performance.py` — Page object (13 methods)
- `ocs_ci/ocs/ui/helpers/mcg_profile_helpers.py` — Utilities and profile specs
- `ocs_ci/ocs/ui/page_objects/storage_cluster.py` — Updated with button method

## Run Tests

```bash
# All tests
pytest tests/functional/ui/test_mcg_performance_profile_ui.py -v

# Specific test
pytest tests/functional/ui/test_mcg_performance_profile_ui.py::TestConfigurePerformancePageLayout::test_configure_performance_page_layout -v

# With debug logs
pytest tests/functional/ui/test_mcg_performance_profile_ui.py -v --log-cli-level=DEBUG
```

## Requirements

- ODF/OCS 4.23+ or 5.0+
- Kubeadmin access
- MCG configured (NooBaa CR Ready)
- At least 2 nodes (1 with OCS label, 1 additional)

## MCG Profiles (x86)

| Profile | Core | DB | Endpoint | Count | DB Inst | PV Vols |
|---------|------|----|---------|----|---|----|
| **default** | 500m/1, 1Gi/4Gi | 1/1, 2Gi/2Gi | 500m/2, 1Gi/3Gi | 1-2 | 2 | 3 |
| **mixed-workload** | 1/2, 2Gi/4Gi | 4/4, 8Gi/8Gi | 2/4, 2Gi/4Gi | 2-4 | 2 | 3 |
| **small-objects** | 1/2, 2Gi/6Gi | 6/6, 16Gi/16Gi | 1/4, 2Gi/4Gi | 2-4 | 2 | 3 |

**IBM Z (s390x)**: CPU requests multiplied by 0.2; limits and memory unchanged.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Button not found | Verify cluster version 4.23+, check StorageCluster page |
| Section not present | Verify page fully loaded, use wait_for_element_to_be_visible from helpers_ui.py |
| Nodes not found | Check OCS labels on nodes, ensure cluster has 2+ nodes |
| Selenium timeout | Use explicit waits from helpers_ui.py (wait_for_element_to_be_clickable, etc.); check network connectivity |

## Profile Reset

```bash
oc patch storagecluster ocs-storagecluster -n openshift-storage --type merge \
  -p '{"spec":{"multiCloudGateway":{"performanceProfile":"default"}}}'
```

## Extending Tests

Add test methods to appropriate class, mark with `@ui @tier2 @jira("RHSTOR-8629")`, use ConfigurePerformancePage methods for interactions.
