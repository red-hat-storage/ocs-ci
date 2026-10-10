# MCG Performance Profiles UI Test Automation

## Overview

This test suite automates the UI test cases for the MCG (Multicloud Object Gateway) Performance Profiles feature (RHSTOR-8629). It verifies the Configure Performance page layout, profile selection functionality, and resource requirement displays.

## Test Files

### Main Test File
- **`test_mcg_performance_profile_ui.py`** - Contains UI test cases for:
  - **UI-1**: Configure Performance Page Layout (Core Storage and MCG Sections) - P2
  - **UI-2**: Core Storage Profile Selection (Regression) - P3
  - **UI-3**: MCG Profile Selection Updates StorageCluster CR - P1

### Page Objects
- **`ocs_ci/ocs/ui/page_objects/configure_performance.py`** - ConfigurePerformancePage class with methods to:
  - Verify section presence and layout
  - Get and validate node lists
  - Select profiles from dropdowns
  - Save/cancel changes
  - Read profile values from StorageCluster CR

### Helper Utilities
- **`ocs_ci/ocs/ui/helpers/mcg_profile_helpers.py`** - Utility functions for:
  - MCG profile resource specifications
  - CPU/memory normalization
  - Reading profiles from CRs
  - Pod resource verification

## Test Cases Implemented

### UI-1: Configure Performance Page Layout
**Polarion ID**: OCS-8317
**Priority**: P2
**Status**: Ready for execution

#### What it tests:
1. Navigate to Configure Performance page
2. Verify two sections exist: Core Storage and MCG
3. Verify Core Storage displays inline (not as modal)
4. Verify Core Storage shows only OCS-labeled nodes
5. Verify MCG section shows all cluster nodes
6. Verify Core Storage has no node selection option

#### Run:
```bash
pytest tests/functional/ui/test_mcg_performance_profile_ui.py::TestConfigurePerformancePageLayout::test_configure_performance_page_layout -v
```

### UI-2: Core Storage Profile Selection Regression
**Polarion ID**: OCS-8319
**Priority**: P3
**Status**: Ready for execution

#### What it tests:
Tests that Core Storage profile selection (lean, balanced, performance) still works after the Configure Performance page redesign.

#### Run:
```bash
pytest tests/functional/ui/test_mcg_performance_profile_ui.py::TestCoreStorageRegression::test_core_storage_profile_selection_regression -v
```

### UI-3: MCG Profile Selection
**Polarion ID**: OCS-8318
**Priority**: P1
**Status**: Ready for execution

#### What it tests:
1. MCG profile selection updates StorageCluster CR
2. UI shows the active profile when page is reopened
3. Switching between profiles updates CR correctly

#### Run:
```bash
pytest tests/functional/ui/test_mcg_performance_profile_ui.py::TestMCGProfileSelection::test_mcg_profile_selection -v
```

## Cluster Requirements

- ODF/OCS cluster version 4.23+ or 5.0+
- Kubeadmin access to the cluster
- MCG configured and healthy (NooBaa CR in Ready state)
- At least one Core Storage node with OCS label
- At least one additional node (for MCG node display verification)

## Setup Instructions

### Run Tests

**Run all MCG UI tests:**
```bash
pytest tests/functional/ui/test_mcg_performance_profile_ui.py -v
```

**Run specific test:**
```bash
pytest tests/functional/ui/test_mcg_performance_profile_ui.py::TestConfigurePerformancePageLayout::test_configure_performance_page_layout -v
```

**Run with custom options:**
```bash
pytest tests/functional/ui/test_mcg_performance_profile_ui.py -v \
  --tb=short \
  -m "ui and tier2"
```

## Test Data

### MCG Profiles (x86)
Profile resource specifications are defined in `mcg_profile_helpers.py`:

| Profile | Core | DB | Endpoint | Endpoint Count | DB Instances | PV Pool Vols |
|---------|------|----|---------|----|---|---|
| **default** | 500m/1 CPU, 1Gi/4Gi | 1/1 CPU, 2Gi/2Gi | 500m/2 CPU, 1Gi/3Gi | 1-2 | 2 | 3 |
| **mixed-workload** | 1/2 CPU, 2Gi/4Gi | 4/4 CPU, 8Gi/8Gi | 2/4 CPU, 2Gi/4Gi | 2-4 | 2 | 3 |
| **small-objects** | 1/2 CPU, 2Gi/6Gi | 6/6 CPU, 16Gi/16Gi | 1/4 CPU, 2Gi/4Gi | 2-4 | 2 | 3 |

### IBM Z (s390x) Adjustment
CPU requests are multiplied by 0.2 on IBM Z (CPU limits, memory, and endpoint counts are unchanged).

## Page Selectors and Locators

The ConfigurePerformancePage class uses the following XPath and CSS selectors:

```python
# Core Storage section
CORE_STORAGE_SECTION = "//*[contains(text(), 'Core Storage')]/ancestor::*[contains(@class, 'section')]"

# MCG section
MCG_SECTION = "//*[contains(text(), 'Multicloud')]/ancestor::*[contains(@class, 'section')]"

# MCG Profile selector dropdown
MCG_PROFILE_SELECTOR = "//*[contains(@class, 'c-select') and contains(@class, 'odf-configure-performance__selector')]"

# Save button
SAVE_BUTTON = "//button[contains(text(), 'Save')]"

# Cancel button
CANCEL_BUTTON = "//button[contains(text(), 'Cancel')]"
```

If selectors need adjustment for your environment, update them in `configure_performance.py`.

## Troubleshooting

### Common Issues

1. **"Configure Performance button not found"**
   - Verify you are on the StorageCluster details page
   - Check that your cluster version supports the feature (4.23+)
   - Inspect the page in browser to find the correct button selector

2. **"Core Storage section not present"**
   - Ensure you are on the Configure Performance page
   - Check page load timing - may need to increase WebDriverWait timeout

3. **"Nodes not found in section"**
   - Verify nodes are properly labeled with OCS label in Core Storage section
   - Check cluster has at least 1 OCS node and 1 additional node

4. **Selenium timeout errors**
   - Increase timeout values in `ConfigurePerformancePage.__init__()` if needed
   - Check network connectivity to cluster console
   - Verify browser driver is compatible with installed browser

### Debugging

Enable detailed logging:
```bash
pytest tests/functional/ui/test_mcg_performance_profile_ui.py -v \
  --log-cli-level=DEBUG \
  --capture=no
```

Take screenshots on failure:
```bash
pytest tests/functional/ui/test_mcg_performance_profile_ui.py -v \
  --screenshots=on_failure
```

## Test Execution Notes

### Timing
- Each test typically takes 3-5 minutes to complete
- Profile changes can take 1-2 minutes to propagate through system
- Do not interrupt tests mid-execution

### State Management
- Tests are designed to be idempotent where possible
- Each test should clean up its changes (e.g., revert profiles to default)
- If cleanup fails, manually reset profiles via CLI:
  ```bash
  oc patch storagecluster ocs-storagecluster -n openshift-storage --type merge \
    -p '{"spec":{"multiCloudGateway":{"performanceProfile":"default"}}}'
  ```

### Test Isolation
- Tests should not interfere with each other
- Run tests individually or in small groups to isolate failures
- Use separate clusters for concurrent test runs

## Extending Tests

### Adding New Test Cases

1. Create test method in appropriate class in `test_mcg_performance_profile_ui.py`
2. Add Polarion ID and mark with `@ui @tier2 @jira("RHSTOR-8629")`
3. Use ConfigurePerformancePage methods for UI interactions
4. Verify results using OCP CR reads and pod resource checks

Example:
```python
@ui
@tier2
@jira("RHSTOR-8629")
@polarion_id("OCS-XXXX")
def test_new_ui_case(self, setup_ui_class):
    """Test description matching RHSTOR-8629 test plan"""
    navigator = PageNavigator()
    configure_perf_page = navigator.nav_storage_cluster_default_page().click_configure_performance_button()

    # Your test logic here
    assert configure_perf_page.is_mcg_section_present()
```

### Adding New Page Object Methods

Add methods to ConfigurePerformancePage to encapsulate UI interactions:
```python
def new_method(self) -> bool:
    """Document the method"""
    try:
        # Implement using Selenium
        logger.info("✓ Method succeeded")
        return True
    except Exception as e:
        logger.error(f"Method failed: {e}")
        return False
```

## References

- **Test Strategy**: RHSTOR-8629 (MCG Profiles)Test Strategy.txt
- **Epic**: https://redhat.atlassian.net/browse/RHSTOR-8629
- **Feature Documentation**:
  - RHSTOR-9082: (UI) Move existing configure performance to another component
  - RHSTOR-9083: (UI) Add performance profile section for MCG
