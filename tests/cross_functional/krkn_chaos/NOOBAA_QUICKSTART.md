# NooBaa KRKN Chaos Tests - Quick Start Guide

## Prerequisites

1. **ODF/OCS Cluster**: Running OpenShift Data Foundation with NooBaa
2. **KRKN Setup**: KRKN framework installed (handled by test fixture)
3. **NooBaa Health**: Verify NooBaa pods are running:
   ```bash
   oc get pods -n openshift-storage | grep noobaa
   ```

## 5-Minute Quick Start

### Step 1: Verify NooBaa is Running
```bash
# Check NooBaa pods
oc get pods -n openshift-storage -l app=noobaa

# Expected output:
# noobaa-core-0                   Running
# noobaa-db-pg-0                  Running
# noobaa-operator-...             Running
# noobaa-endpoint-...             Running
```

### Step 2: Run Your First Test
```bash
# Run a single 20-minute NooBaa database chaos test
cd /path/to/ocs-ci

pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_pod_disruption_with_s3_workload[noobaa-db-20min-180s-interval] \
    -v -s
```

### Step 3: Watch the Chaos
```bash
# In another terminal, watch NooBaa pods restart
watch -n 2 "oc get pods -n openshift-storage | grep noobaa"

# You should see pods being killed and restarting every ~3 minutes
```

### Step 4: Review Results
The test will output:
- ✅ S3 workload health status
- ✅ Number of pod kills executed
- ✅ NooBaa recovery validation
- ✅ Final health check results

## Test Scenarios

### Beginner: Quick Database Test
**Duration**: 20 minutes | **Pod Kills**: ~6 times | **Interval**: 3 minutes
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_pod_disruption_with_s3_workload[noobaa-db-20min-180s-interval]
```

### Intermediate: Extended Core Test
**Duration**: 30 minutes | **Pod Kills**: ~7 times | **Interval**: 4 minutes
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_pod_disruption_with_s3_workload[noobaa-core-30min-240s-interval]
```

### Advanced: Long-Running Database Test
**Duration**: 60 minutes | **Pod Kills**: ~12 times | **Interval**: 5 minutes
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_pod_disruption_with_s3_workload[noobaa-db-60min-300s-interval]
```

### Expert: Extreme Strength Test
**Duration**: 50 minutes | **Pod Kills**: ~33 per pod | **All Pods Simultaneously**
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_strength_testing[noobaa-extreme-stress]
```

## Test Filtering

### Run All Database Tests
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py \
    -k "noobaa-db"
```

### Run All 20-Minute Tests
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py \
    -k "20min"
```

### Run All Strength Tests
```bash
pytest --ocsci-conf conf/ocsci/krkn_noobaa_chaos_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py::TestKrKnNooBaaChaos::test_krkn_noobaa_strength_testing
```

## Configuration Customization

### Quick Config Changes

Edit `conf/ocsci/krkn_noobaa_chaos_config.yaml`:

```yaml
ENV_DATA:
  krkn_config:
    mcg_config:
      # Reduce buckets for faster tests
      num_buckets: 1

      # Reduce iterations for shorter tests
      iteration_count: 10

      # Disable metadata operations for lighter load
      metadata_ops_enabled: false
```

### Override Config at Runtime

```bash
# Use your own config
pytest --ocsci-conf /path/to/my_config.yaml \
    tests/cross_functional/krkn_chaos/test_krkn_noobaa_chaos.py
```

## Monitoring During Tests

### Watch Pod Status
```bash
watch -n 2 "oc get pods -n openshift-storage | grep noobaa"
```

### Check NooBaa Logs
```bash
# NooBaa core logs
oc logs -n openshift-storage noobaa-core-0 -f

# NooBaa DB logs
oc logs -n openshift-storage noobaa-db-pg-0 -f

# NooBaa operator logs
oc logs -n openshift-storage deployment/noobaa-operator -f
```

### Monitor S3 Operations
```bash
# Check test namespace for workload pods
oc get pods -n <test-namespace> -l app=awscli
```

## Expected Test Output

### During Test Execution
```
🚀 Starting NooBaa pod disruption chaos for 1200s
⚡ Pods will be killed every 180s
💥 Total expected disruptions: 6
📋 Generated pod disruption scenario: /path/to/scenario.yaml
🔥 Executing KRKN chaos scenarios...
✅ Chaos execution completed
🔍 Validating S3 workload health after chaos
✅ S3 workloads validated and cleaned up successfully
📊 Chaos Results:
   Total scenarios: 6
   Successful: 5
   Failed: 1
✅ NooBaa pods are healthy
🎉 NooBaa pod disruption test for noobaa-db-pg-0 completed successfully
```

### Pod Behavior During Test
```
NAME               READY   STATUS    RESTARTS   AGE
noobaa-db-pg-0     1/1     Running   0          5m
noobaa-db-pg-0     1/1     Terminating   0      8m    ← Pod killed
noobaa-db-pg-0     0/1     Pending       0      8m
noobaa-db-pg-0     0/1     ContainerCreating   0   8m
noobaa-db-pg-0     1/1     Running       1      9m    ← Pod recovered
```

## Troubleshooting Common Issues

### Issue: No NooBaa Pods Found
```
Error: No NooBaa core pods found - NooBaa may not be deployed
```
**Solution**: Deploy NooBaa or verify it's enabled in your ODF cluster

### Issue: Bucket Creation Timeout
```
Error: Bucket failed to become healthy: timeout
```
**Solution**: Check NooBaa operator status and OBC controller

### Issue: S3 Operations Failing
```
Warning: Workload validation/cleanup issue: Request timeout
```
**Solution**: This is expected during pod kills. Verify pods recovered to Running state.

### Issue: Test Takes Too Long
```
# Reduce test duration by using shorter scenarios
pytest ... -k "20min"  # Only run 20-minute tests
```

## Next Steps

1. **Review Documentation**: Read `README_NOOBAA_CHAOS.md` for detailed info
2. **Customize Tests**: Modify configuration for your cluster size
3. **Run Full Suite**: Execute all tests to validate complete NooBaa resilience
4. **Analyze Results**: Review logs and metrics for performance insights
5. **Report Issues**: File bugs for any unexpected failures

## Quick Reference

| Scenario | Duration | Pod Kills | Best For |
|----------|----------|-----------|----------|
| noobaa-db-20min-180s | 20 min | ~6 | Quick validation |
| noobaa-db-30min-240s | 30 min | ~7 | Extended testing |
| noobaa-db-60min-300s | 60 min | ~12 | Long-running resilience |
| noobaa-core-20min-180s | 20 min | ~6 | S3 service testing |
| noobaa-operator-20min-180s | 20 min | ~6 | Operator resilience |
| noobaa-high-stress | 30 min | ~15/pod | Strength testing |
| noobaa-extreme-stress | 50 min | ~33/pod | Extreme validation |
| noobaa-ultimate-stress | 80 min | ~80/pod | Ultimate resilience |

## Support

For issues or questions:
1. Check `README_NOOBAA_CHAOS.md` for detailed documentation
2. Review test logs for error details
3. Verify cluster and NooBaa health
4. Consult ODF/NooBaa documentation

## Safety Notes

⚠️ **These tests will repeatedly kill NooBaa pods!**
- Temporary S3 service disruptions are expected
- Do not run on production clusters without approval
- Monitor cluster health during testing
- Allow pods to fully recover between test runs

✅ **Tests are designed to be safe:**
- Pods automatically restart after kill
- S3 workload validates data integrity
- No permanent data loss expected
- Database corruption checks included
