# NooBaa Pod Targeting Strategy

## Pod Topology

Your NooBaa cluster has the following pods:

```
NAME                       ROLE        STATUS
noobaa-db-pg-cluster-1     primary     Running    ← PostgreSQL primary (read/write)
noobaa-db-pg-cluster-2     replica     Running    ← PostgreSQL replica (read-only)
noobaa-core-0              -           Running    ← NooBaa S3 service
noobaa-operator-...        -           Running    ← NooBaa operator
noobaa-endpoint-...        -           Running    ← NooBaa endpoint
```

## Current Test Configuration

### ✅ **PRIMARY POD TARGETING** (Current Implementation)

The tests are configured to **specifically target the PRIMARY database pod** for realistic failover testing:

```python
# Targets ONLY the primary pod by exact name
("noobaa-db-pg-cluster-1", ...)
```

### Why Target Primary Pod?

1. **Realistic Failure Scenario**: In production, you're most concerned with primary pod failures
2. **Triggers Failover**: Killing the primary forces PostgreSQL to promote the replica
3. **S3 Impact**: Primary failures have immediate impact on write operations
4. **Recovery Testing**: Validates automatic failover and recovery mechanisms

### Chaos Behavior with Primary Targeting

```
Time 0s:     [PRIMARY: cluster-1] [REPLICA: cluster-2] ← Both running
Time 180s:   Kill PRIMARY cluster-1
             [PRIMARY: DEAD     ] [REPLICA: cluster-2] ← Primary killed
Time 181s:   PostgreSQL detects failure, starts failover
             [PRIMARY: DEAD     ] [NEW PRIMARY: cluster-2] ← Replica promoted
Time 200s:   Original pod restarts
             [REPLICA: cluster-1] [PRIMARY: cluster-2] ← cluster-1 becomes replica
Time 360s:   Kill PRIMARY cluster-2 (now the primary!)
             [REPLICA: cluster-1] [PRIMARY: DEAD     ] ← New primary killed
Time 361s:   Failover back
             [NEW PRIMARY: cluster-1] [PRIMARY: DEAD] ← cluster-1 promoted again
...continues for test duration
```

## Alternative Configurations

### Option A: Random DB Pod Selection

If you want KRKN to randomly kill **either** primary OR replica:

```python
# Matches both primary and replica
("noobaa-db-pg-cluster-[0-9]+", ...)
# or
("noobaa-db-pg.*-[0-9]+", ...)
```

**Behavior**:
- KRKN randomly selects 1 pod from matching pods
- ~50% chance primary, ~50% chance replica
- Less predictable testing

### Option B: Kill Both Simultaneously

If you want to kill **both** DB pods at once (extreme testing):

```python
PodScenarios.regex_openshift_pod_kill(
    ...
    name_pattern="noobaa-db-pg-cluster-[0-9]+",
    kill=2,  # Kill 2 pods instead of 1
    ...
)
```

**Behavior**:
- Both primary and replica killed simultaneously
- Complete PostgreSQL cluster outage
- NooBaa S3 service completely unavailable
- Tests catastrophic failure recovery

### Option C: Sequential Primary + Replica Testing

Create separate test scenarios for each:

```python
@pytest.mark.parametrize(
    "target_pod,...",
    [
        ("noobaa-db-pg-cluster-1", ...),  # Test 1: Kill primary
        ("noobaa-db-pg-cluster-2", ...),  # Test 2: Kill replica
    ],
)
```

**Behavior**:
- Separate test runs for primary and replica
- More granular validation
- Better for understanding impact of each role

## Current Test Scenarios

### Basic Pod Disruption Tests

| Test ID | Target Pod | Duration | Kills | What Gets Killed |
|---------|-----------|----------|-------|------------------|
| 1 | noobaa-db-pg-cluster-1 | 20 min | ~6 | PRIMARY pod only |
| 2 | noobaa-db-pg-cluster-1 | 30 min | ~7 | PRIMARY pod only |
| 3 | noobaa-db-pg-cluster-1 | 60 min | ~12 | PRIMARY pod only |
| 4 | noobaa-core-0 | 20 min | ~6 | Core service |
| 5 | noobaa-core-0 | 30 min | ~7 | Core service |
| 6 | noobaa-operator.* | 20 min | ~6 | Operator pod |

### Strength Tests

| Test Level | Targets | What Gets Killed |
|-----------|---------|------------------|
| High | All 3 components | PRIMARY db + core + operator |
| Extreme | All 3 components | PRIMARY db + core + operator |
| Ultimate | All 3 components | PRIMARY db + core + operator |

**Note**: In strength tests, all three pods are killed **in parallel** during each iteration.

## Expected NooBaa Behavior

### When Primary DB Pod is Killed

1. **Immediate Impact** (~0-2 seconds):
   - Write operations fail
   - Read operations continue (served by replica)
   - S3 PUT/POST/DELETE fail with 503 errors

2. **Failover** (~2-5 seconds):
   - PostgreSQL replica detects primary failure
   - Replica promotes itself to primary
   - S3 operations resume

3. **Pod Recovery** (~30-60 seconds):
   - Original primary pod restarts
   - Rejoins cluster as replica
   - Full HA restored

4. **Next Kill** (after kill_interval):
   - Now kills the **current** primary (which might be cluster-2!)
   - Failover happens again

### S3 Operation Expectations During Chaos

✅ **Expected (Acceptable)**:
- Temporary 503 errors during pod restart (1-5 seconds)
- Brief S3 operation delays
- Automatic retry success after failover
- No data loss

❌ **Unexpected (Test Failure)**:
- Permanent S3 failures after 60+ seconds
- Data corruption
- Database unable to recover
- Pods stuck in CrashLoopBackOff

## Monitoring During Tests

### Watch Pod Status
```bash
# Terminal 1: Watch pod status
watch -n 2 "kubectl get pods -n openshift-storage | grep noobaa"

# You'll see:
# noobaa-db-pg-cluster-1    1/1   Running     0   5m    ← Primary
# noobaa-db-pg-cluster-2    1/1   Running     0   5m    ← Replica
# ...
# noobaa-db-pg-cluster-1    0/1   Terminating 0   8m    ← PRIMARY KILLED
# noobaa-db-pg-cluster-2    1/1   Running     0   8m    ← Replica (becomes primary)
# ...
# noobaa-db-pg-cluster-1    0/1   Pending     0   8m    ← Restarting
# noobaa-db-pg-cluster-1    1/1   Running     1   9m    ← Recovered (now replica)
```

### Check PostgreSQL Role
```bash
# Check which pod is currently primary
kubectl get pods -n openshift-storage \
    -l cnpg.io/cluster=noobaa-db-pg-cluster \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.cnpg\.io/instanceRole}{"\n"}{end}'

# Output shows:
# noobaa-db-pg-cluster-1    primary   ← Currently primary
# noobaa-db-pg-cluster-2    replica   ← Currently replica
```

### Monitor S3 Operations
```bash
# Terminal 3: Watch S3 workload logs
kubectl logs -n <test-namespace> -f pod/s3cli-0
```

## Recommendations

### For Standard Testing (Current Configuration)
✅ **Keep current setup**: Target primary pod specifically
- Most realistic failure scenario
- Tests actual failover behavior
- Validates HA works correctly

### For Extreme/Chaos Testing
Consider adding tests that:
1. Kill both DB pods simultaneously (`kill=2`)
2. Kill primary + core + operator all at once
3. Alternate between primary and replica kills

### For Production Validation
Run current tests with:
- Longer durations (60 min)
- Shorter kill intervals (120s)
- Multiple concurrent S3 workloads
- Monitor for zero data loss

## Summary

**Current Implementation**: ✅ **PRIMARY POD TARGETING**
- Targets: `noobaa-db-pg-cluster-1` (primary database pod)
- Behavior: Repeatedly kills PRIMARY pod, forcing failover to replica
- Realistic: Simulates most common production failure
- Validated: Tests automatic HA failover and recovery

The tests are correctly configured to provide realistic and meaningful chaos testing of NooBaa's high availability capabilities! 🎯
