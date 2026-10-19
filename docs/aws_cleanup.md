# AWS Cleanup Script Enhancements

## Summary

Enhanced the AWS cleanup script (`ocs_ci/cleanup/aws/cleanup.py`) to handle HyperShift and Cluster API (CAPI) managed clusters, and added VPC protection mechanisms.

## Key Changes

### 1. VPC Protection Mechanisms

#### Protected VPC Names
Added a list of VPC names that should never be deleted:
- `rosa-hcp-vpc` (vpc-01d80ccb49f22d798)

**Location:** `ocs_ci/cleanup/aws/defaults.py`
```python
PROTECTED_VPC_NAMES = [
    "rosa-hcp-vpc",
]
```

To add more protected VPCs, simply add them to this list.

#### Tag-Based Protection
**VPCs** tagged with `do_not_delete: true` (or `yes`/`1`) will be completely skipped during cleanup, **including all resources within them** (EC2 instances, subnets, security groups, etc.).

**Example:**
```bash
# Protect entire VPC and everything inside it
aws ec2 create-tags --resources vpc-xxxxx --tags Key=do_not_delete,Value=true
```

**Important:** The protection applies at the **VPC level**. If a VPC is protected, the cleanup script:
1. Skips the VPC entirely
2. Does NOT process any instances within the VPC
3. Does NOT delete any resources (subnets, route tables, etc.) within the VPC

This means you only need to tag the VPC itself, not individual resources.

### 2. HyperShift/CAPI Cluster Detection

The script now automatically detects HyperShift and CAPI clusters by looking for these tags:
- `hypershift.openshift.io/*` - HyperShift clusters
- `sigs.k8s.io/cluster-api-provider-aws/*` - Cluster API managed resources

### 3. Proper VPC Deletion Order

For HyperShift/CAPI clusters, the script deletes resources in the correct order to avoid dependency errors:

1. **EC2 Instances** - Terminate all instances and wait
2. **NAT Gateways** - Delete and wait (60s)
3. **Elastic IPs** - Release unattached IPs
4. **Load Balancers** - Delete ELBv2 load balancers
5. **Internet Gateways** - Detach and delete
6. **VPC Endpoints** - Delete all endpoints
7. **Network Interfaces** - Detach and delete
8. **Subnets** - Delete all subnets
9. **Route Tables** - Delete (except main)
10. **Security Groups** - Delete (except default)
11. **Network ACLs** - Delete (except default)
12. **VPC** - Finally delete the VPC itself

This order ensures that dependencies are properly handled.

## Usage

### Running the Cleanup Script

The script works the same way as before:

```bash
# Cleanup clusters older than 12 hours in us-west-2
aws-cleanup --hours 12 --region us-west-2 --force
```

### What Happens Now

1. **Protected VPCs**: VPCs matching protected names or tagged with `do_not_delete: true` are skipped
2. **HyperShift Clusters**: Detected automatically and cleaned up using direct AWS API calls
3. **Regular IPI Clusters**: Continue to use `openshift-install destroy cluster`
4. **UPI Clusters**: Continue to use CloudFormation cleanup

### Log Output

You'll see messages like:
```
INFO - VPC vpc-xxxxx (name: rosa-hcp-vpc) is protected by name pattern: rosa-hcp-vpc
INFO - Skipping protected VPC vpc-xxxxx
INFO - Detected HyperShift cluster from tag: hypershift.openshift.io/cluster-name
INFO - Detected CAPI cluster from tag: sigs.k8s.io/cluster-api-provider-aws/cluster/cl-1-infra
INFO - Starting HyperShift/CAPI cleanup for VPC vpc-xxxxx
INFO - Terminating 3 EC2 instances
INFO - Deleting NAT Gateway nat-xxxxx
INFO - Successfully cleaned up HyperShift/CAPI VPC vpc-xxxxx
```

## Adding More Protected VPCs

### By Name Pattern
Edit `ocs_ci/cleanup/aws/defaults.py`:
```python
PROTECTED_VPC_NAMES = [
    "rosa-hcp-vpc",
    "production-vpc",  # Add your VPC name here
    "do-not-delete-vpc",
]
```

### By Tag (Runtime)
Tag any VPC or resource you want to protect:
```bash
aws ec2 create-tags \
  --resources vpc-xxxxx \
  --tags Key=do_not_delete,Value=true
```

## Testing

### Test Protection Mechanisms

1. **Tag a test VPC**:
```bash
aws ec2 create-tags \
  --resources vpc-test123 \
  --tags Key=do_not_delete,Value=true
```

2. **Run cleanup**:
```bash
aws-cleanup --hours 1 --region us-west-2 --force
```

3. **Verify**: The tagged VPC should be skipped with a log message.

### Test HyperShift Cleanup

1. **Identify HyperShift cluster**: Look for instances with `hypershift.openshift.io/*` tags
2. **Run cleanup**: The script will automatically detect and use the new cleanup method
3. **Monitor logs**: Watch for "Starting HyperShift/CAPI cleanup" messages

## Known Limitations

1. **NAT Gateway deletion**: Takes ~60 seconds to complete, adding to cleanup time
2. **Nested dependencies**: Some resources may fail to delete if unexpected dependencies exist
3. **Parallel cleanup**: HyperShift cleanup runs serially (not in threads) to avoid AWS rate limits

## Troubleshooting

### VPC Deletion Fails

**Symptom**: "Failed to clean up HyperShift/CAPI VPC"

**Solutions**:
1. Check logs for which resource failed to delete
2. Manually inspect dependencies:
   ```bash
   aws ec2 describe-vpc-attribute --vpc-id vpc-xxxxx --attribute enableDnsHostnames
   aws ec2 describe-network-interfaces --filters Name=vpc-id,Values=vpc-xxxxx
   aws ec2 describe-nat-gateways --filter Name=vpc-id,Values=vpc-xxxxx
   ```
3. Re-run cleanup - it will skip already-deleted resources

### Protected VPC Not Being Skipped

**Check**:
1. VPC name exactly matches pattern in `PROTECTED_VPC_NAMES`
2. Tag is correctly set: `do_not_delete: true` (case-sensitive)
3. Check logs for "Skipping protected VPC" message

## Future Enhancements

Potential improvements:
1. Add support for other cluster types (ROSA STS, ARO, etc.)
2. Parallel HyperShift cleanup with semaphore for AWS rate limiting
3. Dry-run mode to preview what would be deleted
4. Export list of deleted resources to a report file
