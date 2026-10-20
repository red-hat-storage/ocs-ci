# AWS Cleanup Script Documentation

## Overview

Enhanced AWS cleanup script (`ocs_ci/cleanup/aws/cleanup.py`) for comprehensive cleanup of OpenShift clusters and their AWS resources. Handles IPI, UPI, HyperShift, and CAPI clusters with robust dependency resolution and multi-region support.

## Quick Start

```bash
# Basic cleanup - clusters older than 12 hours in default region
aws-cleanup --hours 12 --force

# Multi-region cleanup
aws-cleanup --hours 12 --region us-east-2,us-west-2 --force

# Cleanup with IAM role cleanup
aws-cleanup --hours 12 --region us-east-2 --force --cleanup-iam-roles

# Specific cluster
aws-cleanup --cluster-name my-cluster-abc123 --force
```

## Command Line Options

### Required Options

- `--force` - Skip confirmation prompt (REQUIRED for automation)
- `--hours HOURS` - Maximum cluster age in hours (minimum: 10)

### Optional Options

- `--region REGION[,REGION...]` - AWS region(s) to clean (comma-separated)
- `--cluster-name NAME` - Delete specific cluster by name
- `--cleanup-iam-roles` - Enable orphaned IAM role cleanup (disabled by default)
- `--prefix PREFIX:HOURS` - Special retention rules for cluster name prefixes
- `--sweep-buckets` - Delete S3 buckets instead of clusters

### Prefix Retention Examples

```bash
# Never delete clusters starting with "production"
aws-cleanup --hours 12 --prefix production:never --force

# Keep "staging" clusters for 48 hours
aws-cleanup --hours 12 --prefix staging:48 --force

# Multiple prefixes
aws-cleanup --hours 12 \
  --prefix prod:never \
  --prefix staging:48 \
  --prefix dev:24 \
  --force
```

## Key Features

### 1. Multi-Region Support

Clean up resources across multiple AWS regions in a single run:

```bash
aws-cleanup --hours 12 --region us-east-2,us-west-2,us-east-1 --force
```

Each region is processed sequentially with full resource cleanup.

### 2. VPC Protection Mechanisms

#### Protected VPC Names

VPCs matching protected names are never deleted:

**Location:** `ocs_ci/cleanup/aws/defaults.py`
```python
PROTECTED_VPC_NAMES = [
    "rosa-hcp-vpc",
    "production-vpc",
]
```

#### Tag-Based Protection

**VPCs or EC2 instances** tagged with `do_not_delete: true` are completely skipped:

```bash
# Protect entire VPC and all resources within it
aws ec2 create-tags --resources vpc-xxxxx --tags Key=do_not_delete,Value=true

# Protect specific instance
aws ec2 create-tags --resources i-xxxxx --tags Key=do_not_delete,Value=true
```

**Important:** VPC-level protection applies to ALL resources within the VPC.

### 3. Comprehensive VPC Cleanup

The script performs cleanup in the correct dependency order to avoid errors:

**15-Step Cleanup Process:**

1. **Terminate EC2 Instances** - All instances terminated and waited for
2. **Delete VPC Peering Connections** - Both requester and accepter roles
3. **Delete NAT Gateways** - Wait up to 5 minutes for deletion
4. **Release Elastic IPs** - NAT Gateway EIPs and other VPC-attached EIPs
5. **Delete VPC Endpoints** - Wait up to 5 minutes, skip AWS-managed (GuardDuty, etc.)
6. **Delete Load Balancers** - ELBv2 and Classic ELBs, wait 90s
7. **Detach Internet Gateways** - Retry with aggressive EIP cleanup
8. **Wait for AWS-Managed ENIs** - 60s for automatic cleanup
9. **Delete Network Interfaces** - Retry 3 times with 30s waits, skip AWS-managed ENIs
10. **Delete Route Tables** - Disassociate before deleting (except main)
11. **Delete Subnets** - All subnets in VPC
12. **Delete Security Groups** - Remove all rules first (except default)
13. **Delete Network ACLs** - Replace with default before deleting
14. **Final Cleanup Pass** - Re-check for leftover VPC endpoints and ENIs
15. **Delete VPC** - Finally delete the VPC itself

### 4. Network Interface (ENI) Handling

**Robust ENI cleanup with intelligent detection:**

- **Skip AWS-managed ENIs:**
  - NAT gateway ENIs
  - VPC endpoint ENIs
  - Load balancer ENIs (ELA attachments)
  - AWS service ENIs (GuardDuty, Macie, etc.)

- **Retry logic for "in use" ENIs:**
  - 3 retry attempts with 30s waits
  - Handles ENIs being released by parent resources
  - Detailed logging of failures

- **Orphaned VPC endpoint ENIs:**
  - Detects when VPC endpoint is deleted but ENIs remain
  - Attempts deletion of orphaned ENIs
  - Warns about ENIs blocking VPC deletion

### 5. CloudFormation Stack Cleanup

**Orphaned CloudFormation stacks are automatically cleaned up:**

- Lists all stacks in non-deleted states
- Checks stack age against threshold
- Deletes stacks in failed states (DELETE_FAILED, CREATE_FAILED, etc.)
- Handles stacks without EC2 instances
- Attempts stack deletion before falling back to direct VPC cleanup

**Stack states cleaned:**
- CREATE_COMPLETE
- ROLLBACK_COMPLETE
- UPDATE_COMPLETE / UPDATE_ROLLBACK_COMPLETE
- DELETE_FAILED
- CREATE_FAILED / ROLLBACK_FAILED

### 6. IAM Role Cleanup (Optional)

**Conservative cleanup with `--cleanup-iam-roles` flag:**

```bash
aws-cleanup --hours 12 --region us-east-2 --force --cleanup-iam-roles
```

**Safety criteria (ALL must be true to delete a role):**

1. ✅ Role name matches OpenShift cluster pattern
2. ✅ Associated cluster/VPC does not exist in ANY region
3. ✅ Role has not been used in last 7 days
4. ✅ Role age exceeds time threshold
5. ✅ Role is not an AWS service-linked role
6. ✅ Respects cluster prefix retention rules

**Detected role patterns:**
- `<cluster-id>-openshift-*`
- `<cluster-id>-worker-role`
- `<cluster-id>-master-role`
- `<cluster-id>-bootstrap-role`
- `<cluster-id>-cloud-credentials`

**Deletion process (follows AWS documentation):**
1. Remove role from instance profiles
2. Delete inline policies
3. Detach managed policies
4. Delete role
5. Delete empty instance profiles

**Important:** IAM cleanup is **disabled by default** for safety. Always review logs before enabling in automation.

## Cluster Type Detection

The script automatically detects and handles different cluster types:

### IPI Clusters
- Uses `openshift-install destroy cluster`
- Verifies VPC deletion after installer completes
- Falls back to manual cleanup if installer fails

### UPI Clusters
- Deletes CloudFormation stacks in correct order
- Handles worker, master, bootstrap, security group stacks
- Cleans up infrastructure and VPC stacks

### HyperShift/CAPI Clusters
- Auto-detected by tags: `hypershift.openshift.io/*` or `sigs.k8s.io/cluster-api-provider-aws/*`
- Uses direct VPC cleanup (no installer)
- Handles guest clusters in shared VPCs

### Orphaned CloudFormation VPCs
- VPCs with CloudFormation tags but no instances
- Attempts CloudFormation stack deletion first
- Falls back to direct VPC cleanup if stack not found

## Usage Examples

### Daily Cleanup Job

```bash
#!/bin/bash
# Cleanup clusters older than 12 hours in production regions
aws-cleanup \
  --hours 12 \
  --region us-east-2,us-west-2 \
  --prefix production:never \
  --prefix staging:48 \
  --force
```

### Cleanup with IAM Roles

```bash
# Weekly cleanup including IAM roles
aws-cleanup \
  --hours 168 \
  --region us-east-2,us-west-2 \
  --cleanup-iam-roles \
  --force
```

### Specific Cluster Cleanup

```bash
# Delete a specific cluster by name
aws-cleanup \
  --cluster-name mytest-cluster-abc123 \
  --force
```

### S3 Bucket Cleanup

```bash
# Clean up S3 buckets older than 24 hours
aws-cleanup \
  --hours 24 \
  --sweep-buckets
```

## Log Output

The script provides detailed logging at each step:

```
INFO - Running cleanup in regions: ['us-east-2', 'us-west-2']
INFO - Processing region: us-east-2
INFO - Deleting clusters in us-east-2: ['cluster1-abc123', 'cluster2-def456']
INFO - Skipping protected VPC vpc-xxxxx (name: rosa-hcp-vpc)
INFO - Detected HyperShift cluster from VPC tag: hypershift.openshift.io/cluster-name
INFO - Starting VPC cleanup for cluster-name (VPC: vpc-xxxxx)
INFO - Deleting 2 VPC peering connections
INFO - Deleting VPC Endpoint vpce-xxxxx
INFO - Waiting for VPC endpoints to be deleted...
INFO - Skipping AWS-managed VPC endpoint vpce-xxxxx (service: com.amazonaws.us-east-2.guardduty-data)
INFO - Found user-created Network Interface eni-xxxxx (requester: 123456789012)
INFO - Retrying deletion of 3 ENIs that were in use...
INFO - Successfully deleted ENI eni-xxxxx on retry
WARNING - Failed to delete 1 network interfaces after retries: ['eni-yyyyy']
INFO - Checking for orphaned CloudFormation stacks in us-east-2
INFO - Found 5 orphaned CloudFormation stacks to delete
INFO - Attempting to delete CloudFormation stack mystack-vpc
```

## Troubleshooting

### VPC Deletion Fails

**Symptom:** "Failed to clean up VPC vpc-xxxxx"

**Common causes:**
1. VPC peering connections not deleted
2. VPC endpoints still deleting
3. ENIs attached to resources outside VPC
4. NAT Gateways not fully deleted

**Solutions:**
```bash
# Check for remaining dependencies
aws ec2 describe-vpc-peering-connections --filters Name=vpc-id,Values=vpc-xxxxx
aws ec2 describe-vpc-endpoints --filters Name=vpc-id,Values=vpc-xxxxx
aws ec2 describe-network-interfaces --filters Name=vpc-id,Values=vpc-xxxxx
aws ec2 describe-nat-gateways --filter Name=vpc-id,Values=vpc-xxxxx

# Re-run cleanup - it will retry failed resources
aws-cleanup --hours 1 --region us-east-2 --force
```

### ENI Deletion Errors

**Symptom:** "Network interface is currently in use"

**Common causes:**
- ENI attached to load balancer (ELA attachment)
- VPC endpoint ENI not yet released
- NAT Gateway ENI still deleting

**The script handles these automatically:**
- Skips ELA attachments
- Retries "in use" ENIs 3 times
- Waits for parent resources to complete deletion

### CloudFormation Stack Stuck

**Symptom:** Stack in DELETE_FAILED state

**Solution:**
```bash
# Force delete from AWS console with "Retain resources" option
# Then re-run cleanup to delete the VPC
aws-cleanup --hours 1 --region us-east-2 --force
```

### IAM Role Deletion Error

**Symptom:** "Cannot delete entity, must remove roles from instance profile first"

**This is now fixed** - the script follows AWS recommended deletion order:
1. Remove from instance profiles first
2. Delete policies
3. Delete role

If you still see this error, it may indicate a race condition. Re-run the cleanup.

## Protected Resources

### What Gets Protected

- VPCs in `PROTECTED_VPC_NAMES` list
- VPCs tagged with `do_not_delete: true`
- EC2 instances tagged with `do_not_delete: true`
- Clusters matching `never` retention prefixes
- AWS service-linked IAM roles (`/aws-service-role/`)
- AWS-managed VPC endpoints (GuardDuty, Macie, etc.)

### Adding Protected VPCs

**By name (permanent):**

Edit `ocs_ci/cleanup/aws/defaults.py`:
```python
PROTECTED_VPC_NAMES = [
    "rosa-hcp-vpc",
    "production-shared-vpc",
    "do-not-delete-ever",
]
```

**By tag (runtime):**
```bash
aws ec2 create-tags \
  --resources vpc-xxxxx \
  --tags Key=do_not_delete,Value=true
```

## Known Limitations

1. **Sequential processing** - Regions are processed one at a time
2. **NAT Gateway delays** - Can take 5+ minutes to delete
3. **VPC Endpoint delays** - Can take 5+ minutes to delete
4. **AWS rate limits** - Rapid cleanup may hit API throttling
5. **IAM is global** - Role cleanup runs once after all regional cleanup
6. **No dry-run mode** - Use `--hours` threshold to limit scope

## Best Practices

1. **Start conservatively:**
   ```bash
   # First run with high threshold to test
   aws-cleanup --hours 72 --region us-east-2 --force
   ```

2. **Use prefix retention for production:**
   ```bash
   aws-cleanup --hours 12 --prefix prod:never --force
   ```

3. **Review logs** before enabling IAM cleanup

4. **Protect shared resources:**
   - Tag shared VPCs with `do_not_delete`
   - Add to `PROTECTED_VPC_NAMES` for permanent protection

5. **Monitor failed deletions:**
   - Check `failed_cluster_deletions.txt` after each run
   - Investigate failures before re-running

6. **Test in dev environment first:**
   ```bash
   # Test with specific cluster
   aws-cleanup --cluster-name test-cluster-123 --force
   ```

## Recent Enhancements

### October 2024

- ✅ Multi-region support
- ✅ VPC peering connection cleanup
- ✅ VPC endpoint verification and retry logic
- ✅ ENI retry logic (3 attempts)
- ✅ Skip AWS-managed ENIs and VPC endpoints
- ✅ Orphaned CloudFormation VPC cleanup
- ✅ CloudFormation stack cleanup
- ✅ Conservative IAM role cleanup
- ✅ Network ACL association fixes
- ✅ Route table deletion order fixes
- ✅ VPC verification after openshift-install destroy

## Future Enhancements

Potential improvements:

1. **Parallel region processing** - Clean multiple regions simultaneously
2. **Dry-run mode** - Preview what would be deleted without actually deleting
3. **Detailed reports** - Export JSON report of deleted resources
4. **Resource age tracking** - Show age of each resource before deletion
5. **Slack/email notifications** - Alert on cleanup completion or failures
6. **Cost estimation** - Estimate cost savings from cleanup
7. **Automated tagging** - Auto-tag resources created by cleanup script

## Support

For issues or questions:
- Check logs first - they contain detailed error messages
- Review this documentation for common scenarios
- Check `failed_cluster_deletions.txt` for failed clusters
- Manually inspect AWS console for dependency issues
- Re-run cleanup - it's safe to run multiple times

## Version History

- **v3.0 (Oct 2024)** - Multi-region, IAM roles, CF stacks, ENI retry logic
- **v2.0** - HyperShift/CAPI support, VPC protection
- **v1.0** - Initial implementation for IPI/UPI clusters
