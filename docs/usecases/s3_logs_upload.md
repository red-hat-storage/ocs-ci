# Uploading Logs to S3 (IBM Cloud Object Storage)

This guide explains how to automatically upload must-gather logs to IBM Cloud Object Storage (COS) and how to use the standalone CLI tool for manual uploads.

## Overview

The S3 logs upload feature allows you to:
- Automatically upload must-gather logs to IBM Cloud Object Storage after collection
- Generate presigned URLs for easy log access and sharing
- Organize logs by cluster name and timestamp
- Configure retention policies for uploaded logs
- Manually upload any log files using the standalone CLI tool

## Prerequisites

1. **IBM Cloud Object Storage (COS) instance** with:
   - A bucket created for storing logs
   - HMAC credentials (access key ID and secret access key)

2. **boto3 library** (included in ocs-ci requirements)

## Creating HMAC Credentials in IBM Cloud

1. Go to IBM Cloud Console → Resource List → Storage → Your COS instance
2. Click "Service credentials" → "New credential"
3. Enable "Include HMAC Credential" option
4. Click "Add" to create the credential
5. Copy the `access_key_id` and `secret_access_key` from the created credential

## Configuration

### Automatic Upload During Test Runs

To enable automatic upload of must-gather logs, configure your ocs-ci config file:

```yaml
REPORTING:
  # Enable automatic upload of must-gather logs to S3
  s3_logs_upload: true

AUTH:
  # S3 endpoint details for IBM Cloud Object Storage
  logs_s3_endpoint_details:
    # Name of your COS instance (optional, for reference)
    cos_name: "my-cos-instance"

    # Target bucket name where logs will be uploaded
    bucket_name: "ocs-ci-logs"

    # IBM Cloud region (e.g., 'us-south', 'us-east', 'eu-de', 'jp-tok')
    region: "us-south"

    # HMAC credentials for S3-compatible access
    cos_hmac_keys:
      access_key_id: "your-access-key-id-here"
      secret_access_key: "your-secret-access-key-here"

    # Optional: Object retention policy configuration
    retention_policy:
      min: 30      # Minimum retention period in days
      max: 730     # Maximum retention period in days (2 years)
      default: 90  # Default retention if not specified during upload
```

### Running Tests with S3 Upload Enabled

Once configured, run your tests normally:

```bash
run-ci --ocsci-conf my-config.yaml --ocsci-conf s3-config.yaml <other-options>
```

When must-gather logs are collected (on test failure or when configured), they will be automatically uploaded to S3, and the presigned download URL will be logged in the test output.

## Standalone CLI Tool

The `upload-logs-to-s3` command can be used to manually upload any log files to S3.

### Basic Usage

```bash
# Upload a must-gather tarball with default settings
upload-logs-to-s3 -f must-gather.tar.gz

# Upload with custom config file
upload-logs-to-s3 -f must-gather.tar.gz --ocsci-conf my-s3-config.yaml

# Upload with multiple config files (later ones override earlier)
upload-logs-to-s3 -f must-gather.tar.gz --ocsci-conf base.yaml --ocsci-conf s3.yaml
```

### Advanced Options

```bash
# Upload with custom prefix for organization
upload-logs-to-s3 -f must-gather.tar.gz -p "execution_123/test_case_456"

# Upload with custom retention period (180 days)
upload-logs-to-s3 -f must-gather.tar.gz -r 180

# Upload with custom URL expiration (7 days instead of default 30)
upload-logs-to-s3 -f must-gather.tar.gz -e 7

# Upload with custom object name
upload-logs-to-s3 -f must-gather.tar.gz -o custom-name.tar.gz

# Verbose mode for debugging
upload-logs-to-s3 -f must-gather.tar.gz -v

# Combine multiple options
upload-logs-to-s3 -f must-gather.tar.gz \
  --ocsci-conf s3-config.yaml \
  -p "my-execution/failed-test" \
  -r 365 \
  -e 14 \
  -v
```

### CLI Options Reference

| Option | Description | Default |
|--------|-------------|---------|
| `-f, --file` | Path to the file to upload (required) | - |
| `--ocsci-conf` | Path to ocs-ci config file (can be used multiple times) | - |
| `-p, --prefix` | Prefix for organizing files (e.g., "execution/test") | None |
| `-o, --object-name` | Custom object name | File name |
| `-e, --expiration` | URL expiration in days | 30 |
| `-r, --retention` | Object retention period in days | From config (90) |
| `-v, --verbose` | Enable verbose logging | False |

## File Organization

Logs are automatically organized in S3 with the following structure:

```
bucket-name/
├── cluster-name-1/
│   ├── 20260126_143022/
│   │   └── must-gather.tar.gz
│   └── 20260126_150315/
│       └── must-gather.tar.gz
└── cluster-name-2/
    └── 20260126_144530/
        └── must-gather.tar.gz
```

When using the standalone CLI with a custom prefix:

```
bucket-name/
└── execution_123/
    ├── test_case_456/
    │   └── must-gather.tar.gz
    └── test_case_789/
        └── must-gather.tar.gz
```

## Retention Policy

The retention policy controls how long uploaded objects are kept in the bucket:

- **min**: Minimum retention period (default: 30 days)
- **max**: Maximum retention period (default: 730 days / 2 years)
- **default**: Default retention if not specified (default: 90 days)

When uploading, you can specify a custom retention period using the `-r` option. The value will be validated against the min/max limits defined in the retention policy.

Retention information is stored in object metadata and included in upload results.

## Presigned URLs

After a successful upload, a presigned URL is generated that allows anyone with the URL to download the file without authentication. The URL expires after the specified number of days (default: 30 days).

Example output:

```
================================================================================
✓ Upload Successful!
================================================================================
File:              must-gather.tar.gz
Bucket:            ocs-ci-logs
Object Key:        my-cluster/20260126_143022/must-gather.tar.gz
Region:            us-south
Size:              45,678,901 bytes
ETag:              abc123def456...
Upload Time:       2026-01-26T14:30:22Z
Retention:         90 days (expires: 2026-04-26T14:30:22Z)
URL Expires:       2026-02-25T14:30:22Z (30 days)

Presigned URL:
--------------------------------------------------------------------------------
https://s3.us-south.cloud-object-storage.appdomain.cloud/ocs-ci-logs/...
================================================================================
```

## Troubleshooting

### S3 configuration not found

**Error:**
```
Error: S3 configuration not found or not enabled.
```

**Solution:**
- Ensure `REPORTING.s3_logs_upload` is set to `true`
- Verify `AUTH.logs_s3_endpoint_details` is properly configured
- Check that you're loading the correct config file with `--ocsci-conf`

### boto3 not installed

**Error:**
```
ImportError: boto3 is required for S3 logs upload
```

**Solution:**
```bash
pip install boto3
```

### Invalid credentials

**Error:**
```
Failed to upload file: An error occurred (InvalidAccessKeyId)
```

**Solution:**
- Verify your HMAC credentials are correct
- Ensure the credentials have write access to the bucket
- Check that the bucket exists in the specified region

### Retention validation errors

**Warning:**
```
Retention 10 days is below minimum 30, using minimum
```

This is not an error - the system automatically adjusts the retention period to fit within the configured min/max limits.

## Programmatic Usage

You can also use the S3 uploader in your Python code:

```python
from ocs_ci.utility.s3_logs_uploader import upload_logs_to_s3_if_configured

# Upload logs if S3 is configured
result = upload_logs_to_s3_if_configured(
    file_path='/path/to/must-gather.tar.gz',
    prefix='my-execution/test-case',
    retention_days=180,
    metadata={
        'test-id': 'test-123',
        'cluster-version': '4.15'
    }
)

if result and result['success']:
    print(f"Uploaded successfully: {result['presigned_url']}")
else:
    print("Upload failed or S3 not configured")
```

## Security Considerations

1. **Credentials**: Store S3 credentials securely. Never commit them to version control.
2. **Presigned URLs**: URLs are temporary and expire after the specified period.
3. **Bucket Access**: Ensure your bucket has appropriate access policies.
4. **Retention**: Configure retention policies according to your data retention requirements.

## See Also

- Configuration Documentation (`conf/README.md`) - Complete configuration reference
- [Getting Started](../getting_started.md) - General ocs-ci setup
- [Usage Guide](../usage.md) - Running tests with ocs-ci
