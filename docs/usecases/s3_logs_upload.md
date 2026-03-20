# Uploading Logs to S3-compatible Storage

This guide explains how to automatically upload must-gather logs to S3-compatible storage and how to use the standalone CLI tool for manual uploads.

## Overview

The S3 logs upload feature allows you to:
- Automatically upload must-gather logs to S3-compatible storage after collection
- Generate presigned URLs for easy log access and sharing
- Organize logs by cluster name and timestamp
- Configure retention policies for uploaded logs
- Manually upload any log files using the standalone CLI tool

## Prerequisites

1. **S3-compatible storage** with:
   - A bucket created for storing logs
   - Credentials (access key ID and secret access key)
   - S3 endpoint URL

2. **Installation**: Install ocs-ci with the entrypoint. See the Installing section in [Getting Started](../getting_started.md) for detailed installation instructions.

## Getting S3 Credentials

### For IBM Cloud Object Storage

1. Go to IBM Cloud Console → Resource List → Storage → Your COS instance
2. Click "Service credentials" → "New credential"
3. Enable "Include HMAC Credential" option
4. Click "Add" to create the credential
5. Copy the `access_key_id` and `secret_access_key` from the created credential
6. Use endpoint URL format: `https://s3.{region}.cloud-object-storage.appdomain.cloud`

### For Other S3-compatible Providers

Consult your provider's documentation for:
- Access key ID and secret access key generation
- Endpoint URL format

Examples:
- **AWS S3:** `https://s3.{region}.amazonaws.com` (e.g., `https://s3.us-east-1.amazonaws.com`)
- **MinIO:** `http://minio-server:9000` or your MinIO endpoint
- **NooBaa:** Your NooBaa S3 endpoint URL

**Note on credential format:** The flat credential format (`access_key_id` and `secret_access_key` at the top level) is recommended as it works with all S3-compatible providers. The nested `cos_hmac_keys` format is IBM Cloud specific but supported for backward compatibility.

## Configuration

### Configuration Methods

The S3 logs uploader supports multiple configuration methods:

1. **ocs-ci config files** - For integration with test runs
2. **Home directory config** - `~/.ocs-ci-s3-logs.yaml` for standalone CLI usage
3. **Command-line config** - Using `--ocsci-conf` flag with the CLI tool

### Automatic Upload During Test Runs

To enable automatic upload of must-gather logs, configure your ocs-ci config file:

```yaml
REPORTING:
  # Enable automatic upload of must-gather logs to S3
  s3_logs_upload: true

AUTH:
  # S3 endpoint details for S3-compatible storage
  logs_s3_endpoint_details:
    # Target bucket name where logs will be uploaded
    bucket_name: "ocs-ci-logs"

    # S3 endpoint URL
    # For IBM Cloud: https://s3.{region}.cloud-object-storage.appdomain.cloud
    # For AWS: https://s3.{region}.amazonaws.com
    # For NooBaa/MinIO/other providers: consult their documentation
    endpoint_url: "https://s3.us-south.cloud-object-storage.appdomain.cloud"

    # S3 credentials (flat format - works with all providers)
    access_key_id: "your-access-key-id-here"
    secret_access_key: "your-secret-access-key-here"

    # Optional: Name of your storage instance (for reference)
    cos_name: "my-cos-instance"

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

When must-gather logs are collected (on test failure or when configured), they will be automatically uploaded to S3, and the object location information will be logged in the test output.

### Home Directory Configuration (Optional)

For convenient standalone CLI usage, you can create a configuration file in your home directory:

**`~/.ocs-ci-s3-logs.yaml`:**
```yaml
# S3 endpoint details for S3-compatible storage
bucket_name: "ocs-ci-logs"
endpoint_url: "https://s3.us-south.cloud-object-storage.appdomain.cloud"

# S3 credentials (flat format - works with all providers)
access_key_id: "your-access-key-id-here"
secret_access_key: "your-secret-access-key-here"

# Optional: Storage instance name (for reference)
cos_name: "my-cos-instance"

# Optional: Retention policy
retention_policy:
  min: 30
  max: 730
  default: 90
```

**Alternative format for IBM Cloud users (nested credentials):**
```yaml
bucket_name: "ocs-ci-logs"
endpoint_url: "https://s3.us-south.cloud-object-storage.appdomain.cloud"
cos_hmac_keys:
  access_key_id: "your-access-key-id-here"
  secret_access_key: "your-secret-access-key-here"
```

With this file in place, you can use the CLI tool without specifying `--ocsci-conf`:

```bash
upload-logs-to-s3 -f must-gather.tar.gz
```

## Standalone CLI Tool

The `upload-logs-to-s3` command can be used to manually upload log files or directories to S3.

### Basic Usage

```bash
# Upload a file using ~/.ocs-ci-s3-logs.yaml (if it exists)
upload-logs-to-s3 -f must-gather.tar.gz

# Upload a directory (automatically creates tarball)
upload-logs-to-s3 -f /path/to/logs-directory

# Upload directory and delete the created tarball after upload
upload-logs-to-s3 -f /path/to/logs-directory --delete

# Upload with custom config file
upload-logs-to-s3 -f must-gather.tar.gz --ocsci-conf my-s3-config.yaml

# Upload with multiple config files (later ones override earlier)
upload-logs-to-s3 -f must-gather.tar.gz --ocsci-conf base.yaml --ocsci-conf s3.yaml
```

**Note:** The CLI tool checks for configuration in this order:
1. If `--ocsci-conf` is provided, uses those config files
2. Otherwise, tries `~/.ocs-ci-s3-logs.yaml` in your home directory
3. Falls back to ocs-ci framework configuration (if available)

### Advanced Options

```bash
# Upload with custom prefix for organization
upload-logs-to-s3 -f must-gather.tar.gz -p "execution_123/test_case_456"

# Upload with custom retention period (180 days)
upload-logs-to-s3 -f must-gather.tar.gz -r 180

# Upload with custom object name
upload-logs-to-s3 -f must-gather.tar.gz -o custom-name.tar.gz

# Upload a directory with custom options
upload-logs-to-s3 -f /path/to/logs-dir -p "execution_123" -r 180 --delete

# Verbose mode for debugging
upload-logs-to-s3 -f must-gather.tar.gz -v

# Combine multiple options
upload-logs-to-s3 -f must-gather.tar.gz \
  --ocsci-conf s3-config.yaml \
  -p "my-execution/failed-test" \
  -r 365 \
  -v
```

### Directory Upload Behavior

When you provide a directory path to the `-f` option:

1. **Automatic Tarball Creation**: The tool automatically creates a `.tar.gz` tarball from the directory
2. **Temporary Storage**: By default, the tarball is created in the system's temp directory
3. **Automatic Cleanup**: The temporary tarball is automatically deleted after successful upload
4. **Manual Cleanup**: Use the `--delete` flag to explicitly delete the tarball after upload (useful when you want to ensure cleanup even if the tarball is created in a custom location)

**Example workflow:**
```bash
# Directory structure
/path/to/logs-directory/
├── test1.log
├── test2.log
└── subdir/
    └── test3.log

# Upload command
upload-logs-to-s3 -f /path/to/logs-directory

# What happens:
# 1. Creates /tmp/logs-directory.tar.gz (temporary tarball)
# 2. Uploads the tarball to S3
# 3. Automatically deletes /tmp/logs-directory.tar.gz
```

### CLI Options Reference

| Option | Description | Default |
|--------|-------------|---------|
| `-f, --file` | Path to the file or directory to upload (required). If a directory is provided, it will be automatically compressed to a .tar.gz file | - |
| `--ocsci-conf` | Path to ocs-ci config file (can be used multiple times) | - |
| `-p, --prefix` | Prefix for organizing files (e.g., "execution/test") | None |
| `-o, --object-name` | Custom object name | File/directory name |
| `-r, --retention` | Object retention period in days | From config (90) |
| `-d, --delete` | Delete the tarball after successful upload (only applies when uploading a directory) | False |
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

## Upload Results

After a successful upload, detailed information about the uploaded object is displayed, including the S3 URI and object location details needed for retrieval.

### Example Output for File Upload

```
================================================================================
✓ Upload Successful!
================================================================================
File:              must-gather.tar.gz
Bucket:            ocs-ci-logs
Object Key:        my-cluster/20260126_143022/must-gather.tar.gz
S3 URI:            s3://ocs-ci-logs/my-cluster/20260126_143022/must-gather.tar.gz
Size:              45,678,901 bytes
ETag:              abc123def456...
Upload Time:       2026-01-26T14:30:22Z
Retention:         90 days (expires: 2026-04-26T14:30:22Z)

Object Location Information:
--------------------------------------------------------------------------------
To retrieve this object later, use:
  Bucket: ocs-ci-logs
  Key:    my-cluster/20260126_143022/must-gather.tar.gz
================================================================================
```

### Example Output for Directory Upload

```
================================================================================
✓ Upload Successful!
================================================================================
Directory:         /path/to/logs-directory
Tarball Created:   /tmp/logs-directory.tar.gz
Tarball Deleted:   Yes
Bucket:            ocs-ci-logs
Object Key:        execution_123/logs-directory.tar.gz
S3 URI:            s3://ocs-ci-logs/execution_123/logs-directory.tar.gz
Size:              12,345,678 bytes
ETag:              xyz789abc123...
Upload Time:       2026-02-02T16:15:30Z
Retention:         90 days (expires: 2026-05-03T16:15:30Z)

Object Location Information:
--------------------------------------------------------------------------------
To retrieve this object later, use:
  Bucket: ocs-ci-logs
  Key:    execution_123/logs-directory.tar.gz
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
- Check that the bucket exists and the endpoint URL is correct

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
2. **Bucket Access**: Ensure your bucket has appropriate access policies.
3. **Retention**: Configure retention policies according to your data retention requirements.

## See Also

- Configuration Documentation (`conf/README.md`) - Complete configuration reference
- [Getting Started](../getting_started.md) - General ocs-ci setup
- [Usage Guide](../usage.md) - Running tests with ocs-ci
