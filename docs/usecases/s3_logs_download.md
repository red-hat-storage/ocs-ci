# S3 Logs Download Guide

This guide explains how to download log files (especially must-gather tarballs) from IBM Cloud Object Storage (COS) using the `download-logs-from-s3` command-line tool.

## Overview

The `download-logs-from-s3` tool provides an easy way to:
- Download individual log files from S3
- Download multiple log files matching a prefix (e.g., all logs from a test run)
- Automatically extract tarball archives
- Optionally delete tarballs after extraction
- Maintain directory structure locally

## Prerequisites

1. **Python 3.6+** with boto3 installed:
   ```bash
   pip install boto3
   ```

2. **S3 Credentials**: You need IBM Cloud Object Storage HMAC credentials with read access to the bucket.

3. **Installation**: Install ocs-ci with the entrypoint:
   ```bash
   pip install -e .
   ```

## Configuration

The tool supports three ways to provide S3 credentials (in order of precedence):

### 1. Home Directory Config File (Recommended for CLI usage)

Create a file at `~/.ocs-ci-s3-logs.yaml` with your S3 credentials:

```yaml
bucket_name: "your-bucket-name"
region: "us-south"  # IBM Cloud region
access_key_id: "your-access-key-id"
secret_access_key: "your-secret-access-key"
```

**Benefits:**
- No need to specify config on every command
- Credentials stored securely in your home directory
- Works for both upload and download commands

### 2. Custom Config File

Use `--ocsci-conf` to specify a custom config file:

```bash
download-logs-from-s3 --ocsci-conf /path/to/config.yaml s3://bucket/path/file.tar.gz
```

### 3. OCS-CI Framework Config

If running within ocs-ci framework, credentials are loaded from:
```python
config.AUTH['logs_s3_endpoint_details']
```

## Usage

### Single File Download

Download a specific file from S3:

```bash
download-logs-from-s3 s3://bucket-name/path/to/file.tar.gz
```

**What happens:**
- File is downloaded to: `logs/bucket-name/path/to/file.tar.gz`
- Directory structure is created automatically
- Original S3 path structure is preserved locally

**Download to specific location:**
```bash
download-logs-from-s3 s3://bucket-name/path/to/file.tar.gz /tmp/my-logs.tar.gz
```

### Batch Download by Prefix

Download all files matching a prefix (e.g., all logs from a test run):

```bash
download-logs-from-s3 --prefix j231vu1cs33t1/1769470481959
```

**What happens:**
- Lists all objects in the bucket starting with the prefix
- Downloads each file maintaining directory structure
- Shows progress: "Downloading 2/5: path/to/file"
- Provides summary: total objects, downloaded, failed

**Example scenario:**
If your bucket contains:
```
j231vu1cs33t1/1769470481959/test_create_namespace_store_ui/20260127_060925/ocs_must_gather.tar.gz
j231vu1cs33t1/1769470481959/test_bucket_replication/20260127_031114/ocs_must_gather.tar.gz
j231vu1cs33t1/1769470481959/test_another_test/20260127_045230/ocs_must_gather.tar.gz
```

All three files will be downloaded to:
```
logs/bucket-name/j231vu1cs33t1/1769470481959/...
```

### Extract Tarballs

Automatically extract tarball files after download:

```bash
# Single file
download-logs-from-s3 s3://bucket/path/file.tar.gz --extract

# Batch download
download-logs-from-s3 --prefix j231vu1cs33t1/1769470481959 --extract
```

**Supported formats:**
- `.tar`
- `.tar.gz` / `.tgz`
- `.tar.bz2` / `.tbz2`
- `.tar.xz` / `.txz`

**What happens:**
- File is downloaded
- Tarball is extracted to the same directory
- Both tarball and extracted contents are kept (unless `--delete-archive` is used)

### Delete Archives After Extraction

Remove tarball files after successful extraction:

```bash
# Single file
download-logs-from-s3 s3://bucket/path/file.tar.gz --extract --delete-archive

# Batch download
download-logs-from-s3 --prefix j231vu1cs33t1/1769470481959 -e --delete-archive
```

**Note:** `--delete-archive` only works when `--extract` is also specified.

## Examples

### Example 1: Download Single Must-Gather

```bash
download-logs-from-s3 s3://df-logs-mg-ci-bucket-us-south-2026/run_id_999/my_failed_test1/my_ocs_mg_logs.tar.gz
```

**Output:**
```
================================================================================
✓ Download Successful!
================================================================================
S3 URI:            s3://df-logs-mg-ci-bucket-us-south-2026/run_id_999/my_failed_test1/my_ocs_mg_logs.tar.gz
Bucket:            df-logs-mg-ci-bucket-us-south-2026
Object Key:        run_id_999/my_failed_test1/my_ocs_mg_logs.tar.gz
Local Path:        logs/df-logs-mg-ci-bucket-us-south-2026/run_id_999/my_failed_test1/my_ocs_mg_logs.tar.gz
Size:              45,678,901 bytes
Extracted:         No

--------------------------------------------------------------------------------
Your logs are available in: logs/df-logs-mg-ci-bucket-us-south-2026/run_id_999/my_failed_test1/my_ocs_mg_logs.tar.gz
================================================================================
```

### Example 2: Download and Extract

```bash
download-logs-from-s3 s3://bucket/path/logs.tar.gz --extract
```

**Output:**
```
================================================================================
✓ Download Successful!
================================================================================
S3 URI:            s3://bucket/path/logs.tar.gz
Bucket:            bucket
Object Key:        path/logs.tar.gz
Local Path:        logs/bucket/path/logs.tar.gz
Size:              45,678,901 bytes
Extracted:         Yes
Extraction Path:   logs/bucket/path

--------------------------------------------------------------------------------
Your logs are available in: logs/bucket/path
================================================================================
```

### Example 3: Download All Logs from Test Run

```bash
download-logs-from-s3 --prefix j231vu1cs33t1/1769470481959 --extract --delete-archive
```

**Output:**
```
INFO:__main__:Listing objects in bucket 'my-bucket' with prefix 'j231vu1cs33t1/1769470481959'
INFO:__main__:Found 3 objects matching prefix 'j231vu1cs33t1/1769470481959'
INFO:__main__:Processing 1/3: j231vu1cs33t1/1769470481959/test1/logs.tar.gz
INFO:__main__:Successfully downloaded 45,678,901 bytes to logs/my-bucket/j231vu1cs33t1/1769470481959/test1/logs.tar.gz
INFO:__main__:Extracted to: logs/my-bucket/j231vu1cs33t1/1769470481959/test1
INFO:__main__:Deleted archive: logs/my-bucket/j231vu1cs33t1/1769470481959/test1/logs.tar.gz
INFO:__main__:Processing 2/3: j231vu1cs33t1/1769470481959/test2/logs.tar.gz
INFO:__main__:Skipping download - extracted content already exists at: logs/my-bucket/j231vu1cs33t1/1769470481959/test2
INFO:__main__:Processing 3/3: j231vu1cs33t1/1769470481959/test3/logs.tar.gz
INFO:__main__:Successfully downloaded 38,456,789 bytes to logs/my-bucket/j231vu1cs33t1/1769470481959/test3/logs.tar.gz
INFO:__main__:Extracted to: logs/my-bucket/j231vu1cs33t1/1769470481959/test3
INFO:__main__:Deleted archive: logs/my-bucket/j231vu1cs33t1/1769470481959/test3/logs.tar.gz
INFO:__main__:Download summary: 2 downloaded, 1 skipped, 0 failed out of 3 total

================================================================================
✓ Prefix Download Successful!
================================================================================
Prefix:            j231vu1cs33t1/1769470481959
Bucket:            my-bucket
Total Objects:     3
Downloaded:        2
Skipped:           1
Failed:            0
Base Path:         logs

--------------------------------------------------------------------------------
Your logs are available in: logs/my-bucket/j231vu1cs33t1/1769470481959
================================================================================
```

### Example 6: Skip Existing Files (Second Run)

```bash
# First run downloads everything
download-logs-from-s3 -p j231vu1cs33t1/1769470481959 -e -d

# Second run skips already downloaded files
download-logs-from-s3 -p j231vu1cs33t1/1769470481959 -e -d
```

**Output:**
```
INFO:__main__:Found 3 objects matching prefix 'j231vu1cs33t1/1769470481959'
INFO:__main__:Processing 1/3: j231vu1cs33t1/1769470481959/test1/logs.tar.gz
INFO:__main__:Skipping download - extracted content already exists at: logs/my-bucket/j231vu1cs33t1/1769470481959/test1
INFO:__main__:Processing 2/3: j231vu1cs33t1/1769470481959/test2/logs.tar.gz
INFO:__main__:Skipping download - extracted content already exists at: logs/my-bucket/j231vu1cs33t1/1769470481959/test2
INFO:__main__:Processing 3/3: j231vu1cs33t1/1769470481959/test3/logs.tar.gz
INFO:__main__:Skipping download - extracted content already exists at: logs/my-bucket/j231vu1cs33t1/1769470481959/test3
INFO:__main__:Download summary: 0 downloaded, 3 skipped, 0 failed out of 3 total

================================================================================
✓ Prefix Download Successful!
================================================================================
Prefix:            j231vu1cs33t1/1769470481959
Bucket:            my-bucket
Total Objects:     3
Downloaded:        0
Skipped:           3
Failed:            0
Base Path:         logs

--------------------------------------------------------------------------------
Your logs are available in: logs/my-bucket/j231vu1cs33t1/1769470481959
================================================================================
```

### Example 7: Force Re-download

```bash
# Force re-download even if files exist
download-logs-from-s3 -p j231vu1cs33t1/1769470481959 -e -d --redownload
```

### Example 8: Using Custom Config

```bash
download-logs-from-s3 --ocsci-conf my-s3-config.yaml s3://bucket/path/file.tar.gz
```

### Example 9: Verbose Output

```bash
# Long options
download-logs-from-s3 --prefix j231vu1cs33t1/1769470481959 --extract --verbose

# Short options
download-logs-from-s3 -p j231vu1cs33t1/1769470481959 -e -v
```

## Configuration File Format

### Home Config File (`~/.ocs-ci-s3-logs.yaml`)

```yaml
# Required fields
bucket_name: "df-logs-mg-ci-bucket-us-south-2026"
region: "us-south"
access_key_id: "your-hmac-access-key-id"
secret_access_key: "your-hmac-secret-access-key"

# Optional fields
cos_name: "my-cos-instance"  # For reference only
```

### Alternative Format (Nested Credentials)

```yaml
bucket_name: "df-logs-mg-ci-bucket-us-south-2026"
region: "us-south"
cos_hmac_keys:
  access_key_id: "your-hmac-access-key-id"
  secret_access_key: "your-hmac-secret-access-key"
```

### Getting IBM Cloud COS Credentials

1. Log in to IBM Cloud Console
2. Navigate to your Cloud Object Storage instance
3. Go to "Service Credentials"
4. Create new credentials with HMAC enabled
5. Copy the `access_key_id` and `secret_access_key` from the HMAC credentials section

## Command-Line Options

```
usage: download-logs-from-s3 [-h] [--prefix PREFIX] [--ocsci-conf OCSCI_CONF]
                             [-e] [-d] [-v]
                             [s3_uri] [local_path]

positional arguments:
  s3_uri                S3 URI of the file to download
  local_path            Local path where to save the file (optional)

options:
  -h, --help            Show help message and exit
  --prefix, -p PREFIX   Download all objects with this prefix
  --ocsci-conf OCSCI_CONF
                        Path to ocs-ci config file (can be used multiple times)
  -e, --extract         Extract tarball after download
  -d, --delete-archive  Delete tarball file after extraction (requires --extract)
  --redownload          Force re-download even if files already exist locally
  -v, --verbose         Enable verbose logging
```

**Short Options Summary:**
- `-p` = `--prefix`
- `-e` = `--extract`
- `-d` = `--delete-archive`
- `-v` = `--verbose`

**Note:** `--redownload` does not have a short option to avoid accidental use.

## Troubleshooting

### Error: S3 configuration not found

**Problem:**
```
Error: S3 configuration not found or not enabled.
```

**Solution:**
1. Create `~/.ocs-ci-s3-logs.yaml` with your credentials
2. Or use `--ocsci-conf` to specify a config file
3. Or ensure ocs-ci framework config is properly set up

### Error: boto3 not installed

**Problem:**
```
ImportError: boto3 is required for S3 logs download.
```

**Solution:**
```bash
pip install boto3
```

### Error: Access Denied

**Problem:**
```
ClientError: An error occurred (AccessDenied) when calling the GetObject operation
```

**Solution:**
- Verify your HMAC credentials are correct
- Ensure your credentials have read access to the bucket
- Check that the bucket name and region are correct

### Error: No such key

**Problem:**
```
ClientError: An error occurred (NoSuchKey) when calling the GetObject operation
```

**Solution:**
- Verify the S3 URI is correct
- Check that the file exists in the bucket
- Ensure you're using the correct bucket name

### Files Not Extracting

**Problem:** Files download but don't extract

**Solution:**
- Ensure you're using the `--extract` or `-e` flag
- Verify the file is actually a tarball (check extension)
- Check file permissions in the download directory

### Prefix Returns No Files

**Problem:** Using `--prefix` but no files are downloaded

**Solution:**
- Verify the prefix is correct (case-sensitive)
- Check that files with that prefix exist in the bucket
- Try listing objects in the bucket to verify the prefix format

## Tips and Best Practices

1. **Use Home Config**: Set up `~/.ocs-ci-s3-logs.yaml` for convenient CLI usage

2. **Organize Downloads**: The tool preserves S3 directory structure, making it easy to organize logs

3. **Batch Operations**: Use `--prefix` or `-p` to download all logs from a test run at once

4. **Save Space**: Use `--delete-archive` or `-d` with `--extract` or `-e` to save disk space

5. **Use Short Options**: Combine short options for concise commands: `-e -d` instead of `--extract --delete-archive`

6. **Skip Existing Files**: By default, files are skipped if they already exist - saves time and bandwidth

7. **Force Re-download When Needed**: Use `--redownload` or `-r` to refresh logs or ensure latest version

8. **Verbose Mode**: Use `-v` for detailed logging when troubleshooting

9. **Check Disk Space**: Ensure you have enough disk space before downloading large files or using `--prefix`

## Integration with OCS-CI

The download tool integrates seamlessly with ocs-ci:

```python
from ocs_ci.utility.s3_logs_downloader import download_logs_from_s3_if_configured

# Download a file if S3 is configured
result = download_logs_from_s3_if_configured(
    s3_uri="s3://bucket/path/file.tar.gz",
    extract=True,
    delete_archive=True
)

if result and result["success"]:
    print(f"Downloaded to: {result['local_path']}")
```

## Related Documentation

- [S3 Logs Upload Guide](s3_logs_upload.md)
- [IBM Cloud Object Storage Documentation](https://cloud.ibm.com/docs/cloud-object-storage)

## Support

For issues or questions:
1. Check this documentation
2. Review the troubleshooting section
3. Check ocs-ci GitHub issues
4. Contact the ocs-ci team
