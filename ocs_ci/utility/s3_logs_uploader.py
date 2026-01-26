#!/usr/bin/env python3
"""
S3 Logs Uploader for IBM Cloud Object Storage

This module provides functionality to upload log files (especially must-gather tarballs)
to IBM Cloud Object Storage (COS) and generate presigned URLs for easy access.

Can be used as a standalone script or imported as a module.
Integrated with ocs-ci configuration system.
"""

import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Any

try:
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    Config = None
    ClientError = None

# Configure logging
logger = logging.getLogger(__name__)


class S3LogsUploader:
    """
    Handles uploading log files to IBM Cloud Object Storage (S3-compatible).

    Supports:
    - Uploading files with optional prefix (for organizing files)
    - Generating presigned URLs with configurable expiration
    - Returning detailed upload information
    - Integration with ocs-ci config
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the S3 uploader with configuration.

        Args:
            config: Dictionary containing S3 endpoint details with keys:
                - access_key_id: HMAC access key (or nested in cos_hmac_keys)
                - secret_access_key: HMAC secret key (or nested in cos_hmac_keys)
                - bucket_name: Target bucket name
                - region: IBM Cloud region (e.g., 'us-south')
                - cos_name: (optional) COS instance name for reference
                - retention_policy: (optional) Dict with min, max, default retention days
        """
        if boto3 is None:
            raise ImportError(
                "boto3 is required for S3 logs upload. "
                "Please install it: pip install boto3"
            )

        self.config = config
        self.bucket_name = config["bucket_name"]
        self.region = config["region"]

        # Load retention policy if available
        self.retention_policy = config.get(
            "retention_policy", {"min": 30, "max": 730, "default": 90}
        )

        logger.info(
            f"Retention policy: min={self.retention_policy['min']}, "
            f"max={self.retention_policy['max']}, default={self.retention_policy['default']} days"
        )

        # Construct the S3 endpoint URL for IBM Cloud
        # Format: https://s3.{region}.cloud-object-storage.appdomain.cloud
        endpoint_url = f"https://s3.{self.region}.cloud-object-storage.appdomain.cloud"

        # Support both nested and flat credential structure
        if "cos_hmac_keys" in config:
            access_key = config["cos_hmac_keys"]["access_key_id"]
            secret_key = config["cos_hmac_keys"]["secret_access_key"]
        else:
            access_key = config["access_key_id"]
            secret_key = config["secret_access_key"]

        # Initialize boto3 S3 client with IBM Cloud COS configuration
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            config=Config(signature_version="s3v4"),
        )

        logger.info(
            f"Initialized S3 client for bucket '{self.bucket_name}' in region '{self.region}'"
        )

    def _validate_retention_days(self, retention_days: Optional[int]) -> int:
        """
        Validate and return retention days within policy limits.

        Args:
            retention_days: Requested retention period in days, or None for default

        Returns:
            Validated retention days within min/max limits
        """
        if retention_days is None:
            retention_days = self.retention_policy["default"]

        min_days = self.retention_policy["min"]
        max_days = self.retention_policy["max"]

        if retention_days < min_days:
            logger.warning(
                f"Retention {retention_days} days is below minimum {min_days}, using minimum"
            )
            return min_days
        elif retention_days > max_days:
            logger.warning(
                f"Retention {retention_days} days exceeds maximum {max_days}, using maximum"
            )
            return max_days

        return retention_days

    def upload_file(
        self,
        file_path: str,
        prefix: Optional[str] = None,
        object_name: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        retention_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Upload a file to S3 bucket with optional retention policy.

        Args:
            file_path: Path to the file to upload
            prefix: Optional prefix to organize files (e.g., 'execution_name/test_case_name')
            object_name: Optional custom object name. If not provided, uses the file name
            metadata: Optional metadata to attach to the object
            retention_days: Optional retention period in days. If not specified, uses default from policy.
                           Will be validated against min/max limits.

        Returns:
            Dictionary containing upload details:
                - success: Boolean indicating success
                - object_key: Full S3 object key (including prefix)
                - bucket: Bucket name
                - region: Region
                - size_bytes: File size in bytes
                - etag: S3 ETag
                - upload_timestamp: ISO format timestamp
                - retention_days: Applied retention period
                - retention_expires_at: ISO format timestamp when retention expires
                - error: Error message if failed

        Raises:
            FileNotFoundError: If the file doesn't exist
            ClientError: If S3 upload fails
        """
        # Validate and get retention days
        validated_retention = self._validate_retention_days(retention_days)
        # Validate file exists
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Determine object name
        if object_name is None:
            object_name = file_path.name

        # Construct full object key with prefix
        if prefix:
            # Remove leading/trailing slashes and ensure proper format
            prefix = prefix.strip("/")
            object_key = f"{prefix}/{object_name}"
        else:
            object_key = object_name

        # Get file size
        file_size = file_path.stat().st_size

        logger.info(
            f"Uploading '{file_path}' to 's3://{self.bucket_name}/{object_key}' ({file_size} bytes)"
        )
        logger.info(f"Retention period: {validated_retention} days")

        try:
            # Prepare extra args
            extra_args = {}
            if metadata:
                extra_args["Metadata"] = metadata

            # Add retention information to metadata
            if "Metadata" not in extra_args:
                extra_args["Metadata"] = {}
            extra_args["Metadata"]["retention-days"] = str(validated_retention)

            # Calculate retention expiration
            retention_expires = datetime.utcnow() + timedelta(days=validated_retention)
            extra_args["Metadata"]["retention-expires-at"] = (
                retention_expires.isoformat() + "Z"
            )

            # Upload file
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                object_key,
                ExtraArgs=extra_args if extra_args else None,
            )

            # Get object metadata to retrieve ETag
            head_response = self.s3_client.head_object(
                Bucket=self.bucket_name, Key=object_key
            )

            upload_info = {
                "success": True,
                "object_key": object_key,
                "bucket": self.bucket_name,
                "region": self.region,
                "size_bytes": file_size,
                "etag": head_response.get("ETag", "").strip('"'),
                "upload_timestamp": datetime.utcnow().isoformat() + "Z",
                "content_type": head_response.get(
                    "ContentType", "application/octet-stream"
                ),
                "last_modified": (
                    head_response.get("LastModified").isoformat()
                    if head_response.get("LastModified")
                    else None
                ),
                "retention_days": validated_retention,
                "retention_expires_at": retention_expires.isoformat() + "Z",
            }

            logger.info(
                f"Successfully uploaded to '{object_key}' with {validated_retention} days retention"
            )
            return upload_info

        except ClientError as e:
            error_msg = f"Failed to upload file: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "object_key": object_key,
                "bucket": self.bucket_name,
                "error": error_msg,
            }

    def generate_presigned_url(
        self, object_key: str, expiration_days: Optional[int] = None
    ) -> Optional[str]:
        """
        Generate a presigned URL for downloading an object.

        Args:
            object_key: S3 object key (including any prefix)
            expiration_days: Number of days until URL expires. If not specified, uses default from retention policy.

        Returns:
            Presigned URL string, or None if generation fails
        """
        # Use retention policy default if not specified
        if expiration_days is None:
            expiration_days = self.retention_policy["default"]

        try:
            expiration_seconds = expiration_days * 24 * 60 * 60

            url = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": object_key},
                ExpiresIn=expiration_seconds,
            )

            logger.info(
                f"Generated presigned URL for '{object_key}' (expires in {expiration_days} days)"
            )
            return url

        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {str(e)}")
            return None

    def upload_and_get_url(
        self,
        file_path: str,
        prefix: Optional[str] = None,
        object_name: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        retention_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Upload a file and generate a presigned URL in one operation.

        Args:
            file_path: Path to the file to upload
            prefix: Optional prefix to organize files
            object_name: Optional custom object name
            metadata: Optional metadata to attach to the object
            retention_days: Optional retention period in days. If not specified, uses default from policy.
                           This value is used for both file retention and URL expiration.

        Returns:
            Dictionary containing upload details and presigned URL:
                - All fields from upload_file() (including retention info)
                - presigned_url: URL for downloading the file
                - url_expiration_days: Number of days until URL expires
                - url_expires_at: ISO format timestamp of URL expiration
        """
        # Use retention policy default if not specified
        if retention_days is None:
            retention_days = self.retention_policy["default"]

        # Upload the file
        upload_result = self.upload_file(
            file_path=file_path,
            prefix=prefix,
            object_name=object_name,
            metadata=metadata,
            retention_days=retention_days,
        )

        # If upload succeeded, generate presigned URL using same retention period
        if upload_result["success"]:
            presigned_url = self.generate_presigned_url(
                object_key=upload_result["object_key"], expiration_days=retention_days
            )

            upload_result["presigned_url"] = presigned_url
            upload_result["url_expiration_days"] = retention_days

            # Calculate expiration timestamp
            expiration_time = datetime.utcnow() + timedelta(days=retention_days)
            upload_result["url_expires_at"] = expiration_time.isoformat() + "Z"

        return upload_result


def get_s3_config_from_ocs_ci():
    """
    Get S3 configuration from ocs-ci config object.

    Combines S3 endpoint details from AUTH section with retention policy from REPORTING section.

    Returns:
        Dictionary containing S3 configuration, or None if not available
    """
    try:
        from ocs_ci.framework import config as ocsci_config

        # Get S3 endpoint details from AUTH section
        s3_config = ocsci_config.AUTH.get("logs_s3_endpoint_details")
        if not s3_config:
            logger.warning(
                "S3 endpoint details not found in AUTH.logs_s3_endpoint_details"
            )
            return None

        # Get retention policy from REPORTING section
        retention_policy = ocsci_config.REPORTING.get("s3_logs_retention_policy")
        if retention_policy:
            s3_config["retention_policy"] = retention_policy
            logger.debug(
                f"Using retention policy from REPORTING config: {retention_policy}"
            )
        else:
            logger.debug(
                "No retention policy found in REPORTING config, using defaults"
            )

        return s3_config
    except Exception as e:
        logger.error(f"Failed to get S3 config from ocs-ci: {e}")
        return None


def upload_logs_to_s3_if_configured(
    file_path: str,
    prefix: Optional[str] = None,
    object_name: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None,
    retention_days: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Upload logs to S3 if configured in ocs-ci config.

    This is a convenience function that checks if S3 upload is enabled
    and credentials are available before attempting upload.

    Args:
        file_path: Path to the file to upload
        prefix: Optional prefix to organize files
        object_name: Optional custom object name
        metadata: Optional metadata to attach to the object
        retention_days: Optional retention period in days

    Returns:
        Upload result dictionary if successful, None if S3 not configured or upload failed
    """
    try:
        s3_config = get_s3_config_from_ocs_ci()
        if not s3_config:
            logger.debug("S3 upload not configured, skipping")
            return None

        uploader = S3LogsUploader(s3_config)
        result = uploader.upload_and_get_url(
            file_path=file_path,
            prefix=prefix,
            object_name=object_name,
            metadata=metadata,
            retention_days=retention_days,
        )

        if result["success"]:
            logger.info(f"Successfully uploaded logs to S3: {result['presigned_url']}")
        else:
            logger.error(f"Failed to upload logs to S3: {result.get('error')}")

        return result
    except Exception as e:
        logger.error(f"Error during S3 upload: {e}", exc_info=True)
        return None


def main():
    """
    Main function for standalone CLI usage.
    """
    parser = argparse.ArgumentParser(
        description="Upload log files to IBM Cloud Object Storage (S3)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload a file with default settings (uses ocs-ci config)
  %(prog)s -f must-gather.tar.gz

  # Upload with custom config file
  %(prog)s -f must-gather.tar.gz --ocsci-conf my-config.yaml

  # Upload with custom prefix for organization
  %(prog)s -f must-gather.tar.gz -p "execution_123/test_case_456"

  # Upload with custom retention period (affects both file retention and URL expiration)
  %(prog)s -f must-gather.tar.gz -r 180

  # Upload with custom object name
  %(prog)s -f must-gather.tar.gz -o my-custom-name.tar.gz
        """,
    )

    parser.add_argument(
        "-f", "--file", required=True, help="Path to the file to upload"
    )

    parser.add_argument(
        "--ocsci-conf",
        action="append",
        default=[],
        help="Path to ocs-ci config file (can be used multiple times)",
    )

    parser.add_argument(
        "-p",
        "--prefix",
        help='Prefix for organizing files (e.g., "execution_name/test_case_name")',
    )

    parser.add_argument(
        "-o", "--object-name", help="Custom object name (default: use file name)"
    )

    parser.add_argument(
        "-r",
        "--retention",
        type=int,
        help="Object retention period in days (default: from config retention policy). "
        "Used for both file retention and URL expiration. "
        "Will be validated against min/max limits from retention policy.",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    try:
        # Load config files if provided
        if args.ocsci_conf:
            from ocs_ci.utility.framework.initialization import load_config

            load_config(args.ocsci_conf)
            logger.info(f"Loaded config files: {args.ocsci_conf}")

        # Get configuration from ocs-ci
        s3_config = get_s3_config_from_ocs_ci()
        if not s3_config:
            print("\nError: S3 configuration not found or not enabled.")
            print("Please ensure:")
            print("  1. config.AUTH['logs_s3_endpoint_details'] is configured")
            if args.ocsci_conf:
                print(f"  2. Check your config file(s): {args.ocsci_conf}")
            return 1

        # Initialize uploader
        uploader = S3LogsUploader(s3_config)

        # Upload file and get URL
        result = uploader.upload_and_get_url(
            file_path=args.file,
            prefix=args.prefix,
            object_name=args.object_name,
            retention_days=args.retention,
        )

        # Print results
        if result["success"]:
            print("\n" + "=" * 80)
            print("✓ Upload Successful!")
            print("=" * 80)
            print(f"File:              {args.file}")
            print(f"Bucket:            {result['bucket']}")
            print(f"Object Key:        {result['object_key']}")
            print(f"Region:            {result['region']}")
            print(f"Size:              {result['size_bytes']:,} bytes")
            print(f"ETag:              {result['etag']}")
            print(f"Upload Time:       {result['upload_timestamp']}")
            print(
                f"Retention:         {result['retention_days']} days (expires: {result['retention_expires_at']})"
            )
            print(
                f"URL Expires:       {result['url_expires_at']} ({result['url_expiration_days']} days)"
            )
            print("\nPresigned URL:")
            print("-" * 80)
            print(result["presigned_url"])
            print("=" * 80 + "\n")
            return 0
        else:
            print("\n" + "=" * 80)
            print("✗ Upload Failed!")
            print("=" * 80)
            print(f"Error: {result.get('error', 'Unknown error')}")
            print("=" * 80 + "\n")
            return 1

    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=args.verbose)
        print(f"\nError: {str(e)}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
