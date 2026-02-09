#!/usr/bin/env python3
"""
S3 Logs Downloader for IBM Cloud Object Storage

This module provides functionality to download log files from IBM Cloud Object Storage (COS).
Supports downloading from S3 URIs with automatic directory creation and optional tarball extraction.
Also supports downloading multiple files by prefix.

Can be used as a standalone script or imported as a module.
Integrated with ocs-ci configuration system.
"""

import sys
import argparse
import logging
import tarfile
from pathlib import Path
from typing import Dict, Optional, Any, List
from urllib.parse import urlparse

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


class S3LogsDownloader:
    """
    Handles downloading log files from IBM Cloud Object Storage (S3-compatible).

    Supports:
    - Downloading files from S3 URIs
    - Downloading multiple files by prefix
    - Automatic directory creation
    - Optional tarball extraction
    - Integration with ocs-ci config
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the S3 downloader with configuration.

        Args:
            config: Dictionary containing S3 endpoint details with keys:
                - access_key_id: HMAC access key (or nested in cos_hmac_keys)
                - secret_access_key: HMAC secret key (or nested in cos_hmac_keys)
                - bucket_name: Target bucket name
                - region: IBM Cloud region (e.g., 'us-south')
                - cos_name: (optional) COS instance name for reference
        """
        if boto3 is None:
            raise ImportError(
                "boto3 is required for S3 logs download. "
                "Please install it: pip install boto3"
            )

        self.config = config
        self.bucket_name = config["bucket_name"]
        self.region = config["region"]

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

    def parse_s3_uri(self, s3_uri: str) -> Dict[str, str]:
        """
        Parse S3 URI into bucket and key components.

        Args:
            s3_uri: S3 URI in format s3://bucket/key/path

        Returns:
            Dictionary with 'bucket' and 'key' fields

        Raises:
            ValueError: If URI format is invalid
        """
        parsed = urlparse(s3_uri)
        if parsed.scheme != "s3":
            raise ValueError(
                f"Invalid S3 URI scheme: {parsed.scheme}. Expected 's3://'"
            )

        if not parsed.netloc:
            raise ValueError("Invalid S3 URI: missing bucket name")

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        if not key:
            raise ValueError("Invalid S3 URI: missing object key")

        return {"bucket": bucket, "key": key}

    def validate_prefix(self, prefix: str) -> tuple[bool, str]:
        """
        Validate that prefix has minimum required path depth.

        Prefix must contain at least cluster_name/run_id/test_name (3 path components)
        to prevent downloading excessive amounts of data.

        Args:
            prefix: Prefix to validate (e.g., 'lr5026aikv23fc3t1/1770332469850/test_name')

        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if prefix is valid, False otherwise
            - error_message: Empty string if valid, error description if invalid

        Examples:
            Valid:   'lr5026aikv23fc3t1/1770332469850/my_test'
            Valid:   'lr5026aikv23fc3t1/1770332469850/session_logs/rpm_go_versions'
            Invalid: 'lr5026aikv23fc3t1'
            Invalid: 'lr5026aikv23fc3t1/1770332469850'
            Invalid: 'lr5026aikv23fc3t1/1770332469850/'
        """
        # Remove leading/trailing slashes for consistent validation
        prefix_clean = prefix.strip("/")

        if not prefix_clean:
            return False, "Prefix cannot be empty"

        # Split by '/' to count path components
        path_components = prefix_clean.split("/")

        # Must have at least 3 components: cluster_name/run_id/test_name
        if len(path_components) < 3:
            error_msg = (
                f"Prefix must contain at least cluster_name/run_id/test_name (3 path components). "
                f"Got: '{prefix}' with {len(path_components)} component(s). "
                f"Example: 'lr5026aikv23fc3t1/1770332469850/my_test_name'"
            )
            return False, error_msg

        # Check that components are not empty
        if any(not component for component in path_components):
            return False, f"Prefix contains empty path components: '{prefix}'"

        return True, ""

    def list_objects_by_prefix(
        self,
        prefix: str,
        bucket: Optional[str] = None,
    ) -> List[str]:
        """
        List all objects in the bucket that match the given prefix.

        Args:
            prefix: Prefix to filter objects (e.g., 'j231vu1cs33t1/1769470481959')
            bucket: Optional bucket name. If not provided, uses configured bucket

        Returns:
            List of object keys matching the prefix

        Raises:
            ClientError: If S3 list operation fails
        """
        if bucket is None:
            bucket = self.bucket_name

        logger.info(f"Listing objects in bucket '{bucket}' with prefix '{prefix}'")

        try:
            objects = []
            paginator = self.s3_client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects.append(obj["Key"])

            logger.info(f"Found {len(objects)} objects matching prefix '{prefix}'")
            return objects

        except ClientError as e:
            error_msg = f"Failed to list objects: {str(e)}"
            logger.error(error_msg)
            raise

    def download_file(
        self,
        s3_uri: str,
        local_path: Optional[str] = None,
        extract: bool = False,
        delete_archive: bool = False,
        redownload: bool = False,
    ) -> Dict[str, Any]:
        """
        Download a file from S3 bucket.

        Args:
            s3_uri: S3 URI in format s3://bucket/key/path
            local_path: Optional local path where to save the file.
                       If not provided, creates directory structure based on S3 path
                       under 'logs/' directory
            extract: If True and file is a tarball, extract it after download
            delete_archive: If True, delete the tarball after extraction
            redownload: If True, download even if file already exists locally

        Returns:
            Dictionary containing download details:
                - success: Boolean indicating success
                - s3_uri: Original S3 URI
                - bucket: Bucket name
                - object_key: S3 object key
                - local_path: Local file path where file was saved
                - extracted: Boolean indicating if extraction was performed
                - extraction_path: Path where files were extracted (if extracted)
                - archive_deleted: Boolean indicating if archive was deleted after extraction
                - already_exists: Boolean indicating if file was skipped because it already exists
                - size_bytes: File size in bytes
                - error: Error message if failed

        Raises:
            ValueError: If S3 URI format is invalid
            ClientError: If S3 download fails
        """
        # Parse S3 URI
        parsed = self.parse_s3_uri(s3_uri)
        bucket = parsed["bucket"]
        object_key = parsed["key"]

        logger.info(f"Downloading from s3://{bucket}/{object_key}")

        # Determine local path
        if local_path is None:
            # Create directory structure: logs/bucket/path/to/file
            local_path = Path("logs") / bucket / object_key
        else:
            local_path = Path(local_path)

        # Create parent directories if they don't exist
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if file already exists (unless redownload is True)
        if not redownload:
            # Check for extracted content first (if extraction was requested)
            if extract and self._is_tarball(local_path):
                # Check if extraction directory exists and has actual content (not just empty dirs)
                extraction_dir = local_path.parent
                if extraction_dir.exists():
                    # Check if directory has actual files (not just empty subdirectories)
                    has_actual_content = False
                    for item in extraction_dir.rglob("*"):
                        if item.is_file() and item != local_path:
                            has_actual_content = True
                            break

                    # If we found actual files, consider it already extracted
                    if has_actual_content:
                        logger.info(
                            f"Skipping download - extracted content already exists at: {extraction_dir}"
                        )
                        return {
                            "success": True,
                            "s3_uri": s3_uri,
                            "bucket": bucket,
                            "object_key": object_key,
                            "local_path": str(local_path),
                            "already_exists": True,
                            "extracted": True,
                            "extraction_path": str(extraction_dir),
                            "archive_deleted": not local_path.exists(),
                            "size_bytes": 0,
                        }

            # Check if file exists
            if local_path.exists():
                file_size = local_path.stat().st_size
                logger.info(
                    f"Skipping download - file already exists: {local_path} ({file_size:,} bytes)"
                )
                return {
                    "success": True,
                    "s3_uri": s3_uri,
                    "bucket": bucket,
                    "object_key": object_key,
                    "local_path": str(local_path),
                    "already_exists": True,
                    "extracted": False,
                    "archive_deleted": False,
                    "size_bytes": file_size,
                }

        logger.info(f"Saving to: {local_path}")

        try:
            # Download file
            self.s3_client.download_file(bucket, object_key, str(local_path))

            # Get file size
            file_size = local_path.stat().st_size

            result = {
                "success": True,
                "s3_uri": s3_uri,
                "bucket": bucket,
                "object_key": object_key,
                "local_path": str(local_path),
                "size_bytes": file_size,
                "extracted": False,
                "archive_deleted": False,
                "already_exists": False,
            }

            logger.info(f"Successfully downloaded {file_size:,} bytes to {local_path}")

            # Extract if requested and file is a tarball
            if extract and self._is_tarball(local_path):
                extraction_path = self._extract_tarball(local_path)
                result["extracted"] = True
                result["extraction_path"] = str(extraction_path)
                logger.info(f"Extracted to: {extraction_path}")

                # Delete archive if requested
                if delete_archive:
                    try:
                        local_path.unlink()
                        result["archive_deleted"] = True
                        logger.info(f"Deleted archive: {local_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete archive {local_path}: {e}")

            return result

        except ClientError as e:
            error_msg = f"Failed to download file: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "s3_uri": s3_uri,
                "bucket": bucket,
                "object_key": object_key,
                "error": error_msg,
            }
        except Exception as e:
            error_msg = f"Unexpected error during download: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "s3_uri": s3_uri,
                "bucket": bucket,
                "object_key": object_key,
                "error": error_msg,
            }

    def download_by_prefix(
        self,
        prefix: str,
        bucket: Optional[str] = None,
        local_base_path: Optional[str] = None,
        extract: bool = False,
        delete_archive: bool = False,
        redownload: bool = False,
    ) -> Dict[str, Any]:
        """
        Download all objects matching the given prefix.

        Args:
            prefix: Prefix to filter objects (e.g., 'lr5026aikv23fc3t1/1770332469850/test_name')
                   Must contain at least 3 path components: cluster_name/run_id/test_name
            bucket: Optional bucket name. If not provided, uses configured bucket
            local_base_path: Optional base path for downloads. If not provided, uses 'logs/'
            extract: If True and files are tarballs, extract them after download
            delete_archive: If True, delete tarballs after extraction
            redownload: If True, download even if files already exist locally

        Returns:
            Dictionary containing download summary:
                - success: Boolean indicating overall success
                - prefix: The prefix used
                - bucket: Bucket name
                - total_objects: Total number of objects found
                - downloaded: Number of successfully downloaded objects
                - skipped: Number of files skipped (already exist)
                - failed: Number of failed downloads
                - downloads: List of individual download results
                - base_path: Base path where files were downloaded
                - error: Error message if validation or listing failed

        Raises:
            ClientError: If S3 operations fail
        """
        if bucket is None:
            bucket = self.bucket_name

        if local_base_path is None:
            local_base_path = "logs"

        # Validate prefix before attempting download
        is_valid, error_msg = self.validate_prefix(prefix)
        if not is_valid:
            logger.warning(f"Invalid prefix: {error_msg}")
            return {
                "success": False,
                "prefix": prefix,
                "bucket": bucket,
                "error": error_msg,
            }

        logger.info(
            f"Downloading all objects with prefix '{prefix}' from bucket '{bucket}'"
        )

        try:
            # List all objects with the prefix
            object_keys = self.list_objects_by_prefix(prefix, bucket)

            if not object_keys:
                logger.warning(f"No objects found with prefix '{prefix}'")
                return {
                    "success": True,
                    "prefix": prefix,
                    "bucket": bucket,
                    "total_objects": 0,
                    "downloaded": 0,
                    "failed": 0,
                    "downloads": [],
                    "base_path": local_base_path,
                }

            # Download each object
            downloads = []
            downloaded_count = 0
            skipped_count = 0
            failed_count = 0

            for i, object_key in enumerate(object_keys, 1):
                logger.info(f"Processing {i}/{len(object_keys)}: {object_key}")

                # Construct S3 URI
                s3_uri = f"s3://{bucket}/{object_key}"

                # Construct local path maintaining directory structure
                local_path = Path(local_base_path) / bucket / object_key

                # Download the file
                result = self.download_file(
                    s3_uri=s3_uri,
                    local_path=str(local_path),
                    extract=extract,
                    delete_archive=delete_archive,
                    redownload=redownload,
                )

                downloads.append(result)

                if result["success"]:
                    if result.get("already_exists"):
                        skipped_count += 1
                    else:
                        downloaded_count += 1
                else:
                    failed_count += 1

            summary = {
                "success": failed_count == 0,
                "prefix": prefix,
                "bucket": bucket,
                "total_objects": len(object_keys),
                "downloaded": downloaded_count,
                "skipped": skipped_count,
                "failed": failed_count,
                "downloads": downloads,
                "base_path": local_base_path,
            }

            logger.info(
                f"Download summary: {downloaded_count} downloaded, {skipped_count} "
                f"skipped, {failed_count} failed out of {len(object_keys)} total"
            )

            return summary

        except ClientError as e:
            error_msg = f"Failed to download by prefix: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "prefix": prefix,
                "bucket": bucket,
                "error": error_msg,
            }
        except Exception as e:
            error_msg = f"Unexpected error during prefix download: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "prefix": prefix,
                "bucket": bucket,
                "error": error_msg,
            }

    def _is_tarball(self, file_path: Path) -> bool:
        """
        Check if file is a tarball based on extension.

        Args:
            file_path: Path to the file

        Returns:
            True if file appears to be a tarball
        """
        tarball_extensions = [
            ".tar",
            ".tar.gz",
            ".tgz",
            ".tar.bz2",
            ".tbz2",
            ".tar.xz",
            ".txz",
        ]
        return any(str(file_path).endswith(ext) for ext in tarball_extensions)

    def _extract_tarball(self, tarball_path: Path) -> Path:
        """
        Extract tarball to the same directory.

        Args:
            tarball_path: Path to the tarball file

        Returns:
            Path to the extraction directory

        Raises:
            tarfile.TarError: If extraction fails
        """
        extraction_dir = tarball_path.parent
        logger.info(f"Extracting {tarball_path.name} to {extraction_dir}")

        with tarfile.open(tarball_path, "r:*") as tar:
            tar.extractall(path=extraction_dir)

        return extraction_dir


def load_config_from_home():
    """
    Load S3 configuration from ~/.ocs-ci-s3-logs.yaml if it exists.

    Returns:
        Dictionary containing S3 configuration, or None if file doesn't exist
    """
    import yaml

    home_config_path = Path.home() / ".ocs-ci-s3-logs.yaml"

    if not home_config_path.exists():
        return None

    try:
        with open(home_config_path, "r") as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded S3 config from {home_config_path}")
            return config
    except Exception as e:
        logger.error(f"Failed to load config from {home_config_path}: {e}")
        return None


def get_s3_config_from_ocs_ci():
    """
    Get S3 configuration from ocs-ci config object.

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

        return s3_config
    except Exception as e:
        logger.error(f"Failed to get S3 config from ocs-ci: {e}")
        return None


def download_logs_from_s3_if_configured(
    s3_uri: str,
    local_path: Optional[str] = None,
    extract: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Download logs from S3 if configured in ocs-ci config.

    This is a convenience function that checks if S3 download is enabled
    and credentials are available before attempting download.

    Args:
        s3_uri: S3 URI in format s3://bucket/key/path
        local_path: Optional local path where to save the file
        extract: If True and file is a tarball, extract it after download

    Returns:
        Download result dictionary if successful, None if S3 not configured or download failed
    """
    try:
        s3_config = get_s3_config_from_ocs_ci()
        if not s3_config:
            logger.debug("S3 download not configured, skipping")
            return None

        downloader = S3LogsDownloader(s3_config)
        result = downloader.download_file(
            s3_uri=s3_uri,
            local_path=local_path,
            extract=extract,
        )

        if result["success"]:
            logger.info(f"Successfully downloaded logs from S3: {result['s3_uri']}")
            logger.info(f"Local path: {result['local_path']}")
        else:
            logger.error(f"Failed to download logs from S3: {result.get('error')}")

        return result
    except Exception as e:
        logger.error(f"Error during S3 download: {e}", exc_info=True)
        return None


def main():
    """
    Main function for standalone CLI usage.
    """
    parser = argparse.ArgumentParser(
        description="Download log files from IBM Cloud Object Storage (S3)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download a single file (uses ocs-ci config for credentials)
  %(prog)s s3://df-logs-mg-ci-bucket-us-south-2026/run_id_999/my_failed_test1/my_ocs_mg_logs.tar.gz

  # Download with custom config file
  %(prog)s s3://bucket/path/file.tar.gz --ocsci-conf my-config.yaml

  # Download to specific local path
  %(prog)s s3://bucket/path/file.tar.gz /tmp/my-logs.tar.gz

  # Download and extract tarball
  %(prog)s s3://bucket/path/file.tar.gz --extract

  # Download all files with a prefix (minimum 3 path components required)
  %(prog)s --prefix lr5026aikv23fc3t1/1770332469850/my_test_name

  # Download all files with prefix and extract
  %(prog)s --prefix lr5026aikv23fc3t1/1770332469850/session_logs -e
        """,
    )

    parser.add_argument(
        "s3_uri",
        nargs="?",
        help="S3 URI of the file to download (e.g., s3://bucket/path/file.tar.gz). "
        "Not required if using --prefix",
    )

    parser.add_argument(
        "local_path",
        nargs="?",
        default=None,
        help="Local path where to save the file (optional). "
        "If not provided, creates directory structure under logs/ based on S3 path",
    )

    parser.add_argument(
        "--prefix",
        "-p",
        help="Download all objects with this prefix. "
        "Must include at least cluster_name/run_id/test_name (3 path components). "
        "Example: 'lr5026aikv23fc3t1/1770332469850/my_test_name'. "
        "When used, s3_uri argument is not required",
    )

    parser.add_argument(
        "--ocsci-conf",
        action="append",
        default=[],
        help="Path to ocs-ci config file (can be used multiple times)",
    )

    parser.add_argument(
        "-e",
        "--extract",
        action="store_true",
        help="Extract tarball after download (if file is a tarball)",
    )

    parser.add_argument(
        "-d",
        "--delete-archive",
        action="store_true",
        help="Delete tarball file after extraction (only works with --extract)",
    )

    parser.add_argument(
        "-r",
        "--redownload",
        action="store_true",
        help="Force re-download even if files already exist locally",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.prefix and not args.s3_uri:
        parser.error("Either s3_uri or --prefix must be provided")

    if args.prefix and args.s3_uri:
        parser.error("Cannot use both s3_uri and --prefix at the same time")

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

        # Get configuration - try home config first, then ocs-ci config
        s3_config = None

        # Try loading from home directory config file
        if not args.ocsci_conf:
            s3_config = load_config_from_home()
            if s3_config:
                logger.info("Using S3 config from ~/.ocs-ci-s3-logs.yaml")

        # If no home config, try ocs-ci config
        if not s3_config:
            s3_config = get_s3_config_from_ocs_ci()

        if not s3_config:
            print("\nError: S3 configuration not found or not enabled.")
            print("Please ensure one of the following:")
            print("  1. Create ~/.ocs-ci-s3-logs.yaml with S3 credentials")
            print("  2. Use --ocsci-conf to specify a config file")
            print("  3. Configure config.AUTH['logs_s3_endpoint_details'] in ocs-ci")
            if args.ocsci_conf:
                print(f"\nChecked config file(s): {args.ocsci_conf}")
            return 1

        # Initialize downloader
        downloader = S3LogsDownloader(s3_config)

        # Download by prefix or single file
        if args.prefix:
            # Validate prefix before attempting download
            is_valid, error_msg = downloader.validate_prefix(args.prefix)
            if not is_valid:
                print("\n" + "=" * 80)
                print("⚠ WARNING: Invalid Prefix")
                print("=" * 80)
                print(f"\n{error_msg}")
                print(
                    "\nDownload aborted to prevent downloading excessive amounts of data."
                )
                print("\nPlease provide a more specific prefix with at least:")
                print("  - cluster_name/run_id/test_name")
                print("\nExample:")
                print(
                    "  %(prog)s --prefix lr5026aikv23fc3t1/1770332469850/my_test_name"
                    % {"prog": parser.prog}
                )
                print("=" * 80 + "\n")
                return 1

            # Download all files with prefix
            result = downloader.download_by_prefix(
                prefix=args.prefix,
                local_base_path=args.local_path,
                extract=args.extract,
                delete_archive=args.delete_archive,
                redownload=args.redownload,
            )

            # Print results
            if result["success"]:
                print("\n" + "=" * 80)
                print("✓ Prefix Download Successful!")
                print("=" * 80)
                print(f"Prefix:            {result['prefix']}")
                print(f"Bucket:            {result['bucket']}")
                print(f"Total Objects:     {result['total_objects']}")
                print(f"Downloaded:        {result['downloaded']}")
                print(f"Skipped:           {result['skipped']}")
                print(f"Failed:            {result['failed']}")
                print(f"Base Path:         {result['base_path']}")
                print("\n" + "-" * 80)
                print(
                    f"Your logs are available in: {result['base_path']}/{result['bucket']}/{result['prefix']}"
                )
                print("=" * 80 + "\n")
                return 0
            else:
                print("\n" + "=" * 80)
                print("✗ Prefix Download Failed!")
                print("=" * 80)
                print(f"Error: {result.get('error', 'Unknown error')}")
                print("=" * 80 + "\n")
                return 1
        else:
            # Download single file
            result = downloader.download_file(
                s3_uri=args.s3_uri,
                local_path=args.local_path,
                extract=args.extract,
                delete_archive=args.delete_archive,
                redownload=args.redownload,
            )

            # Print results
            if result["success"]:
                print("\n" + "=" * 80)
                print("✓ Download Successful!")
                print("=" * 80)
                print(f"S3 URI:            {result['s3_uri']}")
                print(f"Bucket:            {result['bucket']}")
                print(f"Object Key:        {result['object_key']}")
                print(f"Local Path:        {result['local_path']}")
                print(f"Size:              {result['size_bytes']:,} bytes")

                if result.get("already_exists"):
                    print("Already Exists:    Yes (skipped download)")

                if result.get("extracted"):
                    print("Extracted:         Yes")
                    print(f"Extraction Path:   {result['extraction_path']}")
                    if result.get("archive_deleted"):
                        print("Archive Deleted:   Yes")
                    print("\n" + "-" * 80)
                    print(f"Your logs are available in: {result['extraction_path']}")
                else:
                    print("Extracted:         No")
                    print("\n" + "-" * 80)
                    print(f"Your logs are available in: {result['local_path']}")

                print("=" * 80 + "\n")
                return 0
            else:
                print("\n" + "=" * 80)
                print("✗ Download Failed!")
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

# Made with Bob
