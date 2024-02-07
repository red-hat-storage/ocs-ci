"""
Cloud Credential Operator utility functions
"""
import logging
import os
from ocs_ci.framework import config
from ocs_ci.utility.utils import (
    delete_file,
    download_file,
    exec_cmd,
)

logger = logging.getLogger(__name__)


def configure_cloud_credential_operator():
    """
    Extract and Prepare the CCO utility (ccoctl) binary. This utility
    allows us to create and manage cloud credentials from outside of
    the cluster while in manual mode.

    """
    bin_dir = config.RUN["bin_dir"]
    ccoctl_path = os.path.join(bin_dir, "ccoctl")
    if not os.path.isfile(ccoctl_path):
        # retrieve ccoctl binary from https://mirror.openshift.com
        version = config.DEPLOYMENT.get("ccoctl_version")
        source = f"https://mirror.openshift.com/pub/openshift-v4/clients/ocp/{version}/ccoctl-linux.tar.gz"
        bin_dir = config.RUN["bin_dir"]
        tarball = os.path.join(bin_dir, "ccoctl-linux.tar.gz")
        logger.info("Downloading ccoctl tarball from %s", source)
        download_file(source, tarball)
        cmd = f"tar -xzC {bin_dir} -f {tarball} ccoctl"
        logger.info("Extracting ccoctl binary from %s", tarball)
        exec_cmd(cmd)
        delete_file(tarball)


def get_release_image(openshift_installer):
    """
    Retrieve release image using the openshift installer.

    Args:
        openshift_installer (str): Path to the openshift installer

    Returns:
        str: Release image from the openshift installer.

    """
    logger.info("Retrieving release image")
    cmd = f"{openshift_installer} version"
    proc = exec_cmd(cmd)
    for line in proc.stdout.decode().split("\n"):
        if "release image" in line:
            return line.split(" ")[2].strip()


def create_manifests(openshift_installer, cluster_path):
    """
    Create manifests.

    Args:
        openshift_installer (str): Path to the openshift installer
        cluster_path (str): Path to the cluster directory

    """
    logger.info("Creating manifests")
    cmd = f"{openshift_installer} create manifests --dir {cluster_path}"
    exec_cmd(cmd)


def extract_credentials_requests(
    release_image, credentials_requests_dir, pull_secret_path
):
    """
    Extract the CredentialsRequests.

    Args:
        release_image (str): Release image from the openshift installer
        credentials_requests_dir (str): Path to the CredentialsRequests directory
        pull_secret_path (str): Path to the pull secret

    """
    logger.info("Extracting CredentialsRequests")
    cmd = (
        f"oc adm release extract --cloud=ibmcloud --credentials-requests {release_image} "
        f"--to={credentials_requests_dir} --registry-config={pull_secret_path}"
    )
    exec_cmd(cmd)


def create_service_id(cluster_name, cluster_path, credentials_requests_dir):
    """
    Create the Service ID.

    Args:
        cluster_name (str): Name of the cluster
        cluster_path (str): Path to the cluster directory
        credentials_requests_dir (str): Path to the credentials requests directory

    """
    logger.info("Creating service ID")
    cmd = (
        f"ccoctl ibmcloud create-service-id --credentials-requests-dir {credentials_requests_dir} "
        f"--name {cluster_name} --output-dir {cluster_path}"
    )
    exec_cmd(cmd)


def delete_service_id(cluster_name, credentials_requests_dir):
    """
    Delete the Service ID.

    Args:
        cluster_name (str): Name of the cluster
        credentials_requests_dir (str): Path to the credentials requests directory

    """
    logger.info("Deleting service ID")
    cmd = (
        f"ccoctl ibmcloud delete-service-id --credentials-requests-dir {credentials_requests_dir} "
        f"--name {cluster_name}"
    )
    exec_cmd(cmd)
