"""
Cloud Credential Operator utility functions
"""
import logging
import os
import shutil
import yaml

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


def create_manifests(openshift_installer, output_dir):
    """
    Create manifests.

    Args:
        openshift_installer (str): Path to the openshift installer
        output_dir (str): Path to the output directory

    """
    logger.info("Creating manifests")
    cmd = f"{openshift_installer} create manifests --dir {output_dir}"
    exec_cmd(cmd)


def extract_credentials_requests_ibmcloud(
    release_image, credentials_requests_dir, pull_secret_path
):
    """
    Extract the CredentialsRequests (IBM Cloud variant).

    Args:
        release_image (str): Release image from the openshift installer
        credentials_requests_dir (str): Path to the CredentialsRequests directory
        pull_secret_path (str): Path to the pull secret

    """
    logger.info("Extracting CredentialsRequests")
    cmd = (
        f"oc adm release extract --cloud=ibmcloud --credentials-requests {release_image} "
        + f"--to={credentials_requests_dir} --registry-config={pull_secret_path}"
    )
    exec_cmd(cmd)


def extract_credentials_requests_aws(
    release_image, install_config, pull_secret, credentials_requests_dir
):
    """
    Extract the CredentialsRequests (AWS STS variant).

    Args:
        release_image (str): Release image from the openshift installer
        install_config (str): Location of the install-config.yaml
        credentials_requests_dir (str): Path to the CredentialsRequests directory
    """
    logger.info("Extracting CredentialsRequests")
    cmd = (
        f"oc adm release extract --from={release_image} --credentials-requests --included "
        f"--install-config={install_config} --to={credentials_requests_dir} -a {pull_secret}"
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


def get_cco_container_image(release_image, pull_secret_path):
    """
    Obtain the CCO container image from the OCP release image.

    Args:
        release_image (str): Release image from the openshift installer
        pull_secret_path (str): Path to the pull secret

    Returns:

    """
    logger.info("Obtaining the cco container image from the OCP release image")
    cmd = f"oc adm release info --image-for='cloud-credential-operator' {release_image} -a {pull_secret_path}"
    result = exec_cmd(cmd)
    return result.stdout.decode()


def extract_ccoctl_binary(cco_image, pull_secret_path):
    """
    Extract the ccoctl binary from the CCO container image within the OCP release image.

    Args:
        cco_image (str): Release image from the openshift installer
        pull_secret_path (str): Path to the pull secret

    """
    logger.info("Extracting ccoctl from the CCO container image")
    bin_dir = config.RUN["bin_dir"]
    ccoctl_path = os.path.join(bin_dir, "ccoctl")
    if not os.path.isfile(ccoctl_path):
        extract_cmd = f"oc image extract {cco_image} --file='/usr/bin/ccoctl' -a {pull_secret_path}"
        exec_cmd(extract_cmd)
        chmod_cmd = "chmod 775 ccoctl"
        exec_cmd(chmod_cmd)
        shutil.move("ccoctl", ccoctl_path)


def process_credentials_requests_aws(
    name, aws_region, credentials_requests_dir, output_dir
):
    """
    Process all CredentialsRequest objects.

    Args:
        name (str): Name used to tag any created cloud resources
        aws_region (str): Region to create cloud resources
        credentials_requests_dir (str): Path to the CredentialsRequest directory
        output_dir (str): Path to the output directory

    """
    logger.info("Processing all CredentialsRequest objects")
    cmd = (
        f"ccoctl aws create-all --name={name} --region={aws_region} "
        f"--credentials-requests-dir={credentials_requests_dir} --output-dir={output_dir} "
        "--create-private-s3-bucket"
    )
    exec_cmd(cmd)


def set_credentials_mode_manual(install_config):
    """
    Set credentialsMode to Manual in the install-config.yaml
    """
    logger.info("Set credentialsMode to Manual")
    with open(install_config, "r") as f:
        install_config_data = yaml.safe_load(f)
        install_config_data["credentialsMode"] = "Manual"
    with open(install_config, "w") as f:
        yaml.dump(install_config_data, f)
