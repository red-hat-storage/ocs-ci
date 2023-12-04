"""
This module contains functionality required for CNV installation.
"""
import io
import os
import logging
import tempfile
import platform
import requests
import zipfile
import tarfile

from ocs_ci.framework import config
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.ocs.constants import (
    CNV_NAMESPACE_YAML,
    CNV_OPERATORGROUP_YAML,
    CNV_SUBSCRIPTION_YAML,
    CNV_CATALOG_SOURCE_YAML,
    CNV_HYPERCONVERGED_YAML,
)
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.node import wait_for_nodes_status

logger = logging.getLogger(__name__)


class CNVInstaller(object):
    """
    CNV Installer class for CNV deployment
    """

    def __init__(self):
        self.namespace = constants.CNV_NAMESPACE

    def create_cnv_catalog_source(self):
        """
        Creates a nightly catalogsource manifest for CNV operator deployment from quay registry.

        """
        logger.info("Adding CatalogSource for CNV")
        cnv_catalog_source_data = templating.load_yaml(CNV_CATALOG_SOURCE_YAML)
        cnv_catalog_source_name = cnv_catalog_source_data.get("metadata").get("name")
        cnv_image_tag = config.DEPLOYMENT.get("ocs_csv_channel")[-4:]
        cnv_catalog_source_data["spec"][
            "image"
        ] = f"{constants.CNV_QUAY_NIGHTLY_IMAGE}:{cnv_image_tag}"
        cnv_catalog_source_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="cnv_catalog_source_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            cnv_catalog_source_data, cnv_catalog_source_manifest.name
        )
        run_cmd(f"oc apply -f {cnv_catalog_source_manifest.name}", timeout=2400)
        cnv_catalog_source = CatalogSource(
            resource_name=cnv_catalog_source_name,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )

        # Wait for catalog source is ready
        cnv_catalog_source.wait_for_state("READY")

    def create_cnv_namespace(self):
        """
        Creates the namespace for CNV resources

        Raises:
            CommandFailed: If the 'oc create' command fails.

        """
        try:
            logger.info(f"Creating namespace {self.namespace} for CNV resources")
            namespace_yaml_file = templating.load_yaml(CNV_NAMESPACE_YAML)
            namespace_yaml = OCS(**namespace_yaml_file)
            namespace_yaml.create()
            logger.info(f"CNV namespace {self.namespace} was created successfully")
        except exceptions.CommandFailed as ef:
            if (
                f'project.project.openshift.io "{self.namespace}" already exists'
                in str(ef)
            ):
                logger.info(f"Namespace {self.namespace} already present")
                raise ef

    def create_cnv_operatorgroup(self):
        """
        Creates an OperatorGroup for CNV

        """
        operatorgroup_yaml_file = templating.load_yaml(CNV_OPERATORGROUP_YAML)
        operatorgroup_yaml = OCS(**operatorgroup_yaml_file)
        operatorgroup_yaml.create()
        logger.info("CNV OperatorGroup created successfully")

    def create_cnv_subscription(self):
        """
        Creates subscription for CNV operator

        """
        # Create an operator group for CNV
        logger.info("Creating OperatorGroup for CNV")
        self.create_cnv_operatorgroup()
        cnv_subscription_yaml_data = templating.load_yaml(CNV_SUBSCRIPTION_YAML)
        cnv_channel_version = config.DEPLOYMENT.get("ocs_csv_channel")[-4:]
        cnv_sub_channel = f"nightly-{cnv_channel_version}"
        cnv_subscription_yaml_data["spec"]["channel"] = f"{cnv_sub_channel}"
        cnv_subscription_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="cnv_subscription_manifest", delete=False
        )
        templating.dump_data_to_temp_yaml(
            cnv_subscription_yaml_data, cnv_subscription_manifest.name
        )
        logger.info("Creating subscription for CNV operator")
        run_cmd(f"oc create -f {cnv_subscription_manifest.name}")
        self.wait_for_the_resource_to_discover(
            kind=constants.SUBSCRIPTION,
            namespace=self.namespace,
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
        )
        wait_for_install_plan_and_approve(self.namespace)
        cnv_package_manifest = PackageManifest(
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
            subscription_plan_approval="Manual",
            install_plan_namespace=self.namespace,
        )
        # Wait for package manifest is ready
        cnv_package_manifest.wait_for_resource(
            resource_name=constants.KUBEVIRT_HYPERCONVERGED, timeout=300
        )
        csv = get_csvs_start_with_prefix(
            csv_prefix=constants.KUBEVIRT_HCO_PREFIX, namespace=self.namespace
        )
        csv_name = csv[0]["metadata"]["name"]
        csv_obj = CSV(resource_name=csv_name, namespace=self.namespace)
        csv_obj.wait_for_phase(phase="Succeeded", timeout=720)

    def wait_for_the_resource_to_discover(self, kind, namespace, resource_name):
        """
        Waits for the specified resource to be discovered.

        Args:
            kind (str): The type of the resource to wait for.
            namespace (str): The namespace in which to wait for the resource.
            resource_name (str): The name of the resource to wait for.

        """
        logger.info(f"Waiting for resource {kind} to be discovered")
        for sample in TimeoutSampler(300, 10, ocp.OCP, kind=kind, namespace=namespace):
            resources = sample.get().get("items", [])
            for resource in resources:
                found_resource_name = resource.get("metadata", {}).get("name", "")
                if resource_name in found_resource_name:
                    logger.info(f"{kind} found: {found_resource_name}")
                    return
                logger.debug(f"Still waiting for the {kind}: {resource_name}")

    def deploy_hyper_converged(self):
        """
        Deploys the HyperConverged CR.

        Raises:
            TimeoutExpiredError: If the HyperConverged resource does not become available within the specified time.

        """
        logger.info("Deploying the HyperConverged CR")
        hyperconverged_yaml_file = templating.load_yaml(CNV_HYPERCONVERGED_YAML)
        hyperconverged_yaml = OCS(**hyperconverged_yaml_file)
        hyperconverged_yaml.create()

        # Verify the installation was completed successfully by checking the HyperConverged CR
        ocp = OCP(kind=constants.HYPERCONVERGED, namespace=self.namespace)
        # Wait for the HyperConverged resource named 'kubevirt-hyperconverged'
        # to be in the 'Available' condition within a timeout of 600 seconds.
        result = ocp.wait(
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
            condition="Available",
            timeout=600,
        )
        if not result:
            err_str = (
                "Timeout occurred, or the HyperConverged resource did not become available "
                "within the specified time."
            )
            raise exceptions.TimeoutExpiredError(err_str)
        logger.info(
            f"{constants.HYPERCONVERGED} {constants.KUBEVIRT_HYPERCONVERGED} met condition: Available"
        )

    def enable_software_emulation(self):
        """
        Enable software emulation. This is needed on a cluster where the nodes do not support hardware emulation.

        Note that software emulation, when enabled, is only used as a fallback when hardware emulation
        is not available. Hardware emulation is always attempted first, regardless of the value of the
        useEmulation.

        """
        if config.ENV_DATA["platform"].lower() == "baremetal" and config.DEPLOYMENT.get(
            "local_storage"
        ):
            logger.info("Skipping enabling software emulation")
        else:
            logger.info("Enabling software emulation on the cluster")
            ocp = OCP(kind=constants.HYPERCONVERGED, namespace=self.namespace)
            annonation = (
                'kubevirt.kubevirt.io/jsonpatch=\'[{ "op": "add", "path": "/spec/configuration/developerConfiguration",'
                ' "value": { "useEmulation": true } }]\''
            )
            ocp.annotate(
                annotation=annonation, resource_name=constants.KUBEVIRT_HYPERCONVERGED
            )
            logger.info("successfully enabled software emulation on the cluster")

    def post_install_verification(self):
        """
        Performs CNV post-installation verification.

        Raises:
            TimeoutExpiredError: If the verification conditions are not met within the timeout.
            HyperConvergedHealthException: If the HyperConverged cluster health is not healthy.

        """
        # Validate that all the nodes are ready and CNV pods are running
        logger.info("Validate that all the nodes are ready and CNV pods are running")
        wait_for_nodes_status()
        wait_for_pods_to_be_running(namespace=self.namespace)

        # Verify that all the deployments in the openshift-cnv namespace to be in the 'Available' condition
        logger.info(f"Verify all the deployments status in {self.namespace}")
        ocp = OCP(kind="deployments", namespace=self.namespace)
        result = ocp.wait(
            condition="Available", timeout=600, selector=constants.CNV_SELECTOR
        )
        if not result:
            err_str = "Timeout occurred, or one or more deployments did not meet condition: Available."
            raise exceptions.TimeoutExpiredError(err_str)
        logger.info(
            f"All the deployments in the {self.namespace} namespace met condition: Available"
        )

        # validate that HyperConverged systemHealthStatus is healthy
        logger.info("Validate that HyperConverged systemHealthStatus is healthy")
        ocp = OCP(kind=constants.HYPERCONVERGED, namespace=self.namespace)
        health = (
            ocp.get(resource_name=constants.KUBEVIRT_HYPERCONVERGED)
            .get("status")
            .get("systemHealthStatus")
        )
        if health == "healthy":
            logger.info("HyperConverged cluster health is healthy.")
        else:
            raise exceptions.HyperConvergedHealthException(
                f"HyperConverged cluster is not healthy. Health: {health}"
            )

    def get_virtctl_console_spec_links(self):
        """
        Retrieve the specification links for the virtctl client.

        Returns:
            List[dict]: A list of dictionaries containing specification links.

        Raises:
            exceptions.ResourceNotFoundError: If virtctl ConsoleCLIDownload is not found.

        """
        logger.info("Retrieving the specification links for the virtctl client")
        ocp = OCP(
            kind=constants.CONSOLECLIDOWNLOAD,
            resource_name=constants.VIRTCTL_CLI_DOWNLOADS,
            namespace=self.namespace,
        )
        virtctl_console_cli_downloads_spec_links = ocp.get().get("spec").get("links")

        if virtctl_console_cli_downloads_spec_links:
            logger.info(
                "successfully retrieved the specification links for the virtctl client"
            )
            return virtctl_console_cli_downloads_spec_links
        raise exceptions.ResourceNotFoundError(
            f"{constants.VIRTCTL_CLI_DOWNLOADS} {constants.CONSOLECLIDOWNLOAD} not found"
        )

    def get_virtctl_all_console_links(self):
        """
        Get all the URL links from virtctl specification links.

        Returns:
            List[str]: A list of virtctl download URLs.

        Raises:
            exceptions.ResourceNotFoundError: If no URL entries are found.

        """
        logger.info("Getting all the URL links from virtctl specification links")
        virtctl_console_cli_downloads_spec_links = self.get_virtctl_console_spec_links()
        virtctl_all_urls = [
            entry["href"] for entry in virtctl_console_cli_downloads_spec_links
        ]

        if virtctl_all_urls:
            logger.info(
                "Successfully pulled all the URL links from virtctl specification links"
            )
            return virtctl_all_urls
        raise exceptions.ResourceNotFoundError(
            "No URL entries found in the virtctl console cli download"
            f"spec links: {virtctl_console_cli_downloads_spec_links}"
        )

    def get_virtctl_download_url(self, os_type, os_machine_type):
        """
        Get the virtctl download URL based on the specified platform and architecture.

        Parameters:
            os_type (str): The operating system.
            os_machine_type (str): The operating system machine architecture.

        Returns:
            Optional[str]: The virtctl download URL if found, otherwise None.

        """
        logger.info(f"Getting the virtctl download URL for {os_type} {os_machine_type}")
        virtctl_console_cli_downloads_all_spec_links = (
            self.get_virtctl_console_spec_links()
        )
        for link_info in virtctl_console_cli_downloads_all_spec_links:
            text = link_info.get("text", "")
            href = link_info.get("href", "")
            if (
                os_type.lower() in text.lower()
                and os_machine_type.lower() in text.lower()
            ):
                logger.info(
                    f"The virtctl download URL for {os_type.lower()} {os_machine_type.lower()} is: {link_info['href']}"
                )
                return href
        return None

    def check_virtctl_compatibility(self):
        """
        Check if the virtctl binary is compatible with the current system.

        Raises:
            exceptions.ArchitectureNotSupported: If virtctl is not compatible.

        """
        logger.info("Performing virtctl binary compatibility check...")
        os_type = platform.system()
        os_machine_type = platform.machine()
        virtctl_download_url = self.get_virtctl_download_url(
            os_type=os_type, os_machine_type=os_machine_type
        )
        os_machine_type = "amd64" if os_machine_type == "x86_64" else os_machine_type
        if (
            not virtctl_download_url
            or (os_type.lower() not in virtctl_download_url)
            or (os_machine_type.lower() not in virtctl_download_url)
        ):
            raise exceptions.ArchitectureNotSupported(
                f"Virtctl is NOT compatible to run on this machine: {os_type} {platform.machine()}"
            )

        logger.info(
            f"Virtctl is compatible to run on this machine: {os_type} {platform.machine()}"
        )

    def download_and_extract_virtctl_binary(self, bin_dir=None):
        """
        Download and extract the virtctl binary to bin_dir

        Args:
            bin_dir (str): The directory to store the virtctl binary.

        """
        os_type = platform.system()
        os_machine_type = platform.machine()
        self.check_virtctl_compatibility()

        # Prepare bin directory for virtctl
        bin_dir_rel_path = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
        bin_dir = os.path.abspath(bin_dir_rel_path)
        virtctl_binary_path = os.path.join(bin_dir, "virtctl")
        if os.path.isfile(virtctl_binary_path):
            logger.info(
                f"virtctl binary already exists {virtctl_binary_path}, skipping download."
            )
        else:
            (
                archive_file_binary_data,
                virtctl_download_url,
            ) = self._download_virtctl_archive(os_type, os_machine_type)
            self._extract_virtctl_binary(
                archive_file_binary_data, virtctl_download_url, bin_dir
            )

    def _download_virtctl_archive(self, os_type, os_machine_type):
        """
        Download the virtctl binary archive.

        Args:
            os_type (str): The operating system.
            os_machine_type (str): The operating system machine architecture.

        Returns:
        Tuple(io.BytesIO, str): Binary data of the downloaded archive and the virtctl download URL.

        Raises:
            exceptions.RequestFailed: If the download request fails.

        """
        logger.info("Downloading virtctl archive file")
        virtctl_download_url = self.get_virtctl_download_url(
            os_type=os_type, os_machine_type=os_machine_type
        )
        try:
            # Download the archive
            response = requests.get(virtctl_download_url, verify=False)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to download archive from {virtctl_download_url}. Error: {e}"
            )
            raise

        # Create a BytesIO object from the binary content
        archive_file_binary_data = io.BytesIO(initial_bytes=response.content)
        logger.info(
            f"Successfully downloaded the virtctl file from url:{virtctl_download_url}"
        )

        return archive_file_binary_data, virtctl_download_url

    def _get_archive_file_binary_object(
        self, archive_file_binary_data, virtctl_download_url
    ):
        """
        Get the archive file binary object based on the file extension.

        Args:
            archive_file_binary_data (io.BytesIO): Binary data of the downloaded archive.
            virtctl_download_url (str): The URL of the virtctl download.

        Returns:
            Union[zipfile.ZipFile, tarfile.TarFile]: Returns the archive file binary object of either
            zipfile.ZipFile or tarfile.TarFile type based on the file extension.

        """
        zip_file_extension = ".zip"
        if virtctl_download_url.endswith(zip_file_extension):
            return zipfile.ZipFile(file=archive_file_binary_data)
        else:
            return tarfile.open(fileobj=archive_file_binary_data, mode="r")

    def _extract_virtctl_binary(
        self, archive_file_binary_data, virtctl_download_url, bin_dir
    ):
        """
        Extract the virtctl binary from the archive.

        Args:
            archive_file_binary_data (io.BytesIO): Binary data of the downloaded archive.
            virtctl_download_url (str): The URL of the virtctl download.
            bin_dir (str): The directory to store the virtctl binary.

        """
        archive_file_binary_object = self._get_archive_file_binary_object(
            archive_file_binary_data, virtctl_download_url
        )
        archive_file_binary_object.extractall(path=bin_dir)
        logger.info(f"virtctl binary extracted successfully to path:{bin_dir}")

    def deploy_cnv(self):
        """
        Installs CNV enabling software emulation.

        """
        logger.info("Installing CNV")
        # Create CNV catalog source
        self.create_cnv_catalog_source()
        # Create openshift-cnv namespace
        self.create_cnv_namespace()
        # create CNV subscription
        self.create_cnv_subscription()
        # Deploy the HyperConverged CR
        self.deploy_hyper_converged()
        # Post CNV installation checks
        self.post_install_verification()
        # Enable software emulation
        self.enable_software_emulation()
        # Download and extract the virtctl binary to bin_dir
        self.download_and_extract_virtctl_binary()
