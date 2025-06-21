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
import time

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
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
from ocs_ci.utility.utils import (
    run_cmd,
    exec_cmd,
    get_running_ocp_version,
)
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.ocs.resources.install_plan import wait_for_install_plan_and_approve
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.utility.retry import retry, catch_exceptions
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import ocp
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.node import wait_for_nodes_status
from pkg_resources import parse_version

logger = logging.getLogger(__name__)


class CNVInstaller(object):
    """
    CNV Installer class for CNV deployment
    """

    def __init__(self):
        self.namespace = constants.CNV_NAMESPACE
        self.cnv_nightly_catsrc = "cnv-nightly-catalog-source"

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
        try:
            operatorgroup_yaml.create()
            logger.info("CNV OperatorGroup created successfully")
        except exceptions.CommandFailed as ef:
            if "kubevirt-hyperconverged-group already exists" in str(ef):
                logger.info("kubevirt-hyperconverged-group already exists")

    def create_cnv_subscription(self):
        """
        Creates subscription for CNV operator

        """
        # Create an operator group for CNV
        logger.info("Creating OperatorGroup for CNV")
        self.create_cnv_operatorgroup()
        cnv_subscription_yaml_data = templating.load_yaml(CNV_SUBSCRIPTION_YAML)

        if config.DEPLOYMENT.get("cnv_latest_stable"):
            cnv_subscription_yaml_data["spec"][
                "source"
            ] = constants.OPERATOR_CATALOG_SOURCE_NAME
            cnv_sub_channel = "stable"
        else:
            cnv_channel_version = config.DEPLOYMENT.get("ocs_csv_channel")[-4:]
            cnv_sub_channel = f"nightly-{cnv_channel_version}"

        cnv_subscription_yaml_data["spec"]["channel"] = f"{cnv_sub_channel}"
        cnv_subscription_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="cnv_subscription_manifest", delete=False
        )
        # namespace attribute can be set in a child object. We avoid this behavior by assigning ns to the one from yaml
        self.namespace = cnv_subscription_yaml_data.get("metadata").get("namespace")
        templating.dump_data_to_temp_yaml(
            cnv_subscription_yaml_data, cnv_subscription_manifest.name
        )
        logger.info("Creating subscription for CNV operator")
        retry(exceptions.CommandFailed, tries=25, delay=60, backoff=1)(run_cmd)(
            f"oc apply -f {cnv_subscription_manifest.name}"
        )
        self.wait_for_the_resource_to_discover(
            kind=constants.SUBSCRIPTION_WITH_ACM,
            namespace=self.namespace,
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
        )
        wait_for_install_plan_and_approve(self.namespace, timeout=1500)
        cnv_package_manifest = PackageManifest(
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
            subscription_plan_approval="Manual",
            install_plan_namespace=self.namespace,
        )
        # Wait for package manifest is ready
        cnv_package_manifest.wait_for_resource(
            resource_name=constants.KUBEVIRT_HYPERCONVERGED, timeout=600
        )
        # csv sometimes takes more time to discover
        for csv in TimeoutSampler(
            timeout=900,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=constants.KUBEVIRT_HCO_PREFIX,
            namespace=self.namespace,
        ):
            if csv:
                break
        csv_name = csv[0]["metadata"]["name"]
        csv_obj = CSV(resource_name=csv_name, namespace=self.namespace)
        csv_obj.wait_for_phase(phase="Succeeded", timeout=900)

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

    def catalog_source_created(self, catalogsource_name=None):
        """
        Check if catalog source is created

        Args:
            catalogsource_name (str): Name of the catalogsource

        Returns:
            bool: True if catalog source is created, False otherwise
        """
        if not catalogsource_name:
            catalogsource_name = self.cnv_nightly_catsrc
        return CatalogSource(
            resource_name=self.cnv_nightly_catsrc,
            namespace=constants.MARKETPLACE_NAMESPACE,
        ).check_resource_existence(
            timeout=60,
            should_exist=True,
            resource_name=catalogsource_name,
        )

    @catch_exceptions((CommandFailed))
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
        if config.ENV_DATA[
            "platform"
        ].lower() in constants.BAREMETAL_PLATFORMS and config.DEPLOYMENT.get(
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

    def cnv_hyperconverged_installed(self):
        """
        Check if CNV HyperConverged is already installed.
        Returns:
             bool: True if CNV HyperConverged is installed, False otherwise
        """
        ocp = OCP(kind=constants.ROOK_OPERATOR, namespace=self.namespace)
        return ocp.check_resource_existence(
            timeout=12, should_exist=True, resource_name=constants.CNV_OPERATORNAME
        )

    def post_install_verification(self, raise_exception=False):
        """
        Performs CNV post-installation verification, with raise_exception = False may be used safely to run on
        clusters with CNV installed or not installed.

        Args:
            raise_exception: If True, allow function to fail the job and raise an exception. If false, return False
        instead of raising an exception.

        Returns:
            bool: True if the verification conditions are met, False otherwise
        Raises:
            TimeoutExpiredError: If the verification conditions are not met within the timeout
            and raise_exception is True.
            HyperConvergedHealthException: If the HyperConverged cluster health is not health
            and raise_exception is True.
            ResourceNotFoundError if the namespace does not exist and raise_exception is True.
            ResourceWrongStatusException if the nodes are not ready, verification fails and raise_exception
            is True.
        """
        # Validate that all the nodes are ready and CNV pods are running
        logger.info("Validate that all the nodes are ready and CNV pods are running")

        try:
            OCP(kind="namespace").get(self.namespace)
        except exceptions.CommandFailed:
            if raise_exception:
                raise exceptions.ResourceNotFoundError(
                    f"Namespace {self.namespace} does not exist"
                )
            else:
                logger.warning(f"Namespace {self.namespace} does not exist")
                return False

        try:
            wait_for_nodes_status()
            logger.info("All the nodes are in 'Ready' state")
        except exceptions.ResourceWrongStatusException:
            if raise_exception:
                raise
            else:
                logger.warning("Not all nodes are in 'Ready' state")
                return False

        if wait_for_pods_to_be_running(namespace=self.namespace, timeout=600):
            logger.info("All CNV pods are running")
        else:
            if raise_exception:
                raise exceptions.ResourceWrongStatusException(
                    "Not all CNV pods are running"
                )
            else:
                logger.warning("Not all CNV pods are running")
                return False

        # Verify that all the deployments in the openshift-cnv namespace to be in the 'Available' condition
        logger.info(f"Verify all the deployments status in {self.namespace}")
        ocp = OCP(kind="deployments", namespace=self.namespace)

        try:
            ocp.wait(
                condition="Available", timeout=600, selector=constants.CNV_SELECTOR
            )
        except exceptions.TimeoutExpiredError:
            if raise_exception:
                raise exceptions.TimeoutExpiredError(
                    "Timeout occurred, one or more deployments did not meet condition: Available"
                )
            else:
                logger.warning(
                    "Timeout occurred, or one or more deployments did not meet condition: Available"
                )
                return False

        logger.info(
            f"All the deployments in the {self.namespace} namespace met condition: Available"
        )

        # validate that HyperConverged systemHealthStatus is healthy
        return self.check_hyperconverged_healthy(raise_exception=raise_exception)

    def check_hyperconverged_healthy(self, raise_exception=True):
        """
        Validate that HyperConverged systemHealthStatus is healthy.
        Method throws an exception if the status is not healthy.

        Args:
            raise_exception: If True, allow the verification to fail the job and raise an exception if the
            verification fails, otherwise return False.
        Returns:
            bool: True if the status is healthy, False otherwise.
        """
        logger.info("Validate that HyperConverged systemHealthStatus is healthy")
        ocp = OCP(kind=constants.HYPERCONVERGED, namespace=self.namespace)

        try:
            health = (
                ocp.get(resource_name=constants.KUBEVIRT_HYPERCONVERGED)
                .get("status")
                .get("systemHealthStatus")
            )
            if health == "healthy":
                logger.info("HyperConverged cluster health is healthy.")
                return True
            elif health != "healthy" and raise_exception:
                raise exceptions.HyperConvergedHealthException(
                    f"HyperConverged cluster is not healthy. Health: {health}"
                )
            else:
                logger.warning(
                    f"HyperConverged cluster is not healthy. Health: {health}"
                )
                return False
        except Exception as ef:
            if raise_exception:
                raise exceptions.HyperConvergedHealthException(
                    f"Failed to get the HyperConverged systemHealthStatus. Error: {ef}"
                )
            else:
                logger.warning("Failed to get the HyperConverged systemHealthStatus")
                return False

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
        if os_type == "Darwin":
            os_type = "mac"
        if os_machine_type == "arm64":
            os_machine_type = "arm 64"
        virtctl_download_url = self.get_virtctl_download_url(
            os_type=os_type, os_machine_type=os_machine_type
        )
        os_machine_type = (
            "amd64"
            if os_machine_type == "x86_64"
            else "arm64" if os_machine_type == "arm 64" else os_machine_type
        )
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
        if os_type == "Darwin":
            os_type = "mac"
        if os_machine_type == "arm64":
            os_machine_type = "arm 64"
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
            response = retry(requests.exceptions.RequestException, tries=10, delay=30)(
                requests.get
            )(virtctl_download_url, verify=False)
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

    def deploy_cnv(self, check_cnv_deployed=False, check_cnv_ready=False):
        """
        Installs CNV enabling software emulation.

        Args:
            check_cnv_deployed (bool): If True, check if CNV is already deployed. If so, skip the deployment.
            check_cnv_ready (bool): If True, check if CNV is ready. If so, skip the deployment.
        """
        if check_cnv_deployed:
            if self.cnv_hyperconverged_installed():
                logger.info("CNV operator is already deployed, skipping the deployment")
                return

        if check_cnv_ready:
            if self.post_install_verification(raise_exception=False):
                logger.info("CNV operator ready, skipping the deployment")
                return

        logger.info("Installing CNV")
        # we create catsrc with nightly builds only if config.DEPLOYMENT does not have cnv_latest_stable
        if not config.DEPLOYMENT.get("cnv_latest_stable"):
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

    def disable_multicluster_engine(self):
        """
        Disable multicluster engine on cluster
        """
        logger.info("Disabling multicluster engine")
        cmd = (
            "oc patch mce multiclusterengine "
            '-p \'{"spec":{"overrides":{"components":['
            '{"enabled":false, "name":"hypershift"},'
            '{"enabled":false, "name":"hypershift-local-hosting"}, '
            '{"enabled":false, "name":"local-cluster"}'
            "]}}}' --type=merge"
        )
        cmd_res = exec_cmd(cmd, shell=True)
        if cmd_res.returncode != 0:
            logger.error(f"Failed to disable multicluster engine\n{cmd_res.stderr}")
            return
        logger.info(cmd_res.stdout.decode("utf-8").splitlines())

    def check_if_any_vm_and_vmi(self, namespace=None):
        """
        Checks if any VMs and VM instances are running

        Args:
            namespace (str): namespace to check

        Returns:
            True if any VMs or VMi else False

        """

        vm_obj = OCP(kind=constants.VIRTUAL_MACHINE, namespace=namespace)
        vmi_obj = OCP(kind=constants.VIRTUAL_MACHINE_INSTANCE, namespace=namespace)

        return vm_obj.get(
            out_yaml_format=False, all_namespaces=not namespace, dont_raise=True
        ) or vmi_obj.get(
            out_yaml_format=False, all_namespaces=not namespace, dont_raise=True
        )

    def remove_hyperconverged(self):
        """
        Remove HyperConverged CR

        """
        hyperconverged_obj = OCP(
            kind=constants.HYPERCONVERGED,
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
            namespace=self.namespace,
        )
        hyperconverged_obj.delete(resource_name=constants.KUBEVIRT_HYPERCONVERGED)
        logger.info(
            f"Deleted {constants.HYPERCONVERGED} {constants.KUBEVIRT_HYPERCONVERGED}"
        )

    def remove_cnv_subscription(self):
        """
        Remove CNV subscription

        """
        cnv_sub = OCP(
            kind=constants.SUBSCRIPTION,
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
            namespace=self.namespace,
        )
        cnv_sub.delete(resource_name=constants.KUBEVIRT_HYPERCONVERGED)
        logger.info(f"Deleted subscription {constants.KUBEVIRT_HYPERCONVERGED}")

    def remove_cnv_csv(self):
        """
        Remove CNV ClusterServiceVersion

        """
        cnv_csv = OCP(
            kind=constants.CLUSTER_SERVICE_VERSION,
            selector=constants.CNV_SELECTOR,
            namespace=self.namespace,
        )
        cnv_csv.delete(resource_name=cnv_csv.get()["items"][0]["metadata"]["name"])
        logger.info(f"Deleted ClusterServiceVersion {constants.CNV_OPERATORNAME}")

    def remove_cnv_operator(self):
        """
        Remove CNV operator

        """
        cnv_operator = OCP(
            kind=constants.OPERATOR_KIND, resource_name=constants.CNV_OPERATORNAME
        )
        cnv_operator.delete(resource_name=constants.CNV_OPERATORNAME)
        logger.info(f"Deleted operator {constants.CNV_OPERATORNAME}")

    def remove_crds(self):
        """
        Remove openshift virtualization CRDs

        """
        OCP().exec_oc_cmd(
            command=f"delete crd -n {self.namespace} -l {constants.CNV_SELECTOR}"
        )
        logger.info("Deleted all the openshift virtualization CRDs")

    def remove_namespace(self):
        """
        Remove openshift virtualization namespace

        """
        cnv_namespace = OCP()
        switch_to_default_rook_cluster_project()
        cnv_namespace.delete_project(constants.CNV_NAMESPACE)
        logger.info(f"Deleted the namespace {constants.CNV_NAMESPACE}")

    def uninstall_cnv(self, check_cnv_installed=True):
        """
        Uninstall CNV deployment

        Args:
            check_cnv_installed (bool): True if want to check if CNV installed

        """
        if check_cnv_installed:
            if not self.cnv_hyperconverged_installed():
                logger.info("CNV is not installed, skipping the cleanup...")
                return

        assert not self.check_if_any_vm_and_vmi(), (
            "Vm or Vmi instances are found in the cluster,"
            "Please make sure all VMs and VM instances are removed"
        )
        logger.info(
            "No VM or VM instances are found in the cluster, proceeding with the uninstallation"
        )

        logger.info("Removing the virtualization hyperconverged")
        self.remove_hyperconverged()

        logger.info("Removing the virtualization subscription")
        self.remove_cnv_subscription()

        logger.info("Removing the virtualization CSV")
        self.remove_cnv_csv()

        logger.info("Removing the virtualization Operator")
        self.remove_cnv_operator()

        logger.info("Removing the namespace")
        self.remove_namespace()

        logger.info("Removing the openshift virtualization CRDs")
        self.remove_crds()

    def get_running_cnv_version(self):
        """
        Get the currently deployed cnv version

        Returns:
            string: cnv version

        """
        hyperconverged_obj = OCP(
            kind=constants.HYPERCONVERGED,
            namespace=self.namespace,
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
        )
        cnv_version = hyperconverged_obj.get()["status"]["versions"][0]["version"]
        return cnv_version

    def check_cnv_is_upgradable(self):
        """
        This method checks if the cnv operator is upgradable or not

        Return:
            cnv_upgradeable (bool)): Returns True if Upgradable else False

        """
        cnv_upgradable = False
        if self.cnv_hyperconverged_installed() and self.post_install_verification(
            raise_exception=False
        ):
            kubevirt_hyperconverged = OCP(
                kind=constants.HYPERCONVERGED,
                namespace=self.namespace,
                resource_name=constants.KUBEVIRT_HYPERCONVERGED,
            )
            hyperconverged_conditions = kubevirt_hyperconverged.get()["status"][
                "conditions"
            ]
            for condition in hyperconverged_conditions:
                if condition["type"] == "Upgradeable":
                    cnv_upgradable = True if condition["status"] == "True" else False
                    break
        return cnv_upgradable

    def upgrade_cnv(self):
        """
        Upgrade cnv operator

        Returns:
        bool: if cnv operator is upgraded successfully

        """

        if not self.check_cnv_is_upgradable():
            logger.info("CNV is not upgradable")
            return

        hyperconverged_subs_obj = OCP(
            kind=constants.SUBSCRIPTION_WITH_ACM,
            namespace=self.namespace,
            resource_name=constants.KUBEVIRT_HYPERCONVERGED,
        )

        cnv_operators_nightly_catsrc = CatalogSource(
            resource_name=self.cnv_nightly_catsrc,
            namespace=constants.MARKETPLACE_NAMESPACE,
        )
        logger.info(
            f" currently installed cnv version: {parse_version(self.get_running_cnv_version())}"
        )
        self.upgrade_version = config.UPGRADE.get("upgrade_cnv_version")
        if not self.upgrade_version:
            self.upgrade_version = get_running_ocp_version()
        logger.info(f"Upgarde cnv to: {parse_version(self.upgrade_version)}")

        # we create catsrc with nightly builds only if config.DEPLOYMENT does not have cnv_latest_stable
        if not config.DEPLOYMENT.get("cnv_latest_stable"):
            # Create CNV catalog source
            if not self.catalog_source_created():
                self.create_cnv_catalog_source()
            # Update image details in CNV catalogsource
            patch = f'{{"spec": {{"image": "quay.io/openshift-cnv/nightly-catalog:{self.upgrade_version}"}}}}'
            cnv_operators_nightly_catsrc.patch(params=patch, format_type="merge")
            # wait for catalog source is ready
            cnv_operators_nightly_catsrc.wait_for_state("READY")
            # Update channel and source for CNV subscription
            patch = (
                f'{{"spec": {{"channel": "nightly-{self.upgrade_version}", '
                f'"source": "{self.cnv_nightly_catsrc}"}}}}'
            )
            hyperconverged_subs_obj.patch(params=patch, format_type="merge")

        install_plan_approval = hyperconverged_subs_obj.get()["spec"][
            "installPlanApproval"
        ]
        if install_plan_approval != "Automatic":
            patch = '{"spec": {"installPlanApproval": "Automatic"}}'
            hyperconverged_subs_obj.patch(params=patch, format_type="merge")
            wait_for_install_plan_and_approve(self.namespace)

        # Post CNV upgrade checks
        if self.post_install_verification():
            if install_plan_approval == "Manual":
                # setting upgrade approval back to manual
                patch = '{"spec": {"installPlanApproval": "Manual"}}'
                hyperconverged_subs_obj.patch(params=patch, format_type="merge")

            # wait for sometime before checking the latest cnv version
            time.sleep(60)
            return self.upgrade_version in self.get_running_cnv_version()
