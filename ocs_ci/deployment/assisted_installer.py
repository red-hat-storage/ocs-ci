# -*- coding: utf8 -*-
"""
This module implements functionality for deploying OCP cluster via Assisted Installer
"""

from copy import deepcopy
from datetime import datetime
import json
import logging
import os

from ocs_ci.ocs.exceptions import (
    ClusterNotFoundException,
    HostValidationFailed,
    SameNameClusterAlreadyExistsException,
)
from ocs_ci.utility import assisted_installer as ai
from ocs_ci.utility.utils import download_file, TimeoutSampler
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


class AssistedInstallerCluster(object):
    def __init__(self, name, cluster_path, existing_cluster=False, **kwargs):
        """
        Args:
            name (str): Name of the OpenShift cluster.
            cluster_path (str): path to cluster dir
            existing_cluster (bool): controls if we want to create new cluster or load configuration from existing one
            **kwargs: for new cluster, **kwargs are passed to set_cluster_configuration function
                (see the description there)

        """
        self.api = ai.AssistedInstallerAPI()
        # check connection
        logger.info(
            f"Check Assisted Installer API connection: {self.api.get_component_versions()}"
        )

        self.name = name
        self.cluster_path = os.path.expanduser(cluster_path)

        if existing_cluster:
            clusters = self.api.get_clusters()
            if name not in [cl["name"] for cl in clusters]:
                raise ClusterNotFoundException(
                    f"Cluster '{name}' not found in Assisted Installer Console"
                )
            self.id = [cl["id"] for cl in clusters if cl["name"] == name][0]
            # load configuration of existing cluster
            self.load_existing_cluster_configuration()
            logger.info(
                f"Loaded configuration for existing cluster {name} (id: {self.id})"
            )
        else:
            # set up configuration for new cluster
            self.set_cluster_configuration(**kwargs)

    def set_cluster_configuration(
        self,
        openshift_version,
        base_dns_domain,
        api_vip,
        ingress_vip,
        ssh_public_key,
        pull_secret,
        cpu_architecture="x86_64",
        high_availability_mode="Full",
        image_type="minimal-iso",
    ):
        """
        Prepare configuration for new cluster

        Args:
            openshift_version (str): Version of the OpenShift cluster.
            base_dns_domain (str): Base domain of the cluster. All DNS records must be sub-domains of this base and
                include the cluster name.
            api_vip (str): The virtual IPs used to reach the OpenShift cluster's API.
            ingress_vip (str): The virtual IPs used for cluster ingress traffic.
            ssh_public_key (str): SSH public key for debugging OpenShift nodes.
            pull_secret (str): original pull-secret
            cpu_architecture (str): The CPU Architecture: x86_64, aarch64, arm64, ppc64le, s390x, multi
                (default: x86_64)
            high_availability_mode (str): High availability mode: Full or None (default: "Full")
            image_type (str): Type of discovery image full-iso or minimal-iso (default: minimal-iso)
        """
        self.openshift_version = openshift_version
        self.base_dns_domain = base_dns_domain
        self.api_vip = api_vip
        self.ingress_vip = ingress_vip
        # if ssh_public_key contains new line at the end, infrastructure creation fails with error SSH key is not valid
        self.ssh_public_key = ssh_public_key.strip()
        self.pull_secret = self.prepare_pull_secret(pull_secret)
        self.cpu_architecture = cpu_architecture
        self.high_availability_mode = high_availability_mode
        self.image_type = image_type

    def load_existing_cluster_configuration(self):
        """
        Load configuration from existing cluster
        """
        cl_config = self.api.get_cluster(self.id)
        self.infra_id = [
            infra["id"]
            for infra in self.api.get_infra_envs()
            if infra["cluster_id"] == self.id
        ][0]

        infra_config = self.api.get_infra_env(self.infra_id)
        self.openshift_version = cl_config["openshift_version"]
        self.base_dns_domain = cl_config["base_dns_domain"]
        self.api_vip = cl_config["api_vip"]
        self.ingress_vip = cl_config["ingress_vip"]
        self.ssh_public_key = cl_config["ssh_public_key"]
        # self.pull_secret = cl_config["pull_secret"]
        self.cpu_architecture = cl_config["cpu_architecture"]
        self.high_availability_mode = cl_config["high_availability_mode"]
        self.image_type = infra_config["type"]

    def prepare_pull_secret(self, original_pull_secret):
        """
        Combine original pull secret with the pull secret for the Assisted Installer console user.
        We have to replace cloud.openshift.com credentials in the original pull-secret with the credentials for the
        current user, otherwise Assisted Installer will comply that the pull secret belongs to different user.

        Args:
            original_pull_secret (str or dict): content of pull secret
        """
        if isinstance(original_pull_secret, dict):
            # prepare copy of the original pull-secret (to not modify it)
            pull_secret_dict = deepcopy(original_pull_secret)
        elif isinstance(original_pull_secret, str):
            pull_secret_dict = json.loads(original_pull_secret)
        else:
            raise TypeError(
                f"prepare_pull_secret: original_pull_secret value should be of type <dict> or <str>, "
                f"not {type(original_pull_secret)}"
            )

        # get the pull secret for the actual user used for interaction with Assisted Installer Console/API
        ai_user_pull_secret = ai.AccountsMgmtAPI().get_pull_secret_for_current_user()
        # replace cloud.openshift.com configuration in the pull-secret
        pull_secret_dict["auths"]["cloud.openshift.com"] = ai_user_pull_secret["auths"][
            "cloud.openshift.com"
        ]
        return json.dumps(pull_secret_dict)

    def create_cluster(self):
        """
        Create (register) new cluster in Assisted Installer console
        """
        clusters = self.api.get_clusters()
        if self.name in [cl["name"] for cl in clusters]:
            cluster_id = [cl["id"] for cl in clusters if cl["name"] == self.name][0]
            raise SameNameClusterAlreadyExistsException(
                f"Cluster with the same name {self.name} (ID: {cluster_id}) already exists!"
            )

        cluster_configuration = {
            "name": self.name,
            "openshift_version": self.openshift_version,
            "cpu_architecture": self.cpu_architecture,
            "high_availability_mode": self.high_availability_mode,
            "base_dns_domain": self.base_dns_domain,
            "api_vip": self.api_vip,
            "api_vips": [
                {
                    "ip": self.api_vip,
                }
            ],
            "ingress_vip": self.ingress_vip,
            "ingress_vips": [
                {
                    "ip": self.ingress_vip,
                }
            ],
            "ssh_public_key": self.ssh_public_key,
            "pull_secret": self.pull_secret,
        }
        cl_data = self.api.create_cluster(cluster_configuration)
        self.id = cl_data["id"]
        logger.info(f"Created (defined) new cluster {self.name} (id: {self.id})")

    def create_infrastructure_environment(self):
        """
        Create new Infrastructure Environment for the cluster
        """
        infra_env_configuration = {
            "name": self.name,
            "image_type": self.image_type,
            "cluster_id": self.id,
            "cpu_architecture": self.cpu_architecture,
            "openshift_version": self.openshift_version,
            "ssh_authorized_key": self.ssh_public_key,
            "pull_secret": self.pull_secret,
        }
        infra_data = self.api.create_infra_env(infra_env_configuration)
        self.infra_id = infra_data["id"]
        logger.info(
            f"Created infrastructure environment {self.name} (id: {self.infra_id}) for cluster {self.id}"
        )

    def download_discovery_iso(self, local_path):
        """
        Download the discovery iso image

        Args:
            local_path (str): path where to store the discovery iso image

        """
        iso_url = self.api.get_discovery_iso_url(self.infra_id)
        download_file(iso_url, local_path)
        logger.info(f"Downloaded discovery iso from '{iso_url}' to {local_path}")

    def wait_for_discovered_nodes(self, expected_nodes):
        """
        Wait for expected number of nodes to appear in the Assisted Installer infra/cluster

        Args:
            expected_nodes (int): number of expected nodes
        """

        # wait for discovered nodes in cluster definition
        for sample in TimeoutSampler(
            timeout=3600, sleep=300, func=self.api.get_cluster_hosts, cluster_id=self.id
        ):
            logger.debug(f"Discovered {len(sample)} nodes: {[n['id'] for n in sample]}")
            if expected_nodes == len(sample):
                logger.info(
                    f"Discovered expected number ({len(sample)}) of nodes in cluster configuration: "
                    f"{[n['id'] for n in sample]}"
                )
                break

        # wait for discovered nodes in Infrastructure Environment definition
        for sample in TimeoutSampler(
            timeout=3600,
            sleep=300,
            func=self.api.get_infra_env_hosts,
            infra_env_id=self.infra_id,
        ):
            logger.debug(f"Discovered {len(sample)} nodes: {[n['id'] for n in sample]}")
            if expected_nodes == len(sample):
                logger.info(
                    f"Discovered expected number ({len(sample)}) of nodes in Infrastructure Environment: "
                    f"{[n['id'] for n in sample]}"
                )
                break

    @retry(HostValidationFailed, tries=5, delay=60, backoff=1)
    def verify_validations_info_for_discovered_nodes(self):
        """
        Check and verify validations info for the discovered nodes.

        """
        failed_validations = []
        for host in self.api.get_cluster_hosts(self.id):
            vi = json.loads(host["validations_info"])
            for section in vi:
                for v in vi[section]:
                    if v["status"] in ("failure", "pending"):
                        failed_validations.append(
                            f"host {host['id']}, section {section}, {v['id']}: {v['status']} ({v['message']})"
                        )
        if failed_validations:
            msg = f"Failed hosts validations: \n{failed_validations.join(os.linesep)}"
            logger.error(msg)
            raise HostValidationFailed(msg)
        logger.info("Host validations passed on all hosts.")

    def get_host_id_mac_mapping(self):
        """
        Prepare mapping between host ID and mac addresses

        Return:
            dict: host id to mac mapping
        """
        hosts = self.api.get_infra_env_hosts(self.infra_id)
        return {
            h["id"]: json.loads(h["inventory"])["interfaces"][0]["mac_address"]
            for h in hosts
        }

    def update_hosts_config(self, mac_name_mapping, mac_role_mapping):
        """
        Update host names and roles.

        Args:
            mac_name_mapping (dict): host mac address to host name mapping
            mac_role_mapping (dict): host mac address to host role mapping
        """
        host_id_mac_mapping = self.get_host_id_mac_mapping()
        for host_id in host_id_mac_mapping:
            update_data = {
                "host_name": mac_name_mapping[host_id_mac_mapping[host_id]],
                "host_role": mac_role_mapping[host_id_mac_mapping[host_id]],
            }
            self.api.update_infra_env_host(self.infra_id, host_id, update_data)
            logger.info(f"Updated host {host_id} configuration: {update_data}")

    def install_cluster(self):
        """
        Trigger cluster installation
        """
        self.api.install_cluster(self.id)
        logger.info("Started cluster installation")
        # wait for cluster installation success
        for sample in TimeoutSampler(
            timeout=3600, sleep=300, func=self.api.get_cluster, cluster_id=self.id
        ):
            # TODO: add more information about cluster installation progress
            logger.info(
                f"Cluster installation status: {sample['status']} ({sample['status_info']})"
            )
            if sample["status"] == "installed":
                logger.info(
                    f"Cluster was successfully installed (status: {sample['status']} - {sample['status_info']})"
                )
                break
        # create metadata, kubeconfig, kubeadmin-password and openshift_install.log files
        self.create_metadata_file()
        self.create_kubeconfig_file()
        self.create_kubeadmin_password_file()
        self.create_openshift_install_log_file()

    def create_metadata_file(self):
        """
        Create .openshift_install.log file containing URL to OpenShift console
        It is used by our CI jobs to show the console URL in build description.
        """
        self.api.download_cluster_file(self.id, self.cluster_path, "metadata.json")
        logger.info("Created metadata.json file")

    def create_openshift_install_log_file(self):
        """
        Create .openshift_install.log file containing URL to OpenShift console.
        It is used by our CI jobs to show the console URL in build description.
        """
        # Create metadata file to store the cluster name
        installer_log_file = os.path.join(self.cluster_path, ".openshift_install.log")
        formatted_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        cluster_address = self.api.get_cluster_admin_credentials(self.id)["console_url"]
        logger.info(f"Cluster URL: {cluster_address}")
        with open(installer_log_file, "a") as fd:
            fd.writelines(
                [
                    "W/A for our CI to get URL to the cluster in jenkins job. "
                    "Cluster is deployed via Assisted Installer API!\n"
                    f'time="{formatted_time}" level=info msg="Access the OpenShift web-console here: '
                    f"{cluster_address}\"\n'",
                ]
            )
        logger.info("Created .openshift_install.log file")

    def create_kubeconfig_file(self):
        """
        Export kubeconfig to auth directory in cluster path.
        """
        auth_path = os.path.join(self.cluster_path, "auth")
        os.makedirs(auth_path, exist_ok=True)
        path = os.path.join(auth_path, "kubeconfig")
        with open(path, "w") as fd:
            fd.write(self.api.get_cluster_kubeconfig(self.id))
        logger.info("Created kubeconfig file")

    def create_kubeadmin_password_file(self):
        """
        Export password for kubeadmin to auth/kubeadmin-password file in cluster path
        """
        auth_path = os.path.join(self.cluster_path, "auth")
        os.makedirs(auth_path, exist_ok=True)
        path = os.path.join(auth_path, "kubeadmin-password")
        with open(path, "w") as fd:
            fd.write(self.api.get_cluster_admin_credentials(self.id)["password"])
        logger.info("Created kubeadmin-password file")

    def delete_cluster(self):
        """
        Delete the cluster
        """
        self.api.delete_cluster(self.id)
        logger.info(
            "Cluster {self.name} (id: {self.id}) was deleted from Assisted Installer Console"
        )

    def delete_infrastructure_environment(self):
        """
        Delete the Infrastructure Environment
        """
        self.api.delete_infra_env(self.infra_id)
        logger.info(
            f"Infrastructure environment {self.infra_id} for cluster {self.name} (id: {self.id}) "
            "was deleted from Assisted Installer Console"
        )
