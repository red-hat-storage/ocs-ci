# -*- coding: utf8 -*-
"""
This module implements functionality for deploying OCP cluster via Assisted Installer
"""

from copy import deepcopy
import json
import logging
import os

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    ClusterNotFoundException,
    HostValidationFailed,
    OpenShiftAPIResponseException,
    SameNameClusterAlreadyExistsException,
)
from ocs_ci.utility import assisted_installer as ai
from ocs_ci.utility.deployment import create_openshift_install_log_file
from ocs_ci.utility.utils import download_file, TimeoutSampler
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


def _inventory_dict(host):
    """
    Parse host inventory JSON if present.

    Args:
        host (dict): host object from Assisted Installer API

    Returns:
        dict: inventory, or empty dict if missing/invalid
    """
    inventory = host.get("inventory")
    if not inventory:
        return {}
    if isinstance(inventory, str):
        try:
            return json.loads(inventory)
        except json.JSONDecodeError:
            return {}
    return inventory if isinstance(inventory, dict) else {}


def _host_inventory_macs(host):
    """
    Lowercase MAC addresses from a discovered host inventory.

    Args:
        host (dict): host object from Assisted Installer API

    Returns:
        list: MAC strings (may be empty)
    """
    macs = []
    for interface in _inventory_dict(host).get("interfaces", []):
        mac = interface.get("mac_address")
        if mac:
            macs.append(mac.lower())
    return macs


def _conf_mac_to_hostname():
    """
    Build lowercase MAC -> conf server name from ENV_DATA.baremetal.servers.

    Returns:
        dict: mac -> hostname
    """
    mapping = {}
    servers = (config.ENV_DATA.get("baremetal") or {}).get("servers") or {}
    for name, details in servers.items():
        for key in ("public_mac", "private_mac"):
            mac = details.get(key)
            if mac:
                mapping[mac.lower()] = name
    return mapping


def _discovered_host_label(host, mac_name_mapping=None):
    """
    Label for a discovered AI host.

    Preference: requested_hostname, suggested_hostname, inventory.hostname,
    conf MAC match, first inventory MAC, else host id.
    """
    for key in ("requested_hostname", "suggested_hostname"):
        name = host.get(key)
        if name:
            return name

    inventory = _inventory_dict(host)
    inventory_hostname = inventory.get("hostname")
    if inventory_hostname:
        return inventory_hostname

    macs = _host_inventory_macs(host)
    mapping = dict(_conf_mac_to_hostname())
    if mac_name_mapping:
        mapping.update({k.lower(): v for k, v in mac_name_mapping.items()})
    for mac in macs:
        if mac in mapping:
            return mapping[mac]

    if macs:
        return macs[0]

    return host.get("id") or "?"


def _format_hosts_table(headers, rows):
    """
    Format a fixed-width host table for INFO logs (install progress).

    Args:
        headers (tuple|list): column titles
        rows (list): list of row tuples (same length as headers)

    Returns:
        str: multi-line table, or "(no hosts yet)" if rows is empty
    """
    if not rows:
        return "(no hosts yet)"
    headers = tuple(str(h) for h in headers)
    cells = [tuple(str(c) for c in row) for row in rows]
    widths = [len(h) for h in headers]
    for row in cells:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    col_sep = " | "
    fmt = col_sep.join(f"{{:<{w}}}" for w in widths)
    lines = [fmt.format(*headers), col_sep.join("-" * w for w in widths)]
    lines.extend(fmt.format(*row) for row in cells)
    return "\n".join(lines)


class AssistedInstallerCluster(object):
    def __init__(
        self,
        name,
        cluster_path,
        existing_cluster=False,
        openshift_version=None,
        base_dns_domain=None,
        api_vip=None,
        ingress_vip=None,
        ssh_public_key=None,
        pull_secret=None,
        cpu_architecture="x86_64",
        high_availability_mode="Full",
        image_type="minimal-iso",
        static_network_config=None,
        platform="baremetal",
    ):
        """
        Args:
            name (str): Name of the OpenShift cluster.
            cluster_path (str): path to cluster dir
            existing_cluster (bool): controls if we want to create new cluster or load configuration from existing one
                Following parameters are mandatory, if we are creating new cluster.
            openshift_version (str): Version of the OpenShift cluster. (Mandatory if existing_cluster is not True)
            base_dns_domain (str): Base domain of the cluster. All DNS records must be sub-domains of this base and
                include the cluster name. (Mandatory if existing_cluster is not True)
            api_vip (str): The virtual IPs used to reach the OpenShift cluster's API.
                (Mandatory if existing_cluster is not True)
            ingress_vip (str): The virtual IPs used for cluster ingress traffic.
                (Mandatory if existing_cluster is not True)
            ssh_public_key (str): SSH public key for debugging OpenShift nodes.
                (Mandatory if existing_cluster is not True)
            pull_secret (str): original pull-secret
                (Mandatory if existing_cluster is not True)
            cpu_architecture (str): The CPU Architecture: x86_64, aarch64, arm64, ppc64le, s390x, multi
                (default: x86_64)
            high_availability_mode (str): High availability mode: Full or None (default: "Full")
            image_type (str): Type of discovery image full-iso or minimal-iso (default: minimal-iso)
            static_network_config (list(dict)): Static network configuration of hosts, where network_yaml is yaml string
                that can be processed by nmstate
                [{"mac_interface_map": [{"logical_nic_name": "string", "mac_address": "string"}],
                    "network_yaml": "string"},
                    ...]
            platform (str): platform type (none, baremetal or vsphere)

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
            try:
                self.id = [
                    cl["id"]
                    for cl in clusters
                    if cl["name"] == name and cl["kind"] == "Cluster"
                ][0]
            except IndexError:
                # there might be missing "kind: Cluster" with the name, but still present AddHostsCluster
                # so we want to delete that as well
                self.id = [
                    cl["id"]
                    for cl in clusters
                    if cl["name"] == name and cl["kind"] == "AddHostsCluster"
                ][0]

            # load configuration of existing cluster
            self.load_existing_cluster_configuration()
            logger.info(
                f"Loaded configuration for existing cluster {name} (id: {self.id})"
            )
        else:
            # set up configuration for new cluster
            if not (
                openshift_version
                and base_dns_domain
                and api_vip
                and ingress_vip
                and ssh_public_key
                and pull_secret
            ):
                raise TypeError(
                    "When configuring new cluster (existing_cluster=False), "
                    "all following arguments has to be set: "
                    "openshift_version, base_dns_domain, api_vip,  ingress_vip, ssh_public_key and pull_secret"
                )
            self.openshift_version = openshift_version
            self.base_dns_domain = base_dns_domain
            self.api_vip = api_vip
            self.ingress_vip = ingress_vip
            # if ssh_public_key contains new line at the end, infrastructure creation
            # fails with error SSH key is not valid
            self.ssh_public_key = ssh_public_key.strip()
            self.pull_secret = self.prepare_pull_secret(pull_secret)
            self.cpu_architecture = cpu_architecture
            self.high_availability_mode = high_availability_mode
            self.image_type = image_type
            self.static_network_config = static_network_config
            self.platform = platform

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
        try:
            self.api_vip = cl_config["api_vips"][0]["ip"]
            self.ingress_vip = cl_config["ingress_vips"][0]["ip"]
        except (KeyError, IndexError):
            self.api_vip = ""
            self.ingress_vip = ""
        self.ssh_public_key = cl_config.get("ssh_public_key")
        # self.pull_secret = cl_config["pull_secret"]
        self.cpu_architecture = cl_config.get("cpu_architecture")
        self.high_availability_mode = cl_config.get("high_availability_mode")
        self.image_type = infra_config["type"]
        self.openshift_cluster_id = cl_config.get("openshift_cluster_id")
        # load records with: 'kind': 'AddHostsCluster'
        try:
            self.add_hosts_clusters = [
                cl["id"]
                for cl in self.api.get_clusters()
                if self.openshift_cluster_id
                and cl.get("openshift_cluster_id") == self.openshift_cluster_id
                and cl["kind"] == "AddHostsCluster"
            ]
        except OpenShiftAPIResponseException:
            self.add_hosts_clusters = []
        logger.debug(f"AddHostsClusters: {', '.join(self.add_hosts_clusters)}")

        try:
            self.add_hosts_infra_envs = [
                infra["id"]
                for cl_id in self.add_hosts_clusters
                for infra in self.api.get_infra_envs()
                if infra["cluster_id"] == cl_id
            ]
        except OpenShiftAPIResponseException:
            self.add_hosts_infra_envs = []
        logger.debug(
            f"AddHosts Infrastructure Environments: {', '.join(self.add_hosts_infra_envs)}"
        )

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
            "ssh_public_key": self.ssh_public_key,
            "pull_secret": self.pull_secret,
        }

        # VPC BM uses external ALBs for VIPs, not VRRP - requires user_managed_networking
        if (
            config.ENV_DATA.get("infra_type", constants.CLASSIC_INFRA_TYPE)
            == constants.VPC_INFRA_TYPE
        ):
            cluster_configuration["user_managed_networking"] = True
            cluster_configuration["vip_dhcp_allocation"] = False
            cluster_configuration["platform"] = {"type": "none"}
            logger.info(
                "Creating cluster with User Managed Networking (VIPs managed externally by ALBs)"
            )
        else:
            # Traditional platforms (vSphere, baremetal) use managed networking with VRRP VIPs
            cluster_configuration["platform"] = {"type": self.platform}
            cluster_configuration["api_vips"] = [{"ip": self.api_vip}]
            cluster_configuration["ingress_vips"] = [{"ip": self.ingress_vip}]
            logger.info(
                f"Creating cluster with managed networking (platform: {self.platform})"
            )
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

        if self.static_network_config:
            infra_env_configuration["static_network_config"] = (
                self.static_network_config
            )

        infra_data = self.api.create_infra_env(infra_env_configuration)
        self.infra_id = infra_data["id"]
        logger.info(
            f"Created infrastructure environment {self.name} (id: {self.infra_id}) for cluster {self.id}"
        )

    def get_discovery_iso_url(self):
        """
        Get Assisted Installer discovery ISO URL for this cluster's infra-env.

        Returns:
            str: HTTPS URL of the discovery ISO image
        """
        iso_url = self.api.get_discovery_iso_url(self.infra_id)
        logger.info(
            "Discovery ISO URL for infra-env %s: %s",
            self.infra_id,
            ai.redact_iso_url(iso_url),
        )
        return iso_url

    def download_discovery_iso(self, local_path):
        """
        Download the discovery iso image

        Args:
            local_path (str): path where to store the discovery iso image

        """
        iso_url = self.get_discovery_iso_url()
        download_file(iso_url, local_path)
        logger.info(
            "Downloaded discovery iso from '%s' to %s",
            ai.redact_iso_url(iso_url),
            local_path,
        )

    def download_ipxe_config(self, local_path):
        """
        Download the ipxe config for discovery boot

        Args:
            local_path (str): path where to store the ipxe config

        Return:
            str: path to the downloaded ipxe config file

        """
        ipxe_config_path = self.api.download_infra_file(
            infra_env_id=self.infra_id,
            dest_dir=local_path,
            file_name="ipxe-script",
            ipxe_script_type="discovery-image-always",
        )
        logger.info(f"Downloaded iPXE config {ipxe_config_path}")
        return ipxe_config_path

    def wait_for_discovered_nodes(self, expected_nodes):
        """
        Wait for expected number of nodes to appear in the Assisted Installer infra/cluster

        Args:
            expected_nodes (int): number of expected nodes

        """

        # wait for discovered nodes in cluster definition
        for sample in TimeoutSampler(
            timeout=1200, sleep=120, func=self.api.get_cluster_hosts, cluster_id=self.id
        ):
            hosts = [_discovered_host_label(n) for n in sample]
            logger.debug(
                "Discovery progress (cluster): %s/%s hosts=%s",
                len(sample),
                expected_nodes,
                hosts,
            )
            if expected_nodes == len(sample):
                logger.info(
                    "Discovered expected number (%s) of nodes in cluster "
                    "configuration: %s",
                    len(sample),
                    hosts,
                )
                break

        # wait for discovered nodes in Infrastructure Environment definition
        for sample in TimeoutSampler(
            timeout=1200,
            sleep=120,
            func=self.api.get_infra_env_hosts,
            infra_env_id=self.infra_id,
        ):
            hosts = [_discovered_host_label(n) for n in sample]
            logger.debug(
                "Discovery progress (infra-env): %s/%s hosts=%s",
                len(sample),
                expected_nodes,
                hosts,
            )
            if expected_nodes == len(sample):
                logger.info(
                    "Discovered expected number (%s) of nodes in Infrastructure "
                    "Environment: %s",
                    len(sample),
                    hosts,
                )
                break

    def get_infra_env_hosts(self):
        """
        Return:
            list: list of discovered hosts in the Infrastructure Environment

        """
        return self.api.get_infra_env_hosts(infra_env_id=self.infra_id)

    @retry(HostValidationFailed, tries=5, delay=60, backoff=1)
    def verify_validations_info_for_discovered_nodes(self):
        """
        Check and verify validations info for the discovered nodes.
        """
        failed_validations = []
        for host in self.api.get_cluster_hosts(self.id):
            try:
                vi = json.loads(host["validations_info"])
                for section in vi:
                    for v in vi[section]:
                        if v["status"] in ("failure", "pending"):
                            failed_validations.append(
                                f"host {_discovered_host_label(host)}, section "
                                f"{section}, {v['id']}: {v['status']} ({v['message']})"
                            )
            except KeyError as err:
                failed_validations.append(f"host {_discovered_host_label(host)}: {err}")
        if failed_validations:
            msg = f"Failed hosts validations: \n{os.linesep.join(failed_validations)}"
            logger.debug(msg)
            raise HostValidationFailed(msg)
        logger.info("Host validations passed on all hosts.")

    def get_host_id_mac_mapping(self):
        """
        Prepare mapping between host ID and mac addresses

        Return:
            list of lists: host id to mac mapping ([[host1_id, mac1], [host1_id, mac2], [host2_id, mac3],...])

        """
        hosts = self.api.get_infra_env_hosts(self.infra_id)
        mapping = []
        for host in hosts:
            for interface in json.loads(host["inventory"])["interfaces"]:
                if interface["ipv4_addresses"]:
                    mapping.append((host["id"], interface["mac_address"]))
        return mapping

    def get_ip_list_by_cluster_id(self):
        """
        Get list of IP addresses assigned to the hosts in the cluster.
        """
        ip_list = []
        for host in self.api.get_cluster_hosts(self.id):
            inventory = host.get("inventory")
            if not inventory:
                continue
            if isinstance(inventory, str):
                try:
                    inventory = json.loads(inventory)
                except json.JSONDecodeError:
                    logger.warning(
                        "Unable to decode inventory for host %s", host.get("id")
                    )
                    continue
            interfaces = inventory.get("interfaces", [])
            for interface in interfaces:
                for ip_cidr in interface.get("ipv4_addresses", []):
                    ip_list.append(ip_cidr.split("/")[0])
        return ip_list

    def update_hosts_config(self, mac_name_mapping, mac_role_mapping):
        """
        Update host names and roles.

        Args:
            mac_name_mapping (dict): host mac address to host name mapping
            mac_role_mapping (dict): host mac address to host role mapping

        """
        # normalize MAC case — AI and conf may disagree on upper/lower
        mac_name_mapping = {k.lower(): v for k, v in mac_name_mapping.items()}
        mac_role_mapping = {k.lower(): v for k, v in mac_role_mapping.items()}
        host_id_mac_mapping = self.get_host_id_mac_mapping()
        for host_id, mac in host_id_mac_mapping:
            try:
                update_data = {
                    "host_name": mac_name_mapping[mac.lower()],
                    "host_role": mac_role_mapping[mac.lower()],
                }
                self.api.update_infra_env_host(self.infra_id, host_id, update_data)
                logger.info(f"Updated host {host_id} configuration: {update_data}")
            except KeyError:
                # ignoring KeyError failure, because we have more than one mac address for each host and only one of
                # them is used to the name and role mapping
                pass

    def get_discovered_macs(self):
        """
        Collect all MAC addresses reported by discovered infra-env hosts.

        Returns:
            set: lowercase MAC addresses seen in host inventories
        """
        macs = set()
        for host in self.api.get_infra_env_hosts(self.infra_id):
            macs.update(_host_inventory_macs(host))
        return macs

    def install_cluster(self, pending_user_action_handler=None):
        """
        Trigger cluster installation

        Args:
            pending_user_action_handler (function): function handling pending user action for particular host (host
                details as dict provided as first parameter)

        """
        self.api.install_cluster(self.id)
        logger.info("Started cluster installation")

        def _host_ui_role(host, schedulable_masters):
            role = host.get("role")
            if role == "master" and schedulable_masters:
                label = "Control plane node, Worker"
            else:
                label = role or "-"
            if host.get("bootstrap"):
                label = f"{label} (bootstrap)"
            return label

        def _host_stage(host):
            progress = host.get("progress") or {}
            stage = progress.get("current_stage") or "-"
            stages = host.get("progress_stages") or []
            if stage in stages:
                return f"{stage} ({stages.index(stage) + 1}/{len(stages)})"
            return stage

        def _log_host_table(cluster, hosts_avg_pct, cluster_pct):
            hosts = cluster.get("hosts") or []
            schedulable = cluster.get("schedulable_masters")
            if schedulable is None:
                schedulable = bool(hosts) and all(
                    h.get("role") != "worker" for h in hosts
                )
            rows = [
                (
                    _discovered_host_label(host),
                    _host_ui_role(host, schedulable),
                    host.get("status") or "-",
                    _host_stage(host),
                )
                for host in hosts
            ]
            table = _format_hosts_table(("Hostname", "Role", "Status", "Stage"), rows)
            # Cluster progress.total_percentage matches the UI bar; hosts avg is
            # only per-host install stages (can be 100% while still finalizing).
            cluster_pct_txt = f"{cluster_pct}%" if cluster_pct is not None else "n/a"
            logger.info(
                "Cluster %s (%s) cluster_progress=%s hosts_avg=%s%%\n%s",
                cluster.get("status"),
                cluster.get("status_info"),
                cluster_pct_txt,
                hosts_avg_pct,
                table,
            )

        # wait for cluster installation success
        for sample in TimeoutSampler(
            timeout=10800, sleep=300, func=self.api.get_cluster, cluster_id=self.id
        ):
            hosts = sample.get("hosts") or []
            status_per_hosts = [
                (h.get("progress") or {}).get("installation_percentage") or 0
                for h in hosts
            ]
            hosts_avg_pct = (
                round(sum(status_per_hosts) / len(status_per_hosts))
                if status_per_hosts
                else 0
            )
            cluster_progress = sample.get("progress") or {}
            cluster_pct = cluster_progress.get("total_percentage")
            _log_host_table(sample, hosts_avg_pct, cluster_pct)

            if sample["status"] == "installing-pending-user-action":
                for host in hosts:
                    try:
                        if host["status"] == "installing-pending-user-action":
                            msg = (
                                f"{host['requested_hostname']}: {host['status']} "
                                f"({host['status_info']})"
                            )
                            if pending_user_action_handler:
                                logger.info(msg)
                                pending_user_action_handler(host)
                            else:
                                logger.error(msg)
                    except KeyError:
                        pass

            if sample["status"] == "installed":
                logger.info(
                    "Cluster was successfully installed (status: %s - %s)",
                    sample["status"],
                    sample["status_info"],
                )
                break
        # create metadata, kubeconfig, kubeadmin-password and openshift_install.log files
        self.create_metadata_file()
        self.create_kubeconfig_file()
        self.create_kubeadmin_password_file()
        self.create_openshift_install_log_file()

    def create_metadata_file(self):
        """
        Create metadata.json file.
        """
        self.api.download_cluster_file(self.id, self.cluster_path, "metadata.json")
        logger.info("Created metadata.json file")

    def create_openshift_install_log_file(self):
        """
        Create .openshift_install.log file containing URL to OpenShift console.
        It is used by our CI jobs to show the console URL in build description.
        """
        cluster_address = self.api.get_cluster_admin_credentials(self.id)["console_url"]
        create_openshift_install_log_file(self.cluster_path, cluster_address)

    def create_kubeconfig_file(self):
        """
        Export kubeconfig to auth directory in cluster path.
        """
        auth_path = os.path.join(self.cluster_path, "auth")
        os.makedirs(auth_path, exist_ok=True)
        path = os.path.join(auth_path, "kubeconfig")
        with open(path, "w") as fd:
            fd.write(self.api.get_cluster_kubeconfig(self.id))
        config.RUN["kubeconfig"] = path
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
            f"Cluster {self.name} (id: {self.id}) was deleted from Assisted Installer Console"
        )
        if self.add_hosts_clusters:
            for cl_id in self.add_hosts_clusters:
                self.api.delete_cluster(cl_id)
                logger.info(
                    f"AddHostsCluster {self.name} (id: {cl_id}) was deleted from Assisted Installer Console"
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
        if self.add_hosts_infra_envs:
            for infra_id in self.add_hosts_infra_envs:
                self.api.delete_infra_env(infra_id)
                logger.info(
                    f"Infrastructure environment {infra_id} for adding hosts to cluster {self.name} (id: {self.id}) "
                    "was deleted from Assisted Installer Console"
                )
