"""
This module contains helpers functions needed for
external cluster deployment.
"""

from dataclasses import dataclass
import json
import logging
import re
import shlex
import tempfile
import uuid
import os

import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ExternalClusterCephfsMissing,
    ExternalClusterCephSSHAuthDetailsMissing,
    ExternalClusterCrushRuleCreationFailed,
    ExternalClusterDisableCertificateCheckFailed,
    ExternalClusterExporterRunFailed,
    ExternalClusterPoolCreationFailed,
    ExternalClusterRBDNamespaceCreationFailed,
    ExternalClusterReplica1ConfigurationFailed,
    ExternalClusterRGWEndPointMissing,
    ExternalClusterRGWEndPointPortMissing,
    ExternalClusterNodeRoleNotFound,
    ExternalClusterObjectStoreUserCreationFailed,
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources.csv import get_csv_name_start_with_prefix
from ocs_ci.ocs.resources.packagemanifest import (
    PackageManifest,
    get_selector_for_ocs_operator,
)
from ocs_ci.utility import version, ssl_certs
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.utils import (
    upload_file,
    encode,
    decode,
    download_file,
    exec_cmd,
    wait_for_machineconfigpool_status,
    create_config_ini_file,
)

logger = logging.getLogger(__name__)


@dataclass
class ZoneConfig:
    """
    Configuration for a single zone in topology-based replica-1 setup.

    Args:
        zone_name (str): Name of the zone (e.g., "zone-a").
        host_name (str): Ceph OSD host name for this zone (e.g., "osd-0").
        pool_name (str): Custom pool name. If empty, auto-generated from zone_name.

    Raises:
        ValueError: If zone_name or host_name is empty.

    """

    zone_name: str
    host_name: str
    pool_name: str = ""

    def __post_init__(self):
        if not self.zone_name:
            raise ValueError("zone_name cannot be empty")
        if not self.host_name:
            raise ValueError("host_name cannot be empty")


@dataclass
class TopologyReplica1Config:
    """
    Configuration for topology-based replica-1 provisioning.

    Args:
        zones (list[ZoneConfig]): List of zone configurations.
        pool_prefix (str): Prefix for auto-generated pool names.
        pg_num (int): Number of placement groups per pool.

    """

    zones: list[ZoneConfig]
    pool_prefix: str = "rbd-zone"
    pg_num: int = 32

    @property
    def pool_names(self) -> list[str]:
        return [
            zone.pool_name or f"{self.pool_prefix}-{zone.zone_name}"
            for zone in self.zones
        ]

    @property
    def zone_names(self) -> list[str]:
        return [zone.zone_name for zone in self.zones]


class ExternalCluster(object):
    """
    Helper for External RHCS cluster
    """

    def __init__(self, host, user, password=None, ssh_key=None):
        """
        Initialize the variables required for external RHCS cluster

        Args:
             host (str): Host name with FQDN or IP
             user (str): User name
             password (password): Password for the Host (optional if ssh_key provided)
             ssh_key (str): Path to SSH private key for the host (optional if password provided).

        Raises:
            ExternalClusterCephSSHAuthDetailsMissing: In case one of SSH key or password
                is not provided.

        """
        self.host = host
        self.user = user
        self.password = password
        self.ssh_key = ssh_key
        if not (self.password or self.ssh_key):
            raise ExternalClusterCephSSHAuthDetailsMissing(
                "No SSH Auth to connect to external RHCS cluster provided! "
                "Either password or SSH key is missing in EXTERNAL_MODE['login'] section!"
            )
        self.jump_host = config.DEPLOYMENT.get("ssh_jump_host")
        if self.jump_host and not self.jump_host.get("private_key"):
            ssh_key_private = config.DEPLOYMENT.get("ssh_key_private")
            if ssh_key_private:
                self.jump_host["private_key"] = os.path.expanduser(ssh_key_private)

        self.rhcs_conn = Connection(
            host=self.host,
            user=self.user,
            password=self.password,
            private_key=self.ssh_key,
            jump_host=self.jump_host,
        )

    def exec_external_ceph_cmd(
        self,
        cmd: str,
        error_msg: str,
        exception_class: type,
        raise_on_error: bool = True,
    ) -> tuple[int, str, str]:
        """
        Execute a Ceph command on the external RHCS cluster with error handling.

        This method wraps rhcs_conn.exec_cmd() with standardized logging and
        exception handling for external cluster operations.

        Args:
            cmd (str): The Ceph command to execute.
            error_msg (str): Error message prefix for logging on failure.
            exception_class (type): Exception class to raise on failure.
            raise_on_error (bool): If True, raise exception on non-zero return code.

        Returns:
            tuple[int, str, str]: Return code, stdout, stderr.

        Raises:
            exception_class: If command fails and raise_on_error is True.

        """
        retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
        if retcode != 0 and raise_on_error:
            logger.error(f"{error_msg}. Error: {err}")
            raise exception_class(f"{error_msg}: {err}")
        return retcode, out, err

    def build_exporter_base_params(self, include_rgw=True):
        """
        Build the base parameter string for the exporter script.

        Reads cluster config to construct the flags needed by
        create-external-cluster-resources.py. This method has no side effects
        (does not modify config, delete users, or create namespaces).

        Args:
            include_rgw (bool): If True (default), include --rgw-endpoint.
                Set to False for clusters without RGW deployed.

        Returns:
            str: Parameter string for run_exporter_script().

        """
        rbd_name = config.ENV_DATA.get("rbd_name") or defaults.RBD_NAME
        cluster_name = config.ENV_DATA.get("cluster_name") or defaults.RHCS_CLUSTER_NAME

        params = f"--rbd-data-pool-name {rbd_name}"

        if include_rgw:
            rgw_endpoint_port = self.get_rgw_endpoint_api_port()
            rgw_endpoint = get_rgw_endpoint()
            params += f" --rgw-endpoint {rgw_endpoint}:{rgw_endpoint_port}"

        ceph_fs_name = config.ENV_DATA.get("cephfs_name") or self.get_ceph_fs()

        if config.ENV_DATA["restricted-auth-permission"]:
            if config.ENV_DATA["use_k8s_cluster_name"]:
                params = f"{params} --k8s-cluster-name {cluster_name}"
            else:
                params = f"{params} --cluster-name {cluster_name}"
            params = f"{params} --cephfs-filesystem-name {ceph_fs_name} --restricted-auth-permission true"

        if "." in rbd_name or "_" in rbd_name:
            alias_rbd_name = rbd_name.replace(".", "-").replace("_", "-")
            params = (
                f"{params} --restricted-auth-permission true --cluster-name {cluster_name} "
                f"--alias-rbd-data-pool-name {alias_rbd_name}"
            )

        if config.ENV_DATA.get("rgw-realm"):
            rgw_realm = config.ENV_DATA["rgw-realm"]
            rgw_zonegroup = config.ENV_DATA["rgw-zonegroup"]
            rgw_zone = config.ENV_DATA["rgw-zone"]
            params = (
                f"{params} --rgw-realm-name {rgw_realm} --rgw-zonegroup-name {rgw_zonegroup} "
                f"--rgw-zone-name {rgw_zone}"
            )

        if config.EXTERNAL_MODE.get("run_as_user"):
            ceph_user = config.EXTERNAL_MODE["run_as_user"]
            params = f"{params} --run-as-user {ceph_user}"

        if config.EXTERNAL_MODE.get("use_rbd_namespace"):
            rbd_namespace = config.EXTERNAL_MODE.get("rbd_namespace")
            if rbd_namespace:
                params = f"{params} --rados-namespace {rbd_namespace}"
                if "restricted-auth-permission" not in params:
                    params += " --restricted-auth-permission true"
                if "cluster-name" not in params:
                    params += f" --k8s-cluster-name {cluster_name}"

        return params

    def get_external_cluster_details(self):
        """
        Gets the external RHCS cluster details and updates to config.EXTERNAL_MODE

        Raises:
            ExternalClusterExporterRunFailed: If exporter script failed to run on external RHCS cluster

        """
        rbd_name = config.ENV_DATA.get("rbd_name") or defaults.RBD_NAME
        cluster_name = config.ENV_DATA.get("cluster_name") or defaults.RHCS_CLUSTER_NAME

        # Side effects that must run before building params so that
        # build_exporter_base_params() sees restricted-auth and alias flags
        if "." in rbd_name or "_" in rbd_name:
            config.ENV_DATA["restricted-auth-permission"] = True
            config.ENV_DATA["alias_rbd_name"] = rbd_name.replace(".", "-").replace(
                "_", "-"
            )

        params = self.build_exporter_base_params()

        # remove user 'rgw-admin-ops-user' if it exists since user creation is handled by
        # external python script with necessary caps
        if config.MULTICLUSTER.get(
            "multicluster_mode"
        ) == constants.MDR_MODE and not config.MULTICLUSTER.get("primary_cluster"):
            logger.info(
                "Skipping removal of rgw-admin-ops-user user for non primary cluster in MDR setup"
            )
        else:
            self.remove_rgw_user()

        if config.EXTERNAL_MODE.get("use_rbd_namespace"):
            rbd_namespace = config.EXTERNAL_MODE.get("rbd_namespace")
            if not rbd_namespace:
                rbd_namespace = self.create_rbd_namespace(rbd=rbd_name)
                config.EXTERNAL_MODE["rbd_namespace"] = rbd_namespace
                # Append params that build_exporter_base_params skipped
                # (namespace didn't exist in config yet when it ran)
                params += f" --rados-namespace {rbd_namespace}"
                if "restricted-auth-permission" not in params:
                    params += " --restricted-auth-permission true"
                if "cluster-name" not in params:
                    params += f" --k8s-cluster-name {cluster_name}"

            if not config.ENV_DATA.get("restricted-auth-permission"):
                config.ENV_DATA["restricted-auth-permission"] = True

        out = self.run_exporter_script(params=params)

        # encode the exporter script output to base64
        external_cluster_details = encode(out)
        logger.debug(f"Encoded external cluster details: {external_cluster_details}")

        # update the encoded message to config
        config.EXTERNAL_MODE["external_cluster_details"] = external_cluster_details

    def upload_exporter_script(self):
        """
        Upload exporter script to RHCS cluster

        Returns:
            str: absolute path to exporter script

        """
        ocs_version = version.get_semantic_ocs_version_from_config()
        use_configmap = True
        if ocs_version <= version.VERSION_4_18:
            use_configmap = False
        script_path = generate_exporter_script(use_configmap=use_configmap)
        remote_script_path = f"/tmp/{os.path.basename(script_path)}"
        upload_file(
            self.host,
            script_path,
            remote_script_path,
            self.user,
            self.password,
            self.ssh_key,
            ssh_connection=self.rhcs_conn if self.jump_host else None,
        )
        return remote_script_path

    def upload_rgw_cert_ca(self):
        """
        Upload RGW Cert CA to RHCS cluster

        Returns:
            str: absolute path to the CA Cert

        """
        rgw_cert_ca_path = get_and_apply_rgw_cert_ca(apply=False)
        remote_rgw_cert_ca_path = "/tmp/rgw-cert-ca.pem"
        upload_file(
            self.host,
            rgw_cert_ca_path,
            remote_rgw_cert_ca_path,
            self.user,
            self.password,
            self.ssh_key,
            ssh_connection=self.rhcs_conn if self.jump_host else None,
        )
        return remote_rgw_cert_ca_path

    def get_admin_keyring(self):
        """
        Fetches admin keyring from external RHCS cluster and updates to config.EXTERNAL_MODE
        """
        cmd = "ceph auth get client.admin"
        _, out, _ = self.rhcs_conn.exec_cmd(cmd)
        client_admin = out.split()
        for index, value in enumerate(client_admin):
            if value == "key":
                config.EXTERNAL_MODE["admin_keyring"]["key"] = client_admin[index + 2]
                return

    def fetch_cephadm_root_ca_cert_pem(self):
        """
        Return the cephadm-managed root CA (``cephadm_root_ca_cert``) from this host.

        Used for Ceph 19+ when RGW TLS is signed by the cephadm CA. Runs
        ``cephadm shell``; prefixes ``sudo`` when the SSH user is not ``root``.

        Returns:
            str: PEM text (with trailing newline).

        Raises:
            ExternalClusterExporterRunFailed: If the remote command fails or PEM is empty.

        """
        cmd = (
            "/usr/sbin/cephadm shell -- ceph orch certmgr cert get cephadm_root_ca_cert"
        )
        if self.user != "root":
            cmd = f"sudo {cmd}"
        ret, out, err = self.rhcs_conn.exec_cmd(cmd)
        if ret != 0:
            raise ExternalClusterExporterRunFailed(
                f"cephadm_root_ca_cert fetch failed: {err}"
            )
        pem = _normalize_cephadm_certmgr_stdout(out)
        if not pem:
            raise ExternalClusterExporterRunFailed(
                "cephadm_root_ca_cert fetch returned empty output"
            )
        return pem if pem.endswith("\n") else f"{pem}\n"

    def fetch_rgw_server_certificate(self, rgw_endpoint):
        """
        Fetch the actual certificate presented by the RGW server for Ceph < 19.0.

        For Reef (18.x), cephadm doesn't have certmgr, so we extract the cert
        directly from the RGW server endpoint using openssl s_client.

        Args:
            rgw_endpoint (str): RGW endpoint (e.g., "10.1.112.76:443")

        Returns:
            str: PEM-encoded certificate

        Raises:
            ExternalClusterExporterRunFailed: If certificate fetch fails or endpoint format is invalid

        """
        # Validate RGW endpoint format to prevent command injection
        # Expected format: host:port or [ipv6]:port
        endpoint_pattern = r"^\[?[A-Za-z0-9:._-]+\]?:\d{1,5}$"
        if not re.fullmatch(endpoint_pattern, rgw_endpoint):
            raise ExternalClusterExporterRunFailed(
                f"Invalid RGW endpoint format: {rgw_endpoint}. "
                f"Expected format: 'host:port' or '[ipv6]:port'"
            )

        # Use openssl s_client to fetch the server certificate
        # Use shlex.quote() to safely escape the endpoint parameter
        cmd = (
            f"echo | openssl s_client -connect {shlex.quote(rgw_endpoint)} -showcerts 2>/dev/null | "
            f"openssl x509 -outform PEM"
        )

        ret, out, err = self.rhcs_conn.exec_cmd(cmd)
        if ret != 0 or not out or "BEGIN CERTIFICATE" not in out:
            raise ExternalClusterExporterRunFailed(
                f"Failed to fetch RGW server certificate from {rgw_endpoint}: {err}"
            )

        pem = out.strip()
        if not pem.endswith("\n"):
            pem = f"{pem}\n"

        logger.info(f"Fetched RGW server certificate from {rgw_endpoint} (Ceph < 19.0)")
        return pem

    def get_rgw_endpoint_api_port(self):
        """
        Fetches rgw endpoint api port.

        For ceph 6.x, get port information from cephadm ls,
        for ceph 5.x, get port information from ceph config dump and for
        ceph 4.x, get port information from ceph.conf on rgw node

        Returns:
            str: RGW endpoint port

        """
        port = None
        try:
            # For ceph 6.x versions
            cmd = "cephadm ls"
            rgw_node = get_rgw_endpoint()
            rgw_conn = Connection(
                host=rgw_node,
                user=self.user,
                private_key=self.ssh_key,
                password=self.password,
            )
            _, out, _ = rgw_conn.exec_cmd(cmd)
            daemons = json.loads(out)
            port = [
                daemon["ports"][0]
                for daemon in daemons
                if daemon["service_name"].startswith("rgw")
            ][0]
            # if port doesn't have value, need to check followup way
            if not port:
                raise AttributeError(
                    "Command `cephadm ls` output doesn't have information about rgw port."
                )
        except Exception as ex:
            logger.info(f"{ex})")
            try:
                # For ceph 5.x versions
                cmd = "ceph config dump -f json"
                _, out, _ = self.rhcs_conn.exec_cmd(cmd)
                config_dump = json.loads(out)
                for each in config_dump:
                    if each["name"].lower() == "rgw_frontends":
                        # normal deployment: "beast port=80"
                        # RGW with SSL: "beast ssl_port=443 ssl_certificate=config://rgw/cert/rgw.rgw.ssl"
                        for option in each["value"].split():
                            if "port=" in option:
                                port = option.split("=")[-1]
                        break
                # if port doesn't have value, need to check ceph.conf from rgw node
                if not port:
                    raise AttributeError(
                        "config dump has no rgw port information. checking ceph.conf file on rgw node"
                    )
            except Exception as ex:
                # For ceph 4.x versions
                logger.info(ex)
                cmd = "grep -e '^rgw frontends' /etc/ceph/ceph.conf"
                rgw_node = get_rgw_endpoint()
                rgw_conn = Connection(
                    host=rgw_node,
                    user=self.user,
                    private_key=self.ssh_key,
                    password=self.password,
                )
                _, out, _ = rgw_conn.exec_cmd(cmd)
                port = out.split(":")[-1]

        if not port:
            raise ExternalClusterRGWEndPointPortMissing

        logger.info(f"External cluster rgw endpoint api port: {port}")
        return port

    def get_rhel_version(self):
        """
        Fetches the RHEL version on external RHCS cluster

        Returns:
            str: RHEL version

        """
        pattern = re.compile(r".*(\d+.\d+).*")
        cmd = "cat /etc/redhat-release"
        _, out, _ = self.rhcs_conn.exec_cmd(cmd)
        logger.debug(f"RHEL version on external RHCS cluster is {out}")
        return pattern.search(out).groups()[0]

    def update_permission_caps(self, user=None):
        """
        Update permission caps on the external RHCS cluster
        """
        if user:
            params = f"--upgrade --run-as-user={user}"
        else:
            params = "--upgrade"
        out = self.run_exporter_script(params=params)
        logger.info(f"updated permissions for the user are set as {out}")

    def upload_config_ini_file(self, params):
        """
        Upload config.ini file that is used for external cluster
        exporter script --config-file param

        Args:
            params (str): Parameter to pass to exporter script

        Returns:
            str: absolute path to config.ini file

        """
        script_path = create_config_ini_file(params=params)
        upload_file(
            self.host,
            script_path,
            script_path,
            self.user,
            self.password,
            self.ssh_key,
            ssh_connection=self.rhcs_conn if self.jump_host else None,
        )
        return script_path

    def run_exporter_script(self, params):
        """
        Runs the exporter script on RHCS cluster

        Args:
            params (str): Parameter to pass to exporter script

        Returns:
            str: output of exporter script

        """
        # upload exporter script to external RHCS cluster
        script_path = self.upload_exporter_script()

        # upload RGW CA Cert and add required params (for RGW with SSL)
        if config.EXTERNAL_MODE.get("rgw_secure"):
            remote_rgw_cert_ca_path = self.upload_rgw_cert_ca()
            params = f"{params} --rgw-tls-cert-path {remote_rgw_cert_ca_path}"

        # get external RHCS rhel version
        rhel_version = self.get_rhel_version()
        python_version = "python3"
        if version.get_semantic_version(rhel_version) < version.get_semantic_version(
            "8"
        ):
            python_version = "python"

        # run the exporter script on external RHCS cluster
        ocs_version = version.get_semantic_ocs_version_from_config()
        # if condition is for the new feature introduced in
        # 4.17 in OCSQE-2249 where this covers the test case
        # with polarian id OCS-6196
        if (
            "--upgrade" not in params
            and ocs_version >= version.VERSION_4_17
            and config.ENV_DATA.get("rhcs_external_use_config_file")
        ):
            # upload config.ini file to external RHCS cluster
            config_ini_path = self.upload_config_ini_file(params)
            cmd = f"{python_version} {script_path} --config-file {config_ini_path}"
        else:
            cmd = f"{python_version} {script_path} {params}"
        retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
        if retcode != 0 or err != "":
            logger.error(
                f"Failed to run {script_path} with parameters {params}. Error: {err}"
            )
            raise ExternalClusterExporterRunFailed
        return out

    def create_object_store_user(self):
        """
        Create object store user on external cluster and update
        access_key and secret_key to config
        """
        # check if object store user exists or not
        user = defaults.EXTERNAL_CLUSTER_OBJECT_STORE_USER
        if self.is_object_store_user_exists(user):
            logger.info(f"object store user {user} already exists in external cluster")
            # get the access and secret key
            access_key, secret_key = self.get_object_store_user_secrets(user)
        else:
            # create new object store user
            logger.info(f"creating new object store user {user}")
            cmd = (
                f"radosgw-admin user create --uid {user} --display-name "
                f'"Rook RGW Admin Ops user" --caps "buckets=*;users=*;usage=read;metadata=read;zone=read"'
            )
            retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
            if retcode != 0:
                logger.error(f"Failed to create object store user. Error: {err}")
                raise ExternalClusterObjectStoreUserCreationFailed

            # get the access and secret key
            objectstore_user_details = json.loads(out)
            access_key = objectstore_user_details["keys"][0]["access_key"]
            secret_key = objectstore_user_details["keys"][0]["secret_key"]

        # update access_key and secret_key in config.EXTERNAL_MODE
        config.EXTERNAL_MODE["access_key_rgw-admin-ops-user"] = access_key
        config.EXTERNAL_MODE["secret_key_rgw-admin-ops-user"] = secret_key

    def is_object_store_user_exists(self, user, realm=None):
        """
        Checks whether user exists in external cluster

        Args:
            user (str): Object store user name
            realm (str): Name of realm to check

        Returns:
            bool: True if user exists, otherwise false

        """
        cmd = "radosgw-admin user list"
        if realm:
            cmd = f"{cmd} --rgw-realm {realm}"
        _, out, _ = self.rhcs_conn.exec_cmd(cmd)
        objectstore_user_list = json.loads(out)
        if user in objectstore_user_list:
            return True

    def get_object_store_user_secrets(self, user):
        """
        Get the access and secret key for user

        Returns:
            tuple: tuple which contains access_key and secret_key

        """
        cmd = f"radosgw-admin user info --uid {user}"
        _, out, _ = self.rhcs_conn.exec_cmd(cmd)
        user_details = json.loads(out)
        return (
            user_details["keys"][0]["access_key"],
            user_details["keys"][0]["secret_key"],
        )

    def get_ceph_fs(self):
        """
        Fetches the ceph filesystem name

        Returns:
            str: ceph filesystem name

        Raises:
            ExternalClusterCephfsMissing: in case of ceph filesystem doesn't exist

        """
        cmd = "ceph fs ls --format json"
        _, out, _ = self.rhcs_conn.exec_cmd(cmd)
        ceph_fs_list = json.loads(out)
        if not ceph_fs_list:
            raise ExternalClusterCephfsMissing
        return ceph_fs_list[0]["name"]

    def remove_rbd_images(self, images, pool):
        """
        Removes rbd images from external RHCS cluster

        Args:
            images (list): List of rbd images to delete
            pool (str): rbd pool name

        """
        logger.debug(f"deleting rbd images {images} from external RHCS cluster")
        for each_image in images:
            cmd = f"rbd rm {each_image} -p {pool}"
            self.rhcs_conn.exec_cmd(cmd)

    def enable_secure_connection_mode(self):
        """
        Enables secure connection mode for RHCS cluster
        """
        logger.debug("Enabling secure connection mode for external RHCS cluster")
        cmds = (
            "ceph config set global ms_client_mode secure;"
            "ceph config set global ms_cluster_mode secure;"
            "ceph config set global ms_service_mode secure;"
            "ceph config set global rbd_default_map_options ms_mode=secure"
        )
        self.rhcs_conn.exec_cmd(cmds)

    def remove_rgw_user(self, user=None):
        """
        Remove RGW user if it exists

        Args:
            user (str): RGW user name

        """
        user = user if user else defaults.EXTERNAL_CLUSTER_OBJECT_STORE_USER
        if self.is_rgw_user_exists(user):
            logger.info(
                f"Deleting {user} since rgw user {user} will be created by "
                f"external python script with all necessary caps"
            )
            cmd = f"radosgw-admin user rm --uid={user}"
            self.rhcs_conn.exec_cmd(cmd)
        else:
            logger.debug(
                f"rgw user {user} doesn't exists and it will be created by external python script"
            )

    def is_rgw_user_exists(self, user):
        """
        Checks whether RGW user exists or not

        Args:
            user (str): RGW user name

        Returns:
            bool: True incase user exists, otherwise False

        """
        cmd = "radosgw-admin user list"
        _, out, _ = self.rhcs_conn.exec_cmd(cmd)
        rgw_user_list = json.loads(out)
        logger.debug(f"RGW users: {rgw_user_list}")
        return True if user in rgw_user_list else False

    def create_rbd_namespace(self, rbd, namespace=None):
        """
        Create RBD namespace

        Args:
            rbd (str): RBD pool name where namespace has to create
            namespace (str): Name of RBD namespace

        Returns:
            str: RBD Namespace name

        Raises:
            ExternalClusterRBDNamespaceCreationFailed: In case fails to create RBD namespace

        """
        namespace = namespace or f"rbd-namespace-{uuid.uuid4().hex[:8]}"
        logger.info(f"creating RBD namespace {namespace}")
        cmd = f"rbd namespace create {rbd}/{namespace}"
        retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
        if retcode != 0:
            logger.error(f"Failed to create RBD namespace in {rbd}. Error: {err}")
            raise ExternalClusterRBDNamespaceCreationFailed
        return namespace

    def disable_certificate_check(self):
        """
        Disable certificate check

        Raises:
            ExternalClusterDisableCertificateCheckFailed: In case fails to disable certificate check

        """
        cmd = "ceph config set mgr mgr/cephadm/certificate_check_period 0"
        retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
        if retcode != 0:
            logger.error(f"Failed to disable certificate check. Error: {err}")
            raise ExternalClusterDisableCertificateCheckFailed

    def enable_replica_one_pools(self) -> None:
        """
        Enable replica-1 pool support on the external Ceph cluster.

        Executes: ceph config set mon mon_allow_pool_size_one true

        Raises:
            ExternalClusterReplica1ConfigurationFailed: If configuration fails.

        """
        logger.info("Enabling replica-1 pool support on external Ceph cluster")
        self.exec_external_ceph_cmd(
            cmd="ceph config set mon mon_allow_pool_size_one true",
            error_msg="Failed to enable replica-1 pools",
            exception_class=ExternalClusterReplica1ConfigurationFailed,
        )
        logger.info("Replica-1 pool support enabled successfully")

    def create_zone_crush_rules(
        self, topology_config: TopologyReplica1Config
    ) -> list[str]:
        """
        Create CRUSH rules for each zone in the topology configuration.

        Executes: ceph osd crush rule create-simple <rule-name> <host> osd

        Args:
            topology_config (TopologyReplica1Config): Topology configuration.

        Returns:
            list[str]: List of created rule names.

        Raises:
            ExternalClusterCrushRuleCreationFailed: If rule creation fails.

        """
        if not topology_config.zones:
            raise ValueError("topology_config.zones cannot be empty")

        # Get existing CRUSH rules for idempotency check
        _, out, _ = self.exec_external_ceph_cmd(
            cmd="ceph osd crush rule ls",
            error_msg="Failed to list existing CRUSH rules",
            exception_class=ExternalClusterCrushRuleCreationFailed,
        )
        existing_rules = out.strip().split("\n") if out.strip() else []
        logger.debug(f"Existing CRUSH rules: {existing_rules}")

        created_rules = []
        for zone in topology_config.zones:
            rule_name = f"{zone.zone_name}-rule"

            # Skip if rule already exists (idempotency)
            if rule_name in existing_rules:
                logger.info(f"CRUSH rule {rule_name} already exists, skipping creation")
                created_rules.append(rule_name)
                continue

            logger.info(f"Creating CRUSH rule: {rule_name} for host: {zone.host_name}")
            self.exec_external_ceph_cmd(
                cmd=f"ceph osd crush rule create-simple {rule_name} {zone.host_name} osd",
                error_msg=f"Failed to create CRUSH rule {rule_name}",
                exception_class=ExternalClusterCrushRuleCreationFailed,
            )
            logger.info(f"Created CRUSH rule: {rule_name}")
            created_rules.append(rule_name)

        return created_rules

    def create_rbd_pool(
        self,
        pool_name: str,
        pg_num: int,
        pool_size: int,
        crush_rule: str | None = None,
        min_size: int | None = None,
    ) -> None:
        """
        Create a single replicated RBD pool on the external Ceph cluster.

        Args:
            pool_name (str): Name of the pool to create.
            pg_num (int): Placement groups for the pool.
            pool_size (int): Replication factor.
            crush_rule (str): Optional CRUSH rule name for the pool.
            min_size (int): Optional minimum replication size.

        Raises:
            ExternalClusterPoolCreationFailed: If any step fails.

        """
        rule_part = f" {crush_rule}" if crush_rule else ""
        self.exec_external_ceph_cmd(
            cmd=f"ceph osd pool create {pool_name} {pg_num} {pg_num} replicated{rule_part}",
            error_msg=f"Failed to create pool {pool_name}",
            exception_class=ExternalClusterPoolCreationFailed,
        )

        force_flag = " --yes-i-really-mean-it" if pool_size == 1 else ""
        self.exec_external_ceph_cmd(
            cmd=f"ceph osd pool set {pool_name} size {pool_size}{force_flag}",
            error_msg=f"Failed to set size {pool_size} for pool {pool_name}",
            exception_class=ExternalClusterPoolCreationFailed,
        )

        if min_size is not None:
            self.exec_external_ceph_cmd(
                cmd=f"ceph osd pool set {pool_name} min_size {min_size}",
                error_msg=f"Failed to set min_size {min_size} for pool {pool_name}",
                exception_class=ExternalClusterPoolCreationFailed,
            )

        self.exec_external_ceph_cmd(
            cmd=f"ceph osd pool application enable {pool_name} rbd",
            error_msg=f"Failed to enable rbd for pool {pool_name}",
            exception_class=ExternalClusterPoolCreationFailed,
        )

        logger.info(f"Created RBD pool: {pool_name} (size={pool_size})")

    def create_replica_one_pools(
        self, topology_config: TopologyReplica1Config
    ) -> list[str]:
        """
        Create replica-1 RBD pools for each zone in the topology configuration.

        For each zone executes:
        - ceph osd pool create <pool-name> <pg_num> <pg_num> replicated <rule-name>
        - ceph osd pool set <pool-name> size 1 --yes-i-really-mean-it
        - ceph osd pool set <pool-name> min_size 1
        - ceph osd pool application enable <pool-name> rbd

        Args:
            topology_config (TopologyReplica1Config): Topology configuration.

        Returns:
            list[str]: List of created pool names.

        Raises:
            ExternalClusterPoolCreationFailed: If pool creation fails.

        """
        if not topology_config.zones:
            raise ValueError("topology_config.zones cannot be empty")

        # Get existing pools for idempotency check
        _, out, _ = self.exec_external_ceph_cmd(
            cmd="ceph osd pool ls",
            error_msg="Failed to list existing pools",
            exception_class=ExternalClusterPoolCreationFailed,
        )
        existing_pools = out.strip().split("\n") if out.strip() else []
        logger.debug(f"Existing pools: {existing_pools}")

        created_pools = []
        for zone, pool_name in zip(topology_config.zones, topology_config.pool_names):
            rule_name = f"{zone.zone_name}-rule"
            pg_num = topology_config.pg_num

            # Skip if pool already exists (idempotency)
            if pool_name in existing_pools:
                logger.info(f"Pool {pool_name} already exists, skipping creation")
                created_pools.append(pool_name)
                continue

            self.create_rbd_pool(
                pool_name,
                pg_num=pg_num,
                pool_size=1,
                crush_rule=rule_name,
                min_size=1,
            )
            created_pools.append(pool_name)

        return created_pools

    def verify_replica_one_setup(
        self, expected_pools: list[str], expected_rules: list[str]
    ) -> None:
        """
        Verify that replica-1 pools and CRUSH rules are properly configured.

        Args:
            expected_pools (list[str]): List of expected pool names.
            expected_rules (list[str]): List of expected CRUSH rule names.

        Raises:
            ExternalClusterReplica1ConfigurationFailed: If verification fails.

        """
        logger.info("Verifying replica-1 setup")

        # Verify CRUSH rules exist
        _, out, _ = self.exec_external_ceph_cmd(
            cmd="ceph osd crush rule ls",
            error_msg="Failed to list CRUSH rules",
            exception_class=ExternalClusterReplica1ConfigurationFailed,
        )

        existing_rules = out.strip().split("\n")
        logger.info(f"Existing CRUSH rules for verification: {existing_rules}")
        for rule in expected_rules:
            if rule not in existing_rules:
                raise ExternalClusterReplica1ConfigurationFailed(
                    f"CRUSH rule {rule} not found. Existing rules: {existing_rules}"
                )
        logger.info(f"All expected CRUSH rules exist: {expected_rules}")

        # Verify pools exist with correct configuration
        for pool in expected_pools:
            _, out, _ = self.exec_external_ceph_cmd(
                cmd=f"ceph osd pool get {pool} size",
                error_msg=f"Pool {pool} not found or cannot get size",
                exception_class=ExternalClusterReplica1ConfigurationFailed,
            )

            if "size: 1" not in out:
                raise ExternalClusterReplica1ConfigurationFailed(
                    f"Pool {pool} does not have size 1. Got: {out}"
                )
        logger.info(f"All expected pools have size 1: {expected_pools}")

        logger.info("Replica-1 setup verification passed")

    def discover_zones_from_crush_tree(self) -> list[ZoneConfig]:
        """
        Auto-detect zones from the external cluster's CRUSH tree.

        Queries 'ceph osd tree' and extracts host-type buckets.
        Each host becomes a zone (zone-a, zone-b, ...).

        Returns:
            list[ZoneConfig]: Zone configurations derived from CRUSH hosts.

        Raises:
            CommandFailed: If 'ceph osd tree' command fails.

        """
        _, out, _ = self.exec_external_ceph_cmd(
            cmd="ceph osd tree --format json",
            error_msg="Failed to get OSD tree from external cluster",
            exception_class=CommandFailed,
        )
        osd_tree = json.loads(out)
        hosts = [node["name"] for node in osd_tree["nodes"] if node["type"] == "host"]

        zones = [
            ZoneConfig(zone_name=f"zone-{chr(ord('a') + i)}", host_name=host)
            for i, host in enumerate(hosts)
        ]
        logger.info(f"Auto-detected {len(zones)} zones from CRUSH tree: {zones}")
        return zones

    def build_topology_replica1_config(self) -> TopologyReplica1Config:
        """
        Build topology configuration from EXTERNAL_MODE config or CRUSH tree.

        Reads replica1_zones from config.EXTERNAL_MODE if available.
        Falls back to auto-detecting zones from the cluster's CRUSH tree.

        Returns:
            TopologyReplica1Config: Configuration for replica-1 setup.

        """
        zones_config = config.EXTERNAL_MODE.get("replica1_zones", [])
        if zones_config:
            logger.info(f"Using replica1_zones from config: {zones_config}")
            zones = [
                ZoneConfig(
                    zone_name=z["zone_name"],
                    host_name=z["host_name"],
                    pool_name=z.get("pool_name", ""),
                )
                for z in zones_config
            ]
        else:
            logger.info("replica1_zones not configured, auto-detecting from CRUSH tree")
            zones = self.discover_zones_from_crush_tree()

        logger.info(f"Built topology config with {len(zones)} zones")
        return TopologyReplica1Config(zones=zones)

    def setup_topology_replica_one(
        self, topology_config: TopologyReplica1Config
    ) -> dict[str, list[str]]:
        """
        Complete setup of topology-based replica-1 provisioning.

        This is the main entry point that orchestrates:
        1. Enable replica-1 pools (mon_allow_pool_size_one)
        2. Create CRUSH rules for each zone
        3. Create replica-1 pools for each zone
        4. Verify the setup

        Args:
            topology_config (TopologyReplica1Config): Topology configuration.

        Returns:
            dict[str, list[str]]: Dictionary with keys 'pools' and 'rules',
                each containing list of created resource names.

        Raises:
            ExternalClusterReplica1ConfigurationFailed: If setup fails.
            ValueError: If topology_config.zones is empty.

        """
        if not topology_config.zones:
            raise ValueError("topology_config.zones cannot be empty")

        logger.info(
            f"Starting topology-based replica-1 setup with {len(topology_config.zones)} zones"
        )

        # Step 1: Enable replica-1 pools
        self.enable_replica_one_pools()

        # Step 2: Create CRUSH rules
        created_rules = self.create_zone_crush_rules(topology_config)

        # Step 3: Create pools
        created_pools = self.create_replica_one_pools(topology_config)

        # Step 4: Verify setup
        self.verify_replica_one_setup(created_pools, created_rules)

        result = {"pools": created_pools, "rules": created_rules}
        logger.info(
            f"Topology-based replica-1 setup completed. "
            f"Pools: {created_pools}, Rules: {created_rules}"
        )
        return result

    def cleanup_replica_one_pools(self, pool_names: list[str]) -> None:
        """
        Remove replica-1 pools from external cluster.

        Note:
            This method logs warnings for failed deletions but does not raise
            exceptions to allow cleanup of remaining resources.

        Args:
            pool_names (list[str]): List of pool names to remove.

        """
        logger.info(f"Cleaning up replica-1 pools: {pool_names}")

        # Save current pool deletion config
        _, original_value, _ = self.rhcs_conn.exec_cmd(
            "ceph config get mon mon_allow_pool_delete"
        )
        original_value = original_value.strip() or "false"
        logger.info(f"Saved mon_allow_pool_delete original value: {original_value}")

        # Enable pool deletion
        cmd = "ceph config set mon mon_allow_pool_delete true"
        retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
        if retcode != 0:
            logger.warning(f"Failed to enable pool deletion: {err}")

        try:
            for pool_name in pool_names:
                cmd = f"ceph osd pool delete {pool_name} {pool_name} --yes-i-really-really-mean-it"
                logger.info(f"Deleting pool: {pool_name}")
                retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
                if retcode != 0:
                    logger.warning(f"Failed to delete pool {pool_name}: {err}")
        finally:
            # Restore original pool deletion config
            cmd = f"ceph config set mon mon_allow_pool_delete {original_value}"
            self.rhcs_conn.exec_cmd(cmd)

        logger.info("Cleanup of replica-1 pools completed")

    def cleanup_zone_crush_rules(self, rule_names: list[str]) -> None:
        """
        Remove CRUSH rules from external cluster.

        Args:
            rule_names (list[str]): List of rule names to remove.

        """
        logger.info(f"Cleaning up CRUSH rules: {rule_names}")

        for rule_name in rule_names:
            cmd = f"ceph osd crush rule rm {rule_name}"
            logger.info(f"Deleting CRUSH rule: {rule_name}")
            retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
            if retcode != 0:
                logger.warning(f"Failed to delete CRUSH rule {rule_name}: {err}")

        logger.info("Cleanup of CRUSH rules completed")

    def create_topology_pools(
        self,
        pool_names: list[str],
        pool_size: int = 3,
        pg_num: int = 32,
    ) -> list[str]:
        """
        Create replicated RBD pools for topology-aware provisioning.

        Unlike create_replica_one_pools(), this creates standard replicated pools
        (size >= 2) without per-zone CRUSH rules — the default replicated_rule
        distributes replicas across hosts automatically.

        For each pool executes:
        - ceph osd pool create <pool-name> <pg_num> <pg_num> replicated
        - ceph osd pool set <pool-name> size <pool_size>
        - ceph osd pool application enable <pool-name> rbd

        Args:
            pool_names (list[str]): List of pool names to create.
            pool_size (int): Replication factor (default 3).
            pg_num (int): Placement groups per pool (default 32).

        Returns:
            list[str]: List of created pool names.

        Raises:
            ExternalClusterPoolCreationFailed: If pool creation fails.

        """
        if not pool_names:
            raise ValueError("pool_names cannot be empty")

        _, out, _ = self.exec_external_ceph_cmd(
            cmd="ceph osd pool ls",
            error_msg="Failed to list existing pools",
            exception_class=ExternalClusterPoolCreationFailed,
        )
        existing_pools = out.strip().split("\n") if out.strip() else []

        created_pools = []
        for pool_name in pool_names:
            if pool_name in existing_pools:
                logger.info(f"Pool {pool_name} already exists, skipping creation")
                created_pools.append(pool_name)
                continue

            self.create_rbd_pool(pool_name, pg_num=pg_num, pool_size=pool_size)
            created_pools.append(pool_name)

        return created_pools

    def run_topology_exporter_script(
        self, topology_config: TopologyReplica1Config, additional_params: str = ""
    ) -> list[dict]:
        """
        Run exporter script with topology flags for replica-1 pools.

        Builds topology-specific parameters from the configuration:
        - --topology-pools (comma-separated pool names)
        - --topology-failure-domain-label (from constants.ZONE_LABEL)
        - --topology-failure-domain-values (comma-separated zone names)
        - --rbd-data-pool-name (first pool in list)
        - --format json

        Args:
            topology_config (TopologyReplica1Config): Topology configuration with zones.
            additional_params (str): Additional parameters to pass to the script.

        Returns:
            list[dict]: Parsed JSON output from the exporter script.

        Raises:
            ExternalClusterExporterRunFailed: If script execution fails.
            ValueError: If topology_config.zones is empty.

        """
        if not topology_config.zones:
            raise ValueError("topology_config.zones cannot be empty")

        pool_names = topology_config.pool_names
        zone_names = topology_config.zone_names

        # Build topology params
        topology_params = (
            f"--rbd-data-pool-name {pool_names[0]} "
            f"--topology-pools {','.join(pool_names)} "
            f"--topology-failure-domain-label {constants.ZONE_LABEL} "
            f"--topology-failure-domain-values {','.join(zone_names)} "
            f"--format json"
        )

        if additional_params:
            topology_params = f"{topology_params} {additional_params}"

        logger.info(f"Running topology exporter with params: {topology_params}")

        # Run the exporter script using existing infrastructure
        out = self.run_exporter_script(params=topology_params)

        # Parse JSON output
        try:
            resources = json.loads(out)
            logger.info(f"Parsed {len(resources)} resources from exporter output")
            return resources
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse exporter JSON output: {e}")
            logger.debug(f"Raw output: {out}")
            raise ExternalClusterExporterRunFailed(
                f"Failed to parse topology exporter output as JSON: {e}"
            ) from e

    def apply_topology_export_resources(
        self, resources: list[dict], namespace: str | None = None
    ) -> dict[str, list[str]]:
        """
        Apply exported topology resources (secrets, configmaps) to the cluster.

        Args:
            resources (list[dict]): List of resource dicts from exporter script.
                Each dict has 'name', 'kind', and 'data' keys.
            namespace (str): Namespace to create resources in.
                Defaults to cluster_namespace from config.

        Returns:
            dict[str, list[str]]: Dictionary with keys 'secrets' and 'configmaps',
                each containing list of created resource names.

        Raises:
            CommandFailed: If resource creation fails.

        """
        namespace = namespace or config.ENV_DATA["cluster_namespace"]
        created = {"secrets": [], "configmaps": []}

        for resource in resources:
            name = resource.get("name")
            kind = resource.get("kind")
            data = resource.get("data", {})

            if kind == "Secret":
                resource_data = {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": name, "namespace": namespace},
                    "type": "Opaque",
                    "stringData": data,
                }
            elif kind == "ConfigMap":
                resource_data = {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": name, "namespace": namespace},
                    "data": data,
                }

            else:
                logger.debug(f"Skipping resource kind '{kind}': {name}")
                continue

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            ) as f:
                yaml.dump(resource_data, f)
                tmp_path = f.name

            try:
                ocp_resource = OCP(kind=kind, namespace=namespace)
                ocp_resource.apply(yaml_file=tmp_path)
                key = "secrets" if kind == "Secret" else "configmaps"
                created[key].append(name)
                logger.info(f"Created {kind}: {name}")
            finally:
                os.remove(tmp_path)

        logger.info(
            f"Applied topology resources - Secrets: {created['secrets']}, "
            f"ConfigMaps: {created['configmaps']}"
        )
        return created


def save_external_cluster_secret():
    """
    Save the current external cluster secret data for later restoration.

    Returns:
        str: The base64-encoded external_cluster_details value.

    """
    ns = config.ENV_DATA["cluster_namespace"]
    secret_ocp = OCP(kind="Secret", namespace=ns)
    secret_data = secret_ocp.get(resource_name="rook-ceph-external-cluster-details")
    return secret_data["data"]["external_cluster_details"]


def patch_external_cluster_secret(exporter_json_output):
    """
    Patch the rook-ceph-external-cluster-details secret with new exporter output.

    Args:
        exporter_json_output (str): Raw JSON output from the exporter script.

    """
    ns = config.ENV_DATA["cluster_namespace"]
    with tempfile.NamedTemporaryFile(
        mode="w", prefix="external-cluster-details-", suffix=".json", delete=False
    ) as fd:
        fd.write(exporter_json_output)
        tmp_path = fd.name

    try:
        cmd = (
            f"oc set data secret/rook-ceph-external-cluster-details -n {ns} "
            f"--from-file=external_cluster_details={tmp_path}"
        )
        exec_cmd(cmd)
        logger.info("Patched rook-ceph-external-cluster-details secret")
    finally:
        os.unlink(tmp_path)


def restore_external_cluster_secret(original_b64_value):
    """
    Restore the external cluster secret to its original value.

    Args:
        original_b64_value (str): The original base64-encoded value
            from save_external_cluster_secret().

    """
    ns = config.ENV_DATA["cluster_namespace"]
    secret_ocp = OCP(kind="Secret", namespace=ns)
    params = json.dumps(
        [
            {
                "op": "replace",
                "path": "/data/external_cluster_details",
                "value": original_b64_value,
            }
        ]
    )
    secret_ocp.patch(
        resource_name="rook-ceph-external-cluster-details",
        params=params,
        format_type="json",
    )
    logger.info("Restored rook-ceph-external-cluster-details secret")


def get_exporter_script_from_configmap():
    """
    Get the external exporter script from the configmap.
    From 4.18 we can get the external exporter script from the configmap

    Returns:
        str: The exporter script from the configmap

    """
    logger.info(
        f"Get the exporter script from configmap: {constants.EXTERNAL_CLUSTER_SCRIPT_CONFIG}"
    )
    config_map = OCP(
        kind="configmap",
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.EXTERNAL_CLUSTER_SCRIPT_CONFIG,
    )
    exporter_script = config_map.data.get("data", {}).get("script")
    return exporter_script


def get_exporter_script_from_csv():
    """
    Get the external exporter script from the csv.

    From ODF 4.19 the external mode script was removed from the CSV and is
    shipped only in the ConfigMap (rook-ceph-external-cluster-script-config).
    This function must not be used for ODF 4.19+; use get_exporter_script_from_configmap()
    or get_exporter_script(use_configmap=True) instead.

    Returns:
        str: The exporter script from the csv

    Raises:
        ValueError: If running ODF version is 4.19 or above (script is no longer in CSV).
    """
    # From 4.19 the script is only in ConfigMap; avoid KeyError on missing annotation
    try:
        odf_running_version = version.get_ocs_version_from_csv(only_major_minor=True)
    except Exception:
        odf_running_version = version.get_semantic_ocs_version_from_config()
    if odf_running_version >= version.VERSION_4_19:
        raise ValueError(
            "From ODF 4.19 the external mode script is no longer in the CSV; "
            "it is shipped only in the ConfigMap rook-ceph-external-cluster-script-config. "
            "Use get_exporter_script(use_configmap=True) or get_exporter_script_from_configmap()."
        )

    ocs_version = version.get_semantic_ocs_version_from_config()
    operator_name = defaults.ROOK_CEPH_OPERATOR

    if ocs_version <= version.VERSION_4_15:
        operator_name = defaults.OCS_OPERATOR_NAME
    operator_selector = get_selector_for_ocs_operator()
    package_manifest = PackageManifest(
        resource_name=operator_name,
        selector=operator_selector,
    )
    ocs_operator_data = package_manifest.get()
    csv_name = get_csv_name_start_with_prefix(
        csv_prefix=operator_name, namespace=config.ENV_DATA["cluster_namespace"]
    )
    exporter_script = ""
    for each_csv in ocs_operator_data["status"]["channels"]:
        if each_csv["currentCSV"] == csv_name:
            logger.info(f"exporter script for csv: {each_csv['currentCSV']}")
            annotations = each_csv["currentCSVDesc"].get("annotations", {})
            if ocs_version >= version.VERSION_4_16:
                exporter_script = annotations.get("externalClusterScript")
            else:
                exporter_script = annotations.get(
                    "external.features.ocs.openshift.io/export-script"
                )
            if not exporter_script:
                raise ValueError(
                    "CSV does not contain the external mode script annotation. "
                    "On ODF 4.19+ the script is only in ConfigMap; use use_configmap=True."
                )
            break

    return exporter_script


def get_exporter_script(use_configmap=False):
    """
    Get the external exporter script encoded

    Args:
        use_configmap (bool): If True, we will use the configmap to get the external
            exporter script. Otherwise, if False, we will get it from the CSV.

    Returns:
        str: The external exporter script

    """
    logger.info("Get the external exporter script")
    if use_configmap:
        # From 4.18 we can get the external exporter script from the configmap
        exporter_script = get_exporter_script_from_configmap()
    else:
        exporter_script = get_exporter_script_from_csv()

    return exporter_script


def generate_exporter_script(use_configmap=False):
    """
    Generates exporter script for RHCS cluster

    Args:
        use_configmap (bool): If True, we will use the configmap to get the external
            exporter script. Otherwise, if False, we will get it from the CSV.

    Returns:
        str: path to the exporter script

    """
    logger.info("Generating external exporter script")
    encoded_script = get_exporter_script(use_configmap)
    # decode the exporter script and write to file
    external_script = decode(encoded_script)
    external_cluster_details_exporter = tempfile.NamedTemporaryFile(
        mode="w+",
        prefix="external-cluster-details-exporter-",
        suffix=".py",
        delete=False,
    )
    with open(external_cluster_details_exporter.name, "w") as fd:
        fd.write(external_script)
    logger.info(
        f"external cluster script is located at {external_cluster_details_exporter.name}"
    )

    return external_cluster_details_exporter.name


def _normalize_cephadm_certmgr_stdout(raw_stdout):
    text = (raw_stdout or "").strip()
    if "BEGIN CERTIFICATE" in text or "BEGIN TRUSTED CERTIFICATE" in text:
        return text
    if text.startswith("{"):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return text
        for key in ("certificate", "cert", "pem", "data", "ca_certificate"):
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return text


def _external_ceph_semantic_version_or_none():
    """
    Parse Ceph version from ``ceph --version`` on the default external SSH host.

    Uses ``cephadm shell`` so the command runs in the cephadm environment; if that
    fails (e.g. legacy non-cephadm layout), falls back to host ``ceph --version``.
    """
    try:
        ext = get_external_cluster_instance()
        cmd = "/usr/sbin/cephadm shell -- ceph --version"
        if ext.user != "root":
            cmd = f"sudo {cmd}"
        rc, out, _ = ext.rhcs_conn.exec_cmd(cmd)
        if rc != 0 or not (out or "").strip():
            rc, out, _ = ext.rhcs_conn.exec_cmd("ceph --version")
        if rc != 0:
            return None
        m = re.search(r"ceph\s+version\s+(\d+)\.(\d+)", out, re.I)
        if not m:
            return None
        return version.get_semantic_version(
            f"{m.group(1)}.{m.group(2)}", only_major_minor=True
        )
    except Exception as exc:
        logger.debug("Could not determine external Ceph version: %s", exc)
        return None


def external_rgw_ca_should_use_cephadm_fetch():
    """
    True when external Ceph reports version >= 19.0 (cephadm certmgr path), or if
    version is not determined (None) - assuming that from ODF 4.18 we are using Ceph >= 19.
    """
    v = _external_ceph_semantic_version_or_none()
    if v is None:
        return True
    return v >= version.get_semantic_version("19.0", only_major_minor=True)


def try_embed_rgw_ca_pem_in_mcg_cli_resources(service_ca_data, sts_dict):
    """
    If deploy stashed ``embedded_external_rgw_ca_pem``, add it to the service-ca
    ConfigMap and add a matching volumeMount on ``sts_dict`` (first container).

    Returns:
        bool: True if embedding was applied.
    """
    pem = config.EXTERNAL_MODE.get("embedded_external_rgw_ca_pem")
    if not pem:
        return False
    service_ca_data.setdefault("data", {})[constants.EXTERNAL_RGW_CA_CM_KEY] = pem
    sts_dict["spec"]["template"]["spec"]["containers"][0]["volumeMounts"].append(
        {
            "name": "service-ca",
            "mountPath": constants.EXTERNAL_RGW_CA_CONTAINER_PATH,
            "subPath": constants.EXTERNAL_RGW_CA_CM_KEY,
        }
    )
    return True


def get_and_apply_rgw_cert_ca(apply=True):
    """
    Obtain the RGW TLS CA: for external Ceph **19.0+**, fetch ``cephadm_root_ca_cert``
    from the ``_admin`` node via SSH; for Ceph **18.x (Reef)**, fetch the certificate
    directly from the RGW server; otherwise download from ``rgw_cert_ca`` URL.

    Args:
        apply (bool): if True, the certificate is applied as trusted CA by the OCP cluster

    Returns:
        str: path to the local RGW CA PEM file

    """
    rgw_cert_ca_path = tempfile.NamedTemporaryFile(
        mode="w+",
        prefix="rgw-cert-ca",
        suffix=".pem",
        delete=False,
    ).name
    config.EXTERNAL_MODE.pop("embedded_external_rgw_ca_pem", None)

    ceph_version = _external_ceph_semantic_version_or_none()

    # Ceph 19.0+ (Squid): Use cephadm certmgr
    if ceph_version and ceph_version >= version.get_semantic_version(
        "19.0", only_major_minor=True
    ):
        try:
            host, user, password, ssh_key = get_external_cluster_client("_admin")
            pem = ExternalCluster(
                host, user, password, ssh_key
            ).fetch_cephadm_root_ca_cert_pem()
            with open(rgw_cert_ca_path, "w", encoding="utf-8") as pem_fd:
                pem_fd.write(pem)
            config.EXTERNAL_MODE["embedded_external_rgw_ca_pem"] = pem
            logger.info(
                "Using cephadm_root_ca_cert from external cluster (Ceph >= 19.0)"
            )
        except Exception as exc:
            logger.warning(
                "cephadm CA fetch failed (%s); falling back to rgw_cert_ca URL", exc
            )
            download_file(
                config.EXTERNAL_MODE["rgw_cert_ca"],
                rgw_cert_ca_path,
            )

    # Ceph 18.x (Reef): Fetch directly from RGW server
    elif ceph_version and ceph_version >= version.get_semantic_version(
        "18.0", only_major_minor=True
    ):
        try:
            rgw_endpoint = get_rgw_endpoint()
            ext = get_external_cluster_instance()
            rgw_port = ext.get_rgw_endpoint_api_port()
            rgw_full_endpoint = f"{rgw_endpoint}:{rgw_port}"

            pem = ext.fetch_rgw_server_certificate(rgw_full_endpoint)
            with open(rgw_cert_ca_path, "w", encoding="utf-8") as pem_fd:
                pem_fd.write(pem)
            config.EXTERNAL_MODE["embedded_external_rgw_ca_pem"] = pem
            logger.info(
                f"Using server certificate from RGW endpoint {rgw_full_endpoint} (Ceph 18.x)"
            )
        except Exception as exc:
            logger.warning(
                "RGW server certificate fetch failed (%s); falling back to rgw_cert_ca URL",
                exc,
            )
            download_file(
                config.EXTERNAL_MODE["rgw_cert_ca"],
                rgw_cert_ca_path,
            )

    # Older versions or version detection failed: Use rgw_cert_ca URL
    else:
        if ceph_version is None:
            logger.warning(
                "Could not determine Ceph version, falling back to rgw_cert_ca URL"
            )
        else:
            logger.info(f"Ceph version {ceph_version} < 18.0, using rgw_cert_ca URL")
        download_file(
            config.EXTERNAL_MODE["rgw_cert_ca"],
            rgw_cert_ca_path,
        )

    # configure the CA cert to be trusted by the OCP cluster
    if apply:
        ssl_certs.configure_trusted_ca_bundle(ca_cert_path=rgw_cert_ca_path)
        wait_for_machineconfigpool_status("all", timeout=1800)
    return rgw_cert_ca_path


def get_rgw_endpoint():
    """
    Fetches rgw endpoint

    Returns:
        str: rgw endpoint

    Raises:
        ExternalClusterRGWEndPointMissing: in case of rgw endpoint missing

    """
    rgw_endpoint = None
    zone = config.ENV_DATA.get("zone")
    for each in config.EXTERNAL_MODE["external_cluster_node_roles"].values():
        if zone and f"zone-{zone}" not in each.get("location", {}).get(
            "datacenter", ""
        ):
            continue
        if "rgw" in each["role"]:
            if config.EXTERNAL_MODE.get("use_fqdn_rgw_endpoint"):
                logger.info("using FQDN as rgw endpoint")
                rgw_endpoint = each["hostname"]
            elif config.DEPLOYMENT.get("ipv6"):
                logger.info("using IPv6 as rgw endpoint")
                # This is a workaround for DFBUGS-5859 till 4.22 rgw must use hostname in ipv6
                rgw_endpoint = each["hostname"]
            else:
                logger.info("using IPv4 as rgw endpoint")
                rgw_endpoint = each["ip_address"]
            return rgw_endpoint
    if not rgw_endpoint:
        err_msg = "No RGW endpoint found"
        if zone:
            err_msg += f" in zone: {zone}"
        raise ExternalClusterRGWEndPointMissing(err_msg)


def get_external_cluster_client(role=None):
    """
    Resolve SSH target for an external RHCS node by role.

    Args:
        role (str or None): Node role to match in ``external_cluster_node_roles`` (e.g.
            ``client``, ``_admin``). If None, uses ``_admin`` when multicluster else ``client``.

    Returns:
        tuple: (ip_address, user, password, ssh_key)

    Raises:
        ExternalClusterCephSSHAuthDetailsMissing: In case one of SSH key or password
            is not provided.

    """
    user = config.EXTERNAL_MODE["login"]["username"]
    password = config.EXTERNAL_MODE["login"].get("password")
    ssh_key = config.EXTERNAL_MODE["login"].get("ssh_key")
    if not (password or ssh_key):
        raise ExternalClusterCephSSHAuthDetailsMissing(
            "No SSH Auth to connect to external RHCS cluster provided! "
            "Either password or SSH key is missing in EXTERNAL_MODE['login'] section!"
        )
    nodes = config.EXTERNAL_MODE["external_cluster_node_roles"]
    if role is None:
        role = "_admin" if config.multicluster else "client"

    try:
        return get_node_by_role(nodes, role, user, password, ssh_key)
    except ExternalClusterNodeRoleNotFound:
        logger.warning(f"No {role} role defined, using node1 address!")
        return (nodes["node1"]["ip_address"], user, password, ssh_key)


def get_external_cluster_instance() -> "ExternalCluster":
    """
    Create and return an ExternalCluster instance using credentials from config.

    Returns:
        ExternalCluster: Configured external cluster connection.

    Raises:
        ExternalClusterCephSSHAuthDetailsMissing: If credentials missing.

    """
    host, user, password, ssh_key = get_external_cluster_client()
    return ExternalCluster(host, user, password, ssh_key)


def get_node_by_role(nodes, role, user, password, ssh_key):
    """
    Get a node (a tuple with ip, user, password, ssh_key)

    Args:
        nodes (list): List of nodes participating
        role (str): Specific role of the nodes we are looking for
        user (str): Login user name for the node
        password (str): Login password for the node
        ssh_key (str): Path to ssh key

    Returns:
        Tuple: (ip_address, user, password, ssh_key)

    Raises:
        ExternalClusterNodeRoleNotFound (Exception): if no node found with the desired role
    """
    node_by_role = None
    for each in nodes.values():
        if role in each["role"]:
            node_by_role = (each["ip_address"], user, password, ssh_key)
            return node_by_role
    raise ExternalClusterNodeRoleNotFound


def remove_csi_users():
    """
    Remove csi users from external RHCS cluster
    """
    toolbox = pod.get_ceph_tools_pod(skip_creating_pod=True)
    toolbox.exec_cmd_on_pod("ceph auth del client.csi-cephfs-node")
    toolbox.exec_cmd_on_pod("ceph auth del client.csi-cephfs-provisioner")
    toolbox.exec_cmd_on_pod("ceph auth del client.csi-rbd-node")
    toolbox.exec_cmd_on_pod("ceph auth del client.csi-rbd-provisioner")
