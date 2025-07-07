"""
This module contains helpers functions needed for
external cluster deployment.
"""

import json
import logging
import re
import tempfile
import uuid

from ocs_ci.framework import config
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import (
    ExternalClusterCephfsMissing,
    ExternalClusterCephSSHAuthDetailsMissing,
    ExternalClusterExporterRunFailed,
    ExternalClusterRBDNamespaceCreationFailed,
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
    wait_for_machineconfigpool_status,
    create_config_ini_file,
)

logger = logging.getLogger(__name__)


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
        self.rhcs_conn = Connection(
            host=self.host,
            user=self.user,
            password=self.password,
            private_key=self.ssh_key,
        )

    def get_external_cluster_details(self):
        """
        Gets the external RHCS cluster details and updates to config.EXTERNAL_MODE

        Raises:
            ExternalClusterExporterRunFailed: If exporter script failed to run on external RHCS cluster

        """
        # get rgw endpoint port
        rgw_endpoint_port = self.get_rgw_endpoint_api_port()

        # get rgw endpoint
        rgw_endpoint = get_rgw_endpoint()
        rgw_endpoint_with_port = f"{rgw_endpoint}:{rgw_endpoint_port}"

        # get ceph filesystem
        ceph_fs_name = config.ENV_DATA.get("cephfs_name") or self.get_ceph_fs()

        rbd_name = config.ENV_DATA.get("rbd_name") or defaults.RBD_NAME
        cluster_name = config.ENV_DATA.get("cluster_name") or defaults.RHCS_CLUSTER_NAME

        params = (
            f"--rbd-data-pool-name {rbd_name} --rgw-endpoint {rgw_endpoint_with_port}"
        )

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
            config.ENV_DATA["restricted-auth-permission"] = True
            config.ENV_DATA["alias_rbd_name"] = alias_rbd_name

        if config.ENV_DATA.get("rgw-realm"):
            rgw_realm = config.ENV_DATA["rgw-realm"]
            rgw_zonegroup = config.ENV_DATA["rgw-zonegroup"]
            rgw_zone = config.ENV_DATA["rgw-zone"]
            params = (
                f"{params} --rgw-realm-name {rgw_realm} --rgw-zonegroup-name {rgw_zonegroup} "
                f"--rgw-zone-name {rgw_zone}"
            )

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

        if config.EXTERNAL_MODE.get("run_as_user"):
            ceph_user = config.EXTERNAL_MODE["run_as_user"]
            params = f"{params} --run-as-user {ceph_user}"

        if config.EXTERNAL_MODE.get("use_rbd_namespace"):
            rbd_namespace = config.EXTERNAL_MODE.get("rbd_namespace")
            if not rbd_namespace:
                rbd_namespace = self.create_rbd_namespace(rbd=rbd_name)
                config.EXTERNAL_MODE["rbd_namespace"] = rbd_namespace

            params = f"{params} --rados-namespace {rbd_namespace}"
            if "restricted-auth-permission" not in params:
                params += " --restricted-auth-permission true"
                config.ENV_DATA["restricted-auth-permission"] = True
            if "cluster-name" not in params:
                params += f" --k8s-cluster-name {cluster_name}"

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
        upload_file(
            self.host, script_path, script_path, self.user, self.password, self.ssh_key
        )
        return script_path

    def upload_rgw_cert_ca(self):
        """
        Upload RGW Cert CA to RHCS cluster

        Returns:
            str: absolute path to the CA Cert

        """
        rgw_cert_ca_path = get_and_apply_rgw_cert_ca()
        remote_rgw_cert_ca_path = "/tmp/rgw-cert-ca.pem"
        upload_file(
            self.host,
            rgw_cert_ca_path,
            remote_rgw_cert_ca_path,
            self.user,
            self.password,
            self.ssh_key,
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
            self.host, script_path, script_path, self.user, self.password, self.ssh_key
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
            str: RBD Namepsace name

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

    Returns:
        str: The exporter script from the csv

    """
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
            if ocs_version >= version.VERSION_4_16:
                exporter_script = each_csv["currentCSVDesc"]["annotations"][
                    "externalClusterScript"
                ]
            else:
                exporter_script = each_csv["currentCSVDesc"]["annotations"][
                    "external.features.ocs.openshift.io/export-script"
                ]
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


def get_and_apply_rgw_cert_ca():
    """
    Downloads CA Certificate of RGW if SSL is used and apply it to be trusted
    by the OCP cluster

    Returns:
        str: path to the downloaded RGW Cert CA

    """
    rgw_cert_ca_path = tempfile.NamedTemporaryFile(
        mode="w+",
        prefix="rgw-cert-ca",
        suffix=".pem",
        delete=False,
    ).name
    download_file(
        config.EXTERNAL_MODE["rgw_cert_ca"],
        rgw_cert_ca_path,
    )
    # configure the CA cert to be trusted by the OCP cluster
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
            elif config.EXTERNAL_MODE.get("use_ipv6_rgw_endpoint"):
                logger.info("using IPv6 as rgw endpoint")
                rgw_endpoint = each["ipv6_address"]
            else:
                logger.info("using IPv4 as rgw endpoint")
                rgw_endpoint = each["ip_address"]
            return rgw_endpoint
    if not rgw_endpoint:
        err_msg = "No RGW endpoint found"
        if zone:
            err_msg += f" in zone: {zone}"
        raise ExternalClusterRGWEndPointMissing(err_msg)


def get_external_cluster_client():
    """
    Finding the client role node IP address.

    Returns:
        tuple: IP address, user, password of the client, ssh key

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
    node_role = None
    node_role = "_admin" if config.multicluster else "client"

    try:
        return get_node_by_role(nodes, node_role, user, password, ssh_key)
    except ExternalClusterNodeRoleNotFound:
        logger.warning(f"No {node_role} role defined, using node1 address!")
        return (nodes["node1"]["ip_address"], user, password, ssh_key)


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
