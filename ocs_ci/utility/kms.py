"""
This module contains KMS related class and methods
currently supported KMSs: Vault

"""
import logging
import os

import json
import shlex
import tempfile
import subprocess
from subprocess import CalledProcessError
import base64

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import (
    VaultDeploymentError,
    VaultOperationError,
    KMSNotSupported,
    KMSResourceCleaneupError,
    CommandFailed,
    NotFoundError,
    KMSConnectionDetailsError,
)
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    load_auth_config,
    run_cmd,
    get_vault_cli,
    get_running_cluster_id,
    get_default_if_keyval_empty,
    get_cluster_name,
)


logger = logging.getLogger(__name__)


class KMS(object):
    """
    This is base class for any KMS integration

    """

    def __init__(self, provider=None):
        self.kms_provider = provider

    def deploy(self):
        raise NotImplementedError("Child class should implement this method")

    def post_deploy_verification(self):
        raise NotImplementedError("Child class should implement this method")

    def create_csi_kms_resources(self):
        raise NotImplementedError("Child class should implement this method")


class Vault(KMS):
    """
    A class which handles deployment and other
    configs related to vault

    """

    def __init__(self):
        super().__init__("vault")
        self.vault_server = None
        self.port = None
        self.cluster_id = None
        # Name of kubernetes resources
        # for ca_cert, client_cert, client_key
        self.ca_cert_name = None
        self.client_cert_name = None
        self.client_key_name = None
        self.vault_root_token = None
        self.vault_namespace = None
        self.vault_deploy_mode = config.ENV_DATA.get("vault_deploy_mode")
        self.vault_backend_path = None
        self.vault_backend_version = config.ENV_DATA.get(
            "VAULT_BACKEND", defaults.VAULT_DEFAULT_BACKEND_VERSION
        )
        # Base64 encoded (with padding) token
        self.vault_path_token = None
        self.vault_policy_name = None

    def deploy(self):
        """
        This function delegates the deployment of vault
        based on OCP or vault standalone external mode deployment

        """
        if self.vault_deploy_mode == "external":
            self.deploy_vault_external()
        elif self.vault_deploy_mode == "internal":
            self.deploy_vault_internal()
        else:
            raise VaultDeploymentError("Not a supported vault deployment mode")

    def deploy_vault_external(self):
        """
        This function takes care of deployment and configuration
        for external mode vault deployment. We are assuming that
        an external vault service already exists and we will be just
        configuring the necessary OCP objects for OCS like secrets, token etc

        """
        self.gather_init_vault_conf()
        # Update env vars for vault CLI usage
        self.update_vault_env_vars()
        get_vault_cli()
        self.vault_unseal()
        if config.ENV_DATA.get("use_vault_namespace"):
            self.vault_create_namespace()
        self.vault_create_backend_path()
        self.create_ocs_vault_resources()

    def gather_init_vault_conf(self):
        """
        Gather vault configuration and init the vars
        This function currently gathers only for external mode
        Gathering for internal mode woulde be different

        """
        self.vault_conf = self.gather_vault_config()
        self.vault_server = self.vault_conf["VAULT_ADDR"]
        self.port = self.vault_conf["PORT"]
        if not config.ENV_DATA.get("VAULT_SKIP_VERIFY"):
            self.ca_cert_base64 = self.vault_conf["VAULT_CACERT_BASE64"]
            self.client_cert_base64 = self.vault_conf["VAULT_CLIENT_CERT_BASE64"]
            self.client_key_base64 = self.vault_conf["VAULT_CLIENT_KEY_BASE64"]
            self.vault_tls_server = self.vault_conf["VAULT_TLS_SERVER_NAME"]
        self.vault_root_token = self.vault_conf["VAULT_ROOT_TOKEN"]

    def vault_create_namespace(self, namespace=None):
        """
        Create a vault namespace if it doesn't exists

        """

        if namespace:
            self.vault_namespace = namespace
        else:
            self.vault_namespace = get_default_if_keyval_empty(
                config.ENV_DATA, "VAULT_NAMESPACE", constants.VAULT_DEFAULT_NAMESPACE
            )
            if self.vault_namespace == "":
                self.vault_namespace = (
                    f"{constants.VAULT_DEFAULT_NAMESPACE_PREFIX}-"
                    f"{get_running_cluster_id()}-"
                    f"{get_cluster_name(config.ENV_DATA['cluster_path'])}"
                )

        if not self.vault_namespace_exists(self.vault_namespace):
            self.create_namespace(self.vault_namespace)
        os.environ["VAULT_NAMESPACE"] = self.vault_namespace

    def vault_namespace_exists(self, vault_namespace):
        """
        Check if vault namespace already exists

        Args:
            vault_namespace (str): name of the vault namespace

        Returns:
            bool: True if exists else False

        """
        cmd = f"vault namespace lookup {vault_namespace}"
        proc = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        out, err = proc.communicate()
        if proc.returncode:
            if "Namespace not found" in err:
                return False
        return True

    def vault_backend_path_exists(self, backend_path):
        """
        Check if vault backend path already exists

        Args:
            backend_path (str): name of the vault backend path

        Returns:
            bool: True if exists else False

        """
        cmd = f"vault secrets list | grep {backend_path}"
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        out, err = proc.communicate()
        if proc.returncode:
            return False
        return True

    def create_namespace(self, vault_namespace):
        """
        Create a vault namespace

        Args:
            vault_namespace (str): name of the vault namespace

        Raises:
            VaultOperationError: If namespace is not created successfully

        """
        cmd = f"vault namespace create {vault_namespace}"
        proc = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if proc.returncode:
            raise VaultOperationError("Namespace creation failed", err)
        # Check if namespace gets listed
        if self.vault_namespace_exists(vault_namespace):
            logger.info(f"Namespace {vault_namespace} successfully created")
        else:
            logger.error(f"Namespace {vault_namespace} not found in the list")
            raise VaultOperationError()

    def update_vault_env_vars(self):
        """
        In order to run vault CLI we need following env vars
        VAULT_ADDR and VAULT_TOKEN

        """
        os.environ["VAULT_ADDR"] = f"https://{self.vault_server}:{self.port}"
        os.environ["VAULT_TOKEN"] = self.vault_root_token
        os.environ["VAULT_FORMAT"] = "json"
        # setup client crt so that vault cli works smoothly
        # if 'VAULT_SKIP_VERIFY' is True then no need to do
        # this call as vault would have configured for http
        if (
            not config.ENV_DATA.get("VAULT_SKIP_VERIFY")
            and config.ENV_DATA.get("vault_deploy_mode") == "external"
        ):
            self.setup_vault_client_cert()
            os.environ["VAULT_CACERT"] = constants.VAULT_CLIENT_CERT_PATH

    def setup_vault_client_cert(self):
        """
        For Vault cli interaction with the server we need client cert
        to talk to HTTPS on the vault server

        """
        cert_str = base64.b64decode(self.client_cert_base64).decode()
        with open(constants.VAULT_CLIENT_CERT_PATH, "w") as cert:
            cert.write(cert_str)
            logger.info(f"Created cert file at {constants.VAULT_CLIENT_CERT_PATH}")

    def create_ocs_vault_cert_resources(self):
        """
        Explicitly create secrets like ca cert, client cert, client key
        Assumption is vault section in AUTH file contains base64 encoded
        (with padding) ca, client certs, client key

        """

        # create ca cert secret
        ca_data = templating.load_yaml(constants.EXTERNAL_VAULT_CA_CERT)
        self.ca_cert_name = get_default_if_keyval_empty(
            config.ENV_DATA, "VAULT_CACERT", defaults.VAULT_DEFAULT_CA_CERT
        )
        ca_data["metadata"]["name"] = self.ca_cert_name
        ca_data["data"]["cert"] = self.ca_cert_base64
        self.create_resource(ca_data, prefix="ca")

        # create client cert secret
        client_cert_data = templating.load_yaml(constants.EXTERNAL_VAULT_CLIENT_CERT)
        self.client_cert_name = get_default_if_keyval_empty(
            config.ENV_DATA, "VAULT_CLIENT_CERT", defaults.VAULT_DEFAULT_CLIENT_CERT
        )
        client_cert_data["metadata"]["name"] = self.client_cert_name
        client_cert_data["data"]["cert"] = self.client_cert_base64
        self.create_resource(client_cert_data, prefix="clientcert")

        # create client key secert
        client_key_data = templating.load_yaml(constants.EXTERNAL_VAULT_CLIENT_KEY)
        self.client_key_name = get_default_if_keyval_empty(
            config.ENV_DATA, "VAULT_CLIENT_KEY", defaults.VAULT_DEFAULT_CLIENT_KEY
        )
        client_key_data["metadata"]["name"] = self.client_key_name
        client_key_data["data"]["key"] = self.client_key_base64
        self.create_resource(client_key_data, prefix="clientkey")

    def create_ocs_vault_resources(self):
        """
        This function takes care of creating ocp resources for
        secrets like ca cert, client cert, client key and vault token
        Assumption is vault section in AUTH file contains base64 encoded
        (with padding) ca, client certs, client key and vault path token

        """
        if not config.ENV_DATA.get("VAULT_SKIP_VERIFY"):
            self.create_ocs_vault_cert_resources()

        # create oc resource secret for token
        token_data = templating.load_yaml(constants.EXTERNAL_VAULT_KMS_TOKEN)
        # token has to base64 encoded (with padding)
        token_data["data"]["token"] = base64.b64encode(
            # encode() because b64encode expects a byte type
            self.vault_path_token.encode()
        ).decode()  # decode() because b64encode returns a byte type
        self.create_resource(token_data, prefix="token")

        # create ocs-kms-connection-details
        connection_data = templating.load_yaml(
            constants.EXTERNAL_VAULT_KMS_CONNECTION_DETAILS
        )
        connection_data["data"]["VAULT_ADDR"] = os.environ["VAULT_ADDR"]
        connection_data["data"]["VAULT_BACKEND_PATH"] = self.vault_backend_path
        connection_data["data"]["VAULT_CACERT"] = self.ca_cert_name
        connection_data["data"]["VAULT_CLIENT_CERT"] = self.client_cert_name
        connection_data["data"]["VAULT_CLIENT_KEY"] = self.client_key_name
        connection_data["data"]["VAULT_NAMESPACE"] = self.vault_namespace
        connection_data["data"]["VAULT_TLS_SERVER_NAME"] = self.vault_tls_server
        connection_data["data"]["VAULT_BACKEND"] = self.vault_backend_version
        self.create_resource(connection_data, prefix="kmsconnection")

    def create_resource(self, resource_data, prefix=None):
        """
        Given a dictionary of resource data, this function will
        creates oc resource

        Args:
            resource_data (dict): yaml dictionary for resource
            prefix (str): prefix for NamedTemporaryFile

        """
        resource_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix=prefix, delete=False
        )
        templating.dump_data_to_temp_yaml(resource_data, resource_data_yaml.name)
        run_cmd(f"oc create -f {resource_data_yaml.name}", timeout=300)

    def vault_unseal(self):
        """
        Unseal vault if sealed

        Raises:
            VaultOperationError: In case unseal operation failed

        """
        if self.vault_sealed():
            logger.info("Vault is sealed, Unsealing now..")
            for i in range(3):
                kkey = f"UNSEAL_KEY{i+1}"
                self._vault_unseal(self.vault_conf[kkey])
            # Check if vault is unsealed or not
            if self.vault_sealed():
                raise VaultOperationError("Failed to Unseal vault")
            else:
                logger.info("Vault has been successfully unsealed")
        else:
            logger.info("Vault is not sealed")

    def _vault_unseal(self, key):
        """
        Execute unseal command here

        Args:
            key (str): unseal key

        """
        unseal_cmd = f"vault operator unseal {key}"
        subprocess.check_output(shlex.split(unseal_cmd))

    def vault_sealed(self):
        """
        Returns:
            bool: if vault is sealed then return True else False

        """
        status_cmd = "vault status --format=json"
        output = subprocess.check_output(shlex.split(status_cmd))
        outbuf = json.loads(output)
        return outbuf["sealed"]

    def vault_create_backend_path(self, backend_path=None):
        """
        create vault path to be used by OCS

        Raises:
            VaultOperationError exception
        """
        if backend_path:
            self.vault_backend_path = backend_path
        else:
            if config.ENV_DATA.get("VAULT_BACKEND_PATH"):
                self.vault_backend_path = config.ENV_DATA.get("VAULT_BACKEND_PATH")
            else:
                # Generate backend path name using prefix "ocs"
                # "ocs-<cluster-id>"
                self.cluster_id = get_running_cluster_id()
                self.vault_backend_path = (
                    f"{constants.VAULT_DEFAULT_PATH_PREFIX}-{self.cluster_id}-"
                    f"{get_cluster_name(config.ENV_DATA['cluster_path'])}"
                )
        if self.vault_backend_path_exists(self.vault_backend_path):
            logger.info(f"vault path {self.vault_backend_path} already exists")

        else:
            cmd = (
                f"vault secrets enable -path={self.vault_backend_path} "
                f"kv-{self.vault_backend_version}"
            )
            out = subprocess.check_output(shlex.split(cmd))
            if "Success" in out.decode():
                logger.info(f"vault path {self.vault_backend_path} created")
            else:
                raise VaultOperationError(
                    f"Failed to create path f{self.vault_backend_path}"
                )
        if not backend_path:
            self.vault_create_policy()

    def vault_create_policy(self, policy_name=None):
        """
        Create a vault policy and generate token

        Raises:
            VaultOperationError exception

        """
        policy = (
            f'path "{self.vault_backend_path}/*" {{\n'
            f'  capabilities = ["create", "read", "update","delete"]'
            f"\n}}\n"
            f'path "sys/mounts" {{\n'
            f'capabilities = ["read"]\n'
            f"}}"
        )
        vault_hcl = tempfile.NamedTemporaryFile(mode="w+", prefix="test", delete=False)
        with open(vault_hcl.name, "w") as hcl:
            hcl.write(policy)

        if policy_name:
            self.vault_policy_name = policy_name
        else:
            if not get_default_if_keyval_empty(config.ENV_DATA, "VAULT_POLICY", None):
                self.vault_policy_name = (
                    f"{constants.VAULT_DEFAULT_POLICY_PREFIX}-"
                    f"{self.cluster_id}-"
                    f"{get_cluster_name(config.ENV_DATA['cluster_path'])}"
                )
            else:
                self.vault_policy_name = config.ENV_DATA.get("VAULT_POLICY")

        cmd = f"vault policy write {self.vault_policy_name} {vault_hcl.name}"
        out = subprocess.check_output(shlex.split(cmd))
        if "Success" in out.decode():
            logger.info(f"vault policy {self.vault_policy_name} created")
        else:
            raise VaultOperationError(
                f"Failed to create policy f{self.vault_policy_name}"
            )
        self.vault_path_token = self.generate_vault_token()

    def generate_vault_token(self):
        """
        Generate a token for self.vault_policy_name

        Returns:
            str: vault token

        """
        cmd = f"vault token create -policy={self.vault_policy_name} " f"--format=json"
        out = subprocess.check_output(shlex.split(cmd))
        json_out = json.loads(out)
        return json_out["auth"]["client_token"]

    def deploy_vault_internal(self):
        """
        This function takes care of deployment and configuration for
        internal mode vault deployment on OCP

        """
        pass

    def gather_vault_config(self):
        """
        This function populates the vault configuration

        """
        if self.vault_deploy_mode == "external":
            vault_conf = load_auth_config()["vault"]
            return vault_conf

    def get_vault_connection_info(self, resource_name=None):
        """
        Get resource info from ocs-kms-connection-defatils

        Args:
            resource_name (str): name of the resource

        """
        connection_details = ocp.OCP(
            kind="ConfigMap",
            resource_name=constants.VAULT_KMS_CONNECTION_DETAILS_RESOURCE,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        return connection_details.get().get("data")[resource_name]

    def get_vault_backend_path(self):
        """
        Fetch the vault backend path used for this deployment
        This can be obtained from kubernetes secret resource
        'ocs-kms-connection-details'

        .. code-block:: none

            apiVersion: v1
            data:
              KMS_PROVIDER: vault
              KMS_SERVICE_NAME: vault
              VAULT_ADDR: https://xx.xx.xx.xx:8200
              VAULT_BACKEND_PATH: ocs

        """
        if not self.vault_backend_path:
            self.vault_backend_path = self.get_vault_connection_info(
                "VAULT_BACKEND_PATH"
            )
            logger.info(f"setting vault_backend_path = {self.vault_backend_path}")

    def get_vault_path_token(self):
        """
        Fetch token from kubernetes secret
        we need this to find the vault policy
        default name in case of ocs is 'ocs-kms-token'

        .. code-block:: none

            apiVersion: v1
            data:
              token: cy5DRXBKV0lVbzNFQjM1VHlGMFNURzZQWms=
            kind: Secret
            metadata:
              name: ocs-kms-token
            namespace: openshift-storage
            type: Opaque

        """
        if not self.vault_path_token:
            vault_token = ocp.OCP(
                kind="Secret",
                resource_name=constants.VAULT_KMS_TOKEN_RESOURCE,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            token = vault_token.get().get("data")["token"]
            self.vault_path_token = base64.b64decode(token).decode()
            logger.info(f"Setting vault_path_token = {self.vault_path_token}")

    def get_vault_policy(self):
        """
        Get the policy name based on token from vault

        """
        self.vault_policy_name = config.ENV_DATA.get("VAULT_POLICY", None)
        if not self.vault_policy_name:
            cmd = f"vault token lookup {self.vault_path_token}"
            out = subprocess.check_output(shlex.split(cmd))
            json_out = json.loads(out)
            logger.info(json_out)
            for policy in json_out["data"]["policies"]:
                if self.cluster_id in policy:
                    self.vault_policy_name = policy
                    logger.info(f"setting vault_policy_name = {self.vault_policy_name}")

    def remove_vault_backend_path(self):
        """
        remove vault path

        """
        cmd = f"vault secrets disable {self.vault_backend_path}"
        subprocess.check_output(shlex.split(cmd))
        # Check if path doesn't appear in the list
        cmd = "vault secrets list --format=json"
        out = subprocess.check_output(shlex.split(cmd))
        json_out = json.loads(out)
        for path in json_out.keys():
            if self.vault_backend_path in path:
                raise KMSResourceCleaneupError(
                    f"Path {self.vault_backend_path} not deleted"
                )
        logger.info(f"Vault path {self.vault_backend_path} deleted")

    def remove_vault_policy(self):
        """
        Cleanup the policy we used

        """
        cmd = f"vault policy delete {self.vault_policy_name} "
        subprocess.check_output(shlex.split(cmd))
        # Check if policy still exists
        cmd = "vault policy list --format=json"
        out = subprocess.check_output(shlex.split(cmd))
        json_out = json.loads(out)
        if self.vault_policy_name in json_out:
            raise KMSResourceCleaneupError(
                f"Policy {self.vault_policy_name} not deleted"
            )
        logger.info(f"Vault policy {self.vault_policy_name} deleted")

    def remove_vault_namespace(self):
        """
        Cleanup the namespace

        Raises:
            KMSResourceCleanupError: If namespace deletion fails

        """
        # Unset namespace from environment
        # else delete will look for namespace within namespace
        os.environ.pop("VAULT_NAMESPACE")
        cmd = f"vault namespace delete {self.vault_namespace}/"
        subprocess.check_output(shlex.split(cmd))
        if self.vault_namespace_exists(self.vault_namespace):
            raise KMSResourceCleaneupError(
                f"Namespace {self.vault_namespace} deletion failed"
            )
        logger.info(f"Vault namespace {self.vault_namespace} deleted")

    def get_vault_namespace(self):
        """
        From kms connection details resource obtain
        namespace

        """
        if not self.vault_namespace:
            self.vault_namespace = self.get_vault_connection_info("VAULT_NAMESPACE")
            logger.info(f"Setting vault_namespace={self.vault_namespace}")

    def cleanup(self):
        """
        Cleanup the backend resources in case of external

        """
        if not self.vault_server:
            self.gather_init_vault_conf()

        self.update_vault_env_vars()
        try:
            # We need to set vault namespace in the env
            # so that path, policy and token are accessed
            # within the namespace context
            if config.ENV_DATA.get("use_vault_namespace"):
                self.get_vault_namespace()
                os.environ["VAULT_NAMESPACE"] = self.vault_namespace
            # get vault path
            self.get_vault_backend_path()
            # from token secret get token
            self.get_vault_path_token()
            # from token get policy
            if not self.cluster_id:
                self.cluster_id = get_running_cluster_id()
            self.get_vault_policy()
        except (CommandFailed, IndexError):
            logger.error(
                "Error occured during kms resource info gathering,"
                "skipping vault cleanup"
            )
            return

        # Delete the policy and backend path from vault
        # we need root token of vault in the env
        self.remove_vault_backend_path()
        self.remove_vault_policy()
        if self.vault_namespace:
            self.remove_vault_namespace()

    def post_deploy_verification(self):
        """
        Validating the OCS deployment from vault perspective

        """
        if config.ENV_DATA.get("vault_deploy_mode") == "external":
            self.validate_external_vault()

    def validate_external_vault(self):
        """
        This function is for post OCS deployment vault
        verification

        Following checks will be done
        1. check osd encryption keys in the vault path
        2. check noobaa keys in the vault path
        3. check storagecluster CR for 'kms' enabled

        Raises:
            NotFoundError : if key not found in vault OR in the resource CR

        """

        self.gather_init_vault_conf()
        self.update_vault_env_vars()
        if config.ENV_DATA.get("use_vault_namespace"):
            self.get_vault_namespace()
            os.environ["VAULT_NAMESPACE"] = self.vault_namespace
        self.get_vault_backend_path()
        kvlist = vault_kv_list(self.vault_backend_path)

        # Check osd keys are present
        osds = pod.get_osd_pods()
        for osd in osds:
            pvc = (
                osd.get()
                .get("metadata")
                .get("labels")
                .get(constants.CEPH_ROOK_IO_PVC_LABEL)
            )
            if any(pvc in k for k in kvlist):
                logger.info(f"Vault: Found key for {pvc}")
            else:
                logger.error(f"Vault: Key not found for {pvc}")
                raise NotFoundError("Vault key not found")

        # Check for NOOBAA key
        if any(constants.VAULT_NOOBAA_ROOT_SECRET_PATH in k for k in kvlist):
            logger.info("Found Noobaa root secret path")
        else:
            logger.error("Noobaa root secret path not found")
            raise NotFoundError("Vault key for noobaa not found")

        # Check kms enabled
        if not is_kms_enabled():
            logger.error("KMS not enabled on storage cluster")
            raise NotFoundError("KMS flag not found")

    def create_vault_csi_kms_token(
        self, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    ):
        """
        create vault specific csi kms secret resource

        """
        csi_kms_token = templating.load_yaml(constants.EXTERNAL_VAULT_CSI_KMS_TOKEN)
        csi_kms_token["data"]["token"] = base64.b64encode(
            self.vault_path_token.encode()
        ).decode()
        csi_kms_token["metadata"]["namespace"] = namespace
        self.create_resource(csi_kms_token, prefix="csikmstoken")

    def create_vault_csi_kms_connection_details(
        self, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    ):
        """
        Create vault specific csi kms connection details
        configmap resource

        """
        csi_kms_conn_details = templating.load_yaml(
            constants.EXTERNAL_VAULT_CSI_KMS_CONNECTION_DETAILS
        )
        conn_str = csi_kms_conn_details["data"]["1-vault"]
        buf = json.loads(conn_str)
        buf["VAULT_ADDR"] = f"https://{self.vault_server}:{self.port}"
        buf["VAULT_BACKEND_PATH"] = self.vault_backend_path
        buf["VAULT_CACERT"] = get_default_if_keyval_empty(
            config.ENV_DATA, "VAULT_CACERT", defaults.VAULT_DEFAULT_CA_CERT
        )
        buf["VAULT_NAMESPACE"] = self.vault_namespace
        buf["VAULT_TOKEN_NAME"] = get_default_if_keyval_empty(
            config.ENV_DATA, "VAULT_TOKEN_NAME", constants.EXTERNAL_VAULT_CSI_KMS_TOKEN
        )
        csi_kms_conn_details["data"]["1-vault"] = json.dumps(buf)
        csi_kms_conn_details["metadata"]["namespace"] = namespace
        self.create_resource(csi_kms_conn_details, prefix="csikmsconn")


kms_map = {"vault": Vault}


def update_csi_kms_vault_connection_details(update_config):
    """
    Update the vault connection details in the resource
    csi-kms-connection-details

    Args:
         update_config (dict): A dictionary of vault info to be updated

    """
    # Check if csi-kms-connection-details resource already exists
    # if not we might need to rise an exception because without
    # csi-kms-connection details  we can't proceed with update
    csi_kms_conf = ocp.OCP(
        resource_name=constants.VAULT_KMS_CSI_CONNECTION_DETAILS,
        kind="ConfigMap",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )

    try:
        csi_kms_conf.get()
    except CommandFailed:
        raise KMSConnectionDetailsError(
            "CSI KMS connection details don't exist, can't continue with update"
        )
    if csi_kms_conf.data.get("metadata").get("annotations"):
        csi_kms_conf.data["metadata"].pop("annotations")
    for key in update_config.keys():
        csi_kms_conf.data["data"].update({key: json.dumps(update_config[key])})
    resource_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="csikmsconndetailsupdate", delete=False
    )
    templating.dump_data_to_temp_yaml(csi_kms_conf.data, resource_data_yaml.name)
    run_cmd(f"oc apply -f {resource_data_yaml.name}", timeout=300)


def get_kms_deployment():
    provider = config.ENV_DATA["KMS_PROVIDER"]
    try:
        return kms_map[provider]()
    except KeyError:
        raise KMSNotSupported("Not a supported KMS deployment")


def is_kms_enabled():
    """
    Checks StorageCluster yaml if kms is configured.

    Return:
        (bool): True if KMS is configured else False

    """
    cluster = storage_cluster.get_storage_cluster()
    logger.info("Checking if StorageCluster has configured KMS encryption")
    resource = cluster.get()["items"][0]
    encryption = resource.get("spec").get("encryption", {}).get("kms", {}).get("enable")
    return bool(encryption)


def vault_kv_list(path):
    """
    List kv from a given path

    Args:
        path (str): Vault backend path name

    Returns:
        list: of kv present in the path

    """
    cmd = f"vault kv list {path}"
    out = subprocess.check_output(shlex.split(cmd))
    json_out = json.loads(out)
    return json_out


def is_key_present_in_path(key, path):
    """
    Check if key is present in the backend Path

    Args:
        key (str): Name of the key
        path (str): Vault backend path name

    Returns:
        (bool): True if key is present in the backend path
    """
    try:
        kvlist = vault_kv_list(path=path)
    except CalledProcessError:
        return False
    if any(key in k for k in kvlist):
        return True
    else:
        return False


def get_encryption_kmsid():
    """
    Get encryption kmsid from 'csi-kms-connection-details'
    configmap resource

    Returns:
        kmsid (list): A list of KMS IDs available

    Raises:
        KMSConnectionDetailsError: if csi kms connection detail doesn't exist

    """

    kmsid = []
    csi_kms_conf = ocp.OCP(
        resource_name=constants.VAULT_KMS_CSI_CONNECTION_DETAILS,
        kind="ConfigMap",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )
    try:
        csi_kms_conf.get()
    except CommandFailed:
        raise KMSConnectionDetailsError("CSI kms resource doesn't exist")

    for key in csi_kms_conf.get().get("data").keys():
        if constants.VAULT_KMS_PROVIDER in key:
            kmsid.append(key)
    return kmsid


def remove_kmsid(kmsid):
    """
    This function will remove all the details for the given kmsid from the csi-kms-connection-details configmap

    Args:
        kmsid (str) : kmsid to be remove_kmsid

    Raises:
        KMSResourceCleaneupError: If the kmsid entry is not deleted

    """
    ocp_obj = ocp.OCP()
    patch = f'\'[{{"op": "remove", "path": "/data/{kmsid}"}}]\''
    patch_cmd = (
        f"patch -n {constants.OPENSHIFT_STORAGE_NAMESPACE} cm"
        f"{constants.VAULT_KMS_CSI_CONNECTION_DETAILS} --type json -p " + patch
    )
    ocp_obj.exec_oc_cmd(command=patch_cmd)
    kmsid_list = get_encryption_kmsid()
    if any(kmsid in k for k in kmsid_list):
        raise KMSResourceCleaneupError(f"KMS ID {kmsid} deletion failed")
    logger.info(f"KMS ID {kmsid} deleted")


def update_csi_kms_vault_connection_details(update_config):
    """
    Update the vault connection details in the resource
    csi-kms-connection-details
    Args:
         update_config (dict): A dictionary of vault info to be updated
    """
    # Check if csi-kms-connection-details resource already exists
    # if not we might need to rise an exception because without
    # csi-kms-connection details  we can't proceed with update
    csi_kms_conf = ocp.OCP(
        resource_name=constants.VAULT_KMS_CSI_CONNECTION_DETAILS,
        kind="ConfigMap",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )

    try:
        csi_kms_conf.get()
    except CommandFailed:
        raise KMSConnectionDetailsError(
            "CSI kms connection details doesn't exists" "can't continue with update"
        )
    if csi_kms_conf.data.get("metadata").get("annotations"):
        csi_kms_conf.data["metadata"].pop("annotations")
    csi_kms_conf.data["data"].update(update_config)
    resource_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="csikmsconndetailsupdate", delete=False
    )
    templating.dump_data_to_temp_yaml(csi_kms_conf.data, resource_data_yaml.name)
    run_cmd(f"oc apply -f {resource_data_yaml.name}", timeout=300)
