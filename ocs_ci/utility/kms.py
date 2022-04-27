"""
This module contains KMS related class and methods
currently supported KMSs: Vault and HPCS

"""
import logging
import os

import requests
import json
import shlex
import tempfile
import subprocess
from subprocess import CalledProcessError
from semantic_version import Version
import base64

from ocs_ci.framework import config, merge_dict
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
    HPCSDeploymentError,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    load_auth_config,
    run_cmd,
    get_vault_cli,
    get_running_cluster_id,
    get_default_if_keyval_empty,
    get_cluster_name,
    encode,
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
        self.vault_auth_method = constants.VAULT_TOKEN_AUTH
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
        self.kmsid = None
        # Base64 encoded (with padding) token
        self.vault_path_token = None
        self.vault_policy_name = None
        self.vault_kube_auth_path = "kubernetes"
        self.vault_kube_auth_role = constants.VAULT_KUBERNETES_AUTH_ROLE
        self.vault_kube_auth_namespace = None
        self.vault_cwd_kms_sa_name = constants.VAULT_CWD_KMS_SA_NAME

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
        if config.ENV_DATA.get("use_auth_path"):
            self.cluster_id = get_running_cluster_id()
            self.vault_kube_auth_path = (
                f"{constants.VAULT_DEFAULT_PATH_PREFIX}-{self.cluster_id}-"
                f"{get_cluster_name(config.ENV_DATA['cluster_path'])}"
            )
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
            if not config.ENV_DATA.get("VAULT_CA_ONLY", None):
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
        cmd = "vault secrets list --format=json"
        out = subprocess.check_output(shlex.split(cmd))
        json_out = json.loads(out)
        for path in json_out.keys():
            if backend_path in path:
                return True
        return False

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
            and not config.ENV_DATA.get("VAULT_CA_ONLY", None)
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

        if not config.ENV_DATA.get("VAULT_CA_ONLY", None):
            # create client cert secret
            client_cert_data = templating.load_yaml(
                constants.EXTERNAL_VAULT_CLIENT_CERT
            )
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

    def create_ocs_kube_auth_resources(self, sa_name=constants.VAULT_CWD_KMS_SA_NAME):
        """
        This function will create the serviceaccount and clusterrolebindings
        required for kubernetes auth

        Args:
            sa_name (str): Name of the service account in ODF

        """
        ocp_obj = ocp.OCP()
        cmd = f"create -n {constants.OPENSHIFT_STORAGE_NAMESPACE} sa {sa_name}"
        ocp_obj.exec_oc_cmd(command=cmd)
        self.vault_cwd_kms_sa_name = sa_name
        logger.info(f"Created serviceaccount {sa_name}")

        cmd = (
            f"create -n {constants.OPENSHIFT_STORAGE_NAMESPACE} "
            "clusterrolebinding vault-tokenreview-binding "
            "--clusterrole=system:auth-delegator "
            f"--serviceaccount={constants.OPENSHIFT_STORAGE_NAMESPACE}:{sa_name}"
        )
        ocp_obj.exec_oc_cmd(command=cmd)
        logger.info("Created the clusterrolebinding vault-tokenreview-binding")

    def create_ocs_vault_resources(self):
        """
        This function takes care of creating ocp resources for
        secrets like ca cert, client cert, client key and vault token
        Assumption is vault section in AUTH file contains base64 encoded
        (with padding) ca, client certs, client key and vault path token

        """
        if not config.ENV_DATA.get("VAULT_SKIP_VERIFY"):
            self.create_ocs_vault_cert_resources()

        # Create resource and configure kubernetes auth method
        if config.ENV_DATA.get("VAULT_AUTH_METHOD") == constants.VAULT_KUBERNETES_AUTH:
            self.vault_auth_method = constants.VAULT_KUBERNETES_AUTH
            self.create_ocs_kube_auth_resources()
            self.vault_kube_auth_setup(
                auth_path=self.vault_kube_auth_path,
                token_reviewer_name=self.vault_cwd_kms_sa_name,
            )
            self.create_vault_kube_auth_role(
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                role_name=self.vault_kube_auth_role,
                sa_name="rook-ceph-system,rook-ceph-osd,noobaa",
            )
            self.create_vault_kube_auth_role(
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                role_name="odf-rook-ceph-osd",
                sa_name="rook-ceph-osd",
            )
        else:
            # create oc resource secret for token
            token_data = templating.load_yaml(constants.EXTERNAL_VAULT_KMS_TOKEN)
            # token has to base64 encoded (with padding)
            token_data["data"]["token"] = encode(self.vault_path_token)
            self.create_resource(token_data, prefix="token")

        # create ocs-kms-connection-details
        connection_data = templating.load_yaml(
            constants.EXTERNAL_VAULT_KMS_CONNECTION_DETAILS
        )
        connection_data["data"]["VAULT_ADDR"] = os.environ["VAULT_ADDR"]
        if Version.coerce(config.ENV_DATA["ocs_version"]) >= Version.coerce("4.10"):
            connection_data["data"]["VAULT_AUTH_METHOD"] = self.vault_auth_method
        else:
            connection_data["data"].pop("VAULT_AUTH_METHOD")
        connection_data["data"]["VAULT_BACKEND_PATH"] = self.vault_backend_path
        connection_data["data"]["VAULT_CACERT"] = self.ca_cert_name
        if not config.ENV_DATA.get("VAULT_CA_ONLY", None):
            connection_data["data"]["VAULT_CLIENT_CERT"] = self.client_cert_name
            connection_data["data"]["VAULT_CLIENT_KEY"] = self.client_key_name
        else:
            connection_data["data"].pop("VAULT_CLIENT_CERT")
            connection_data["data"].pop("VAULT_CLIENT_KEY")
        if config.ENV_DATA.get("use_vault_namespace"):
            connection_data["data"]["VAULT_NAMESPACE"] = self.vault_namespace
        connection_data["data"]["VAULT_TLS_SERVER_NAME"] = self.vault_tls_server
        if config.ENV_DATA.get("VAULT_AUTH_METHOD") == constants.VAULT_KUBERNETES_AUTH:
            connection_data["data"][
                "VAULT_AUTH_KUBERNETES_ROLE"
            ] = constants.VAULT_KUBERNETES_AUTH_ROLE

        else:
            connection_data["data"].pop("VAULT_AUTH_KUBERNETES_ROLE")
        if config.ENV_DATA.get("use_auth_path"):
            connection_data["data"]["VAULT_AUTH_MOUNT_PATH"] = self.vault_kube_auth_path
        else:
            connection_data["data"].pop("VAULT_AUTH_MOUNT_PATH")
        self.create_resource(connection_data, prefix="kmsconnection")

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

    def vault_create_backend_path(self, backend_path=None, kv_version=None):
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
            if kv_version:
                self.vault_backend_version = kv_version
            else:
                self.vault_backend_version = config.ENV_DATA.get("VAULT_BACKEND")
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

    def get_vault_kube_auth_role(self):
        """
        Fetch the role name from ocs-kms-connection-details configmap

        """
        if not self.vault_kube_auth_role:
            ocs_kms_configmap = ocp.OCP(
                kind="ConfigMap",
                resource_name=constants.VAULT_KMS_CONNECTION_DETAILS_RESOURCE,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            self.vault_kube_auth_role = ocs_kms_configmap.get().get("data")[
                "VAULT_AUTH_KUBERNETES_ROLE"
            ]
            logger.info(f"Setting vault_kube_auth_role = {self.vault_kube_auth_role}")

    def get_vault_policy(self):
        """
        Get the policy name based on token from vault

        """
        self.vault_policy_name = config.ENV_DATA.get("VAULT_POLICY", None)
        if not self.vault_policy_name:
            if config.ENV_DATA.get("VAULT_AUTH_METHOD") == constants.VAULT_TOKEN_AUTH:
                cmd = f"vault token lookup {self.vault_path_token}"
            else:
                cmd = f"vault read auth/{self.vault_kube_auth_path}/role/{self.vault_kube_auth_role}"
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
            if config.ENV_DATA.get("VAULT_AUTH_METHOD") == constants.VAULT_TOKEN_AUTH:
                self.get_vault_path_token()
            else:
                self.get_vault_kube_auth_role()
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
        self,
        kv_version,
        vault_auth_method=constants.VAULT_TOKEN,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    ):
        """
        Create vault specific csi kms connection details
        configmap resource

        """

        csi_kms_conn_details = templating.load_yaml(
            constants.EXTERNAL_VAULT_CSI_KMS_CONNECTION_DETAILS
        )
        if vault_auth_method == constants.VAULT_TOKEN:
            conn_str = csi_kms_conn_details["data"]["1-vault"]
            buf = json.loads(conn_str)
            buf["VAULT_ADDR"] = f"https://{self.vault_server}:{self.port}"
            buf["VAULT_BACKEND_PATH"] = self.vault_backend_path
            buf["VAULT_CACERT"] = get_default_if_keyval_empty(
                config.ENV_DATA, "VAULT_CACERT", defaults.VAULT_DEFAULT_CA_CERT
            )
            buf["VAULT_NAMESPACE"] = self.vault_namespace
            buf["VAULT_TOKEN_NAME"] = get_default_if_keyval_empty(
                config.ENV_DATA,
                "VAULT_TOKEN_NAME",
                constants.EXTERNAL_VAULT_CSI_KMS_TOKEN,
            )
            if kv_version == "v1":
                buf["VAULT_BACKEND"] = "kv"
            else:
                buf["VAULT_BACKEND"] = "kv-v2"

            csi_kms_conn_details["data"]["1-vault"] = json.dumps(buf)

        else:
            conn_str = csi_kms_conn_details["data"]["vault-tenant-sa"]
            buf = json.loads(conn_str)
            buf["vaultAddress"] = f"https://{self.vault_server}:{self.port}"
            buf["vaultBackendPath"] = self.vault_backend_path
            buf["vaultCAFromsecret"] = get_default_if_keyval_empty(
                config.ENV_DATA, "VAULT_CACERT", defaults.VAULT_DEFAULT_CA_CERT
            )
            buf["vaultClientCertFromSecret"] = self.client_cert_name
            buf["vaultClientCertKeyFromSecret"] = self.client_key_name
            if self.vault_namespace:
                buf["vaultNamespace"] = self.vault_namespace
            if self.vault_kube_auth_path:
                buf["vaultAuthPath"] = self.vault_kube_auth_path
            else:
                buf.pop("vaultAuthPath")
            if self.vault_kube_auth_namespace:
                buf["vaultAuthNamespace"] = self.vault_kube_auth_namespace
            else:
                buf.pop("vaultAuthNamespace")
            csi_kms_conn_details["data"]["vault-tenant-sa"] = json.dumps(buf)

        csi_kms_conn_details["metadata"]["namespace"] = namespace
        self.create_resource(csi_kms_conn_details, prefix="csikmsconn")

    def create_token_reviewer_resources(self):
        """
        This function will create the rbd-csi-vault-token-review SA, clusterRole
        and clusterRoleBindings required for the kubernetes auth method with
        vaulttenantsa encryption type.

        Raises:
            CommandFailed: Exception if the command fails

        """

        try:
            rbd_vault_token_reviewer = templating.load_yaml(
                constants.RBD_CSI_VAULT_TOKEN_REVIEWER, multi_document=True
            )
            self.create_resource(rbd_vault_token_reviewer, prefix="rbd-token-review")
            logger.info("rbd-csi-vault-token-reviewer resources created successfully")

        except CommandFailed as cfe:
            if "AlreadyExists" in str(cfe):
                logger.warning("rbd-csi-vault-token-reviewer resources already exists")
            else:
                raise

    def create_tenant_sa(self, namespace):
        """
        This function will create the serviceaccount in the tenant namespace to
        authenticate to Vault when vaulttenantsa KMS type is used for PV encryption.

        Args:
            namespace (str): The tenant namespace where the service account will be created
        """

        tenant_sa = templating.load_yaml(constants.RBD_CSI_VAULT_TENANT_SA)
        tenant_sa["metadata"]["namespace"] = namespace
        self.create_resource(tenant_sa, prefix="tenant-sa")
        logger.info("Tenant SA ceph-csi-vault-sa created successfully")

    def create_tenant_configmap(
        self,
        tenant_namespace,
        **vault_config,
    ):
        """
        This functional will create a configmap in the tenant namespace to override
        the vault config in csi-kms-connection-details configmap.

        Args:
            tenant_namespace (str): Tenant namespace
            vaultBackend (str): KV version to be used, either kv or kv-v2
            vaultBackendPath (str): The backend path in Vault where the encryption
                                      keys will be stored
            vaultNamespace (str): Namespace in Vault, if exists, where the backend
                                   path is created
            vaultRole (str): (Vaulttenantsa) The role name in Vault configured with
                              kube auth method for the given policy and tenant namespace
            vaultAuthPath (str): (Vaulttenantsa) The path where kubernetes auth
                                   method is enabled
            vaultAuthNamespace (str): (Vaulttenantsa) The namespace where kubernetes
                                        auth method is enabled, if exists
        """

        logger.info(f"Creating tenant configmap in namespace {tenant_namespace}")
        tenant_cm = templating.load_yaml(constants.RBD_CSI_VAULT_TENANT_CONFIGMAP)
        tenant_cm["metadata"]["namespace"] = tenant_namespace

        merge_dict(tenant_cm["data"], vault_config)
        for k in tenant_cm["data"].copy():
            if not tenant_cm["data"][k]:
                tenant_cm["data"].pop(k)

        self.create_resource(tenant_cm, prefix="tenant-cm")
        logger.info("Tenant ConfigMap ceph-csi-kms-config created successfully")

    def vault_kube_auth_setup(
        self,
        auth_path=None,
        auth_namespace=None,
        token_reviewer_name=constants.RBD_CSI_VAULT_TOKEN_REVIEWER_NAME,
    ):
        """
        Setup kubernetes auth method in Vault

        Args:
            auth_path (str): The path where kubernetes auth is to be enabled.
                If not provided default 'kubernetes' path is used
            auth_namespace (str): The vault namespace where kubernetes auth is
                to be enabled, if applicable
            token_reviewer_name (str): Name of the token-reviewer serviceaccount
                in openshift-storage namespace

        Raises:
            VaultOperationError: if kube auth method setup fails

        """

        # Get secret name from serviceaccount
        logger.info("Retrieving secret name from serviceaccount ")
        cmd = (
            f"oc get sa {token_reviewer_name} -o jsonpath='{{.secrets[*].name}}'"
            f" -n {constants.OPENSHIFT_STORAGE_NAMESPACE}"
        )
        secrets = run_cmd(cmd=cmd).split()
        for secret in secrets:
            if "-token-" in secret:
                secret_name = secret
        if not secret_name:
            raise NotFoundError("Secret name not found")

        # Get token from secrets
        logger.info(f"Retrieving token from {secret_name}")
        cmd = (
            rf"oc get secret {secret_name} -o jsonpath=\"{{.data[\'token\']}}\""
            f" -n {constants.OPENSHIFT_STORAGE_NAMESPACE}"
        )
        token = base64.b64decode(run_cmd(cmd=cmd)).decode()
        token_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="test", delete=True, dir="."
        )
        token_file_name = os.path.basename(token_file.name)
        with open(token_file.name, "w") as t:
            t.write(token)

        # Get ca.crt from secret
        logger.info(f"Retrieving CA cert from {secret_name}")
        ca_regex = r"{.data['ca\.crt']}"
        cmd = f'oc get secret -n {constants.OPENSHIFT_STORAGE_NAMESPACE} {secret_name} -o jsonpath="{ca_regex}"'
        ca_crt = base64.b64decode(run_cmd(cmd=cmd)).decode()
        ca_file = tempfile.NamedTemporaryFile(
            mode="w+", prefix="test", dir=".", delete=True
        )
        ca_file_name = os.path.basename(ca_file.name)
        with open(ca_file.name, "w") as ca:
            ca.write(ca_crt)

        # get cluster API endpoint
        k8s_host = run_cmd(cmd="oc whoami --show-server").strip()

        # enable kubernetes auth method
        if auth_path and auth_namespace:
            self.vault_kube_auth_namespace = auth_namespace
            cmd = f"vault auth enable -namespace={auth_namespace} -path={auth_path} kubernetes"

        elif auth_path:
            cmd = f"vault auth enable -path={auth_path} kubernetes"

        elif auth_namespace:
            self.vault_kube_auth_namespace = auth_namespace
            cmd = f"vault auth enable -namespace={auth_namespace} kubernetes"

        else:
            cmd = "vault auth enable kubernetes"
        proc = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        out, err = proc.communicate()
        if proc.returncode:
            if "path is already in use" not in err:
                raise VaultOperationError
        if auth_namespace:
            cmd = (
                f"vault write -namespace={self.vault_kube_auth_namespace} "
                f"auth/{self.vault_kube_auth_path}/config token_reviewer_jwt=@{token_file_name} "
                f"kubernetes_host={k8s_host} kubernetes_ca_cert=@{ca_file_name}"
            )
        # Configure kubernetes auth method
        else:
            cmd = (
                f"vault write auth/{self.vault_kube_auth_path}/config token_reviewer_jwt=@{token_file_name} "
                f"kubernetes_host={k8s_host} kubernetes_ca_cert=@{ca_file_name}"
            )
        os.environ.pop("VAULT_FORMAT")
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            env=os.environ,
        )

        if "Success" in proc.stdout.decode():
            logger.info("vault: Kubernetes auth method configured successfully")
        else:
            raise VaultOperationError("Failed to configure kubernetes auth method")
        token_file.close()
        ca_file.close()

    def create_vault_kube_auth_role(
        self,
        namespace,
        role_name="csi-kubernetes",
        sa_name="ceph-csi-vault-sa",
    ):
        """
        Create a role for tenant authentication in Vault

        Args:
           namespace (str): namespace in ODF cluster
           role_name (str): Name of the role in Vault
           sa_name (str): Service account in the tenant namespace to be used for authentication

        """

        cmd = (
            f"vault write auth/{self.vault_kube_auth_path}/role/{role_name} "
            f"bound_service_account_names={sa_name} policies={self.vault_policy_name} "
            f"bound_service_account_namespaces={namespace} ttl=1440h"
        )
        out = subprocess.check_output(shlex.split(cmd))
        if "Success" in out.decode():
            logger.info(f"Role {role_name} created successfully")


class HPCS(KMS):
    """
    A class which handles deployment and other
    configs related to HPCS

    """

    def __init__(self):
        super().__init__("HPCS")
        self.ibm_kp_service_instance_id = None
        self.ibm_kp_secret_name = None
        self.kms_service_name = None
        self.ibm_kp_service_api_key = None
        self.ibm_kp_customer_root_key = None
        self.ibm_kp_base_url = None
        self.ibm_kp_token_url = None
        # default and only supported deploy mode for HPCS
        self.hpcs_deploy_mode = "external"
        self.kmsid = None

    def deploy(self):
        """
        This function delegates the deployment of hpcs
        based on OCP or vault standalone external mode deployment

        """
        if self.hpcs_deploy_mode == "external":
            self.deploy_hpcs_external()
        else:
            raise HPCSDeploymentError("Not a supported HPCS deployment mode")

    def deploy_hpcs_external(self):
        """
        This function takes care of deployment and configuration
        for external mode hpcs deployment. We are assuming that
        an external hpcs service already exists and we will be just
        configuring the necessary OCP objects for OCS like secrets, token etc

        """
        self.gather_init_hpcs_conf()
        self.create_ocs_hpcs_resources()

    def gather_init_hpcs_conf(self):
        """
        Gather hpcs configuration and init the vars
        This function currently gathers only for external mode

        """
        self.hpcs_conf = self.gather_hpcs_config()
        self.ibm_kp_service_instance_id = self.hpcs_conf["IBM_KP_SERVICE_INSTANCE_ID"]
        self.ibm_kp_service_api_key = self.hpcs_conf["IBM_KP_SERVICE_API_KEY"]
        self.ibm_kp_customer_root_key = self.hpcs_conf["IBM_KP_CUSTOMER_ROOT_KEY"]
        self.ibm_kp_base_url = self.hpcs_conf["IBM_KP_BASE_URL"]
        self.ibm_kp_token_url = self.hpcs_conf["IBM_KP_TOKEN_URL"]
        self.ibm_kp_secret_name = "ibm-kp-kms-test-secret"

    def create_ocs_hpcs_resources(self):
        """
        This function takes care of creating ocp resources for
        secrets like hpcs customer root key, service api key, etc.
        Assumption is hpcs section in AUTH file contains hpcs service
        instance id, base url, token url, api key and customer root key.

        """

        # create ibm-kp-kms-secret-somestring secret
        ibm_kp_secret_name = self.create_ibm_kp_kms_secret()
        # update the ibm_kp_secret_name with the parsed secret name
        self.ibm_kp_secret_name = ibm_kp_secret_name

        # 2. create ocs-kms-connection-details
        connection_data = templating.load_yaml(
            constants.EXTERNAL_HPCS_KMS_CONNECTION_DETAILS
        )
        connection_data["data"]["IBM_KP_BASE_URL"] = self.ibm_kp_base_url
        connection_data["data"]["IBM_KP_SECRET_NAME"] = self.ibm_kp_secret_name
        connection_data["data"][
            "IBM_KP_SERVICE_INSTANCE_ID"
        ] = self.ibm_kp_service_instance_id
        connection_data["data"]["IBM_KP_TOKEN_URL"] = self.ibm_kp_token_url
        self.create_resource(connection_data, prefix="kmsconnection")

    def delete_resource(self, resource_name, resource_type, resource_namespace):
        """
        Given resource type, resource name and namespace, this function will
        delete oc resource

        Args:
            resource_name : name of the resource
            resource_type: type of resource such as secret
            resource_namespace: namespace in which resource is present

        """
        run_cmd(
            f"oc delete {resource_type} {resource_name} -n {resource_namespace}",
            timeout=300,
        )

    def gather_hpcs_config(self):
        """
        This function populates the hpcs configuration

        """
        if self.hpcs_deploy_mode == "external":
            hpcs_conf = load_auth_config()["hpcs"]
            return hpcs_conf

    def create_ibm_kp_kms_secret(self, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE):
        """
        create hpcs specific csi kms secret resource

        """
        ibm_kp_kms_secret = templating.load_yaml(constants.EXTERNAL_IBM_KP_KMS_SECRET)
        ibm_kp_kms_secret["data"][
            "IBM_KP_CUSTOMER_ROOT_KEY"
        ] = self.ibm_kp_customer_root_key
        ibm_kp_kms_secret["data"][
            "IBM_KP_SERVICE_API_KEY"
        ] = self.ibm_kp_service_api_key
        ibm_kp_kms_secret["metadata"]["name"] = helpers.create_unique_resource_name(
            "test", "ibm-kp-kms"
        )
        ibm_kp_kms_secret["metadata"]["namespace"] = namespace
        self.create_resource(ibm_kp_kms_secret, prefix="ibmkpkmssecret")

        return ibm_kp_kms_secret["metadata"]["name"]

    def create_hpcs_csi_kms_connection_details(
        self, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    ):
        """
        Create hpcs specific csi kms connection details
        configmap resource

        """
        # create hpcs secret resource
        ibm_kp_secret_name = self.create_ibm_kp_kms_secret()

        csi_kms_conn_details = templating.load_yaml(
            constants.EXTERNAL_HPCS_CSI_KMS_CONNECTION_DETAILS
        )
        conn_str = csi_kms_conn_details["data"]["1-hpcs"]
        buf = json.loads(conn_str)
        buf["IBM_KP_SERVICE_INSTANCE_ID"] = self.ibm_kp_service_instance_id
        buf["IBM_KP_SECRET_NAME"] = ibm_kp_secret_name
        buf["IBM_KP_BASE_URL"] = self.ibm_kp_base_url
        buf["IBM_KP_TOKEN_URL"] = self.ibm_kp_token_url

        csi_kms_conn_details["data"]["1-hpcs"] = json.dumps(buf)
        csi_kms_conn_details["metadata"]["namespace"] = namespace
        self.create_resource(csi_kms_conn_details, prefix="csikmsconn")

    def cleanup(self):
        """
        Cleanup the backend resources in case of external

        """
        # nothing to cleanup as of now
        logger.warning("Nothing to cleanup from HPCS")

    def post_deploy_verification(self):
        """
        Validating the OCS deployment from hpcs perspective

        """
        if config.ENV_DATA.get("hpcs_deploy_mode") == "external":
            self.validate_external_hpcs()

    def get_token_for_ibm_api_key(self):
        """
        This function retrieves the access token in exchange of
        an IBM API key

        Return:
            (str): access token for authentication with IBM endpoints
        """
        # decode service api key
        api_key = base64.b64decode(self.ibm_kp_service_api_key).decode()
        payload = {
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
            "apikey": api_key,
        }
        r = requests.post(
            self.ibm_kp_token_url,
            headers={"content-type": "application/x-www-form-urlencoded"},
            data=payload,
            verify=True,
        )
        assert r.ok, f"Couldn't get access token! StatusCode: {r.status_code}."
        return "Bearer " + r.json()["access_token"]

    def list_hpcs_keys(self):
        """
        This function lists the keys present in a HPCS instance

        Return:
            (list): list of keys in a HPCS instance
        """
        access_token = self.get_token_for_ibm_api_key()
        r = requests.get(
            f"{self.ibm_kp_base_url}" + "/api/v2/keys",
            headers={
                "accept": "application/vnd.ibm.kms.key+json",
                "bluemix-instance": self.ibm_kp_service_instance_id,
                "authorization": access_token,
            },
            verify=True,
        )
        assert r.ok, f"Couldn't list HPCS keys! StatusCode: {r.status_code}."
        return r.json()["resources"]

    def validate_external_hpcs(self):
        """
        This function is for post OCS deployment HPCS
        verification

        Following checks will be done
        1. check osd encryption keys in the HPCS path
        2. check noobaa keys in the HPCS path
        3. check storagecluster CR for 'kms' enabled

        Raises:
            NotFoundError : if key not found in HPCS OR in the resource CR

        """
        self.gather_init_hpcs_conf()
        kvlist = self.list_hpcs_keys()
        # Check osd keys are present
        osds = pod.get_osd_pods()
        for osd in osds:
            pvc = (
                osd.get()
                .get("metadata")
                .get("labels")
                .get(constants.CEPH_ROOK_IO_PVC_LABEL)
            )
            if any(pvc in k["name"] for k in kvlist):
                logger.info(f"HPCS: Found key for {pvc}")
            else:
                logger.error(f"HPCS: Key not found for {pvc}")
                raise NotFoundError("HPCS key not found")

        # Check kms enabled
        if not is_kms_enabled():
            logger.error("KMS not enabled on storage cluster")
            raise NotFoundError("KMS flag not found")


kms_map = {"vault": Vault, "hpcs": HPCS}


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


def is_kms_enabled(dont_raise=False):
    """
    Checks StorageCluster yaml if kms is configured.

    Return:
        (bool): True if KMS is configured else False

    """
    cluster = storage_cluster.get_storage_cluster()
    logger.info("Checking if StorageCluster has configured KMS encryption")
    resource_get = cluster.get(dont_raise=dont_raise)
    if resource_get:
        resource = resource_get["items"][0]
        encryption = (
            resource.get("spec").get("encryption", {}).get("kms", {}).get("enable")
        )
        return bool(encryption)


def vault_kv_list(path):
    """
    List kv from a given path

    Args:
        path (str): Vault backend path name

    Returns:
        list: of kv present in the path

    """
    cmd = f"vault kv list -format=json {path}"
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
        if constants.VAULT_KMS_PROVIDER or constants.HPCS_KMS_PROVIDER in key:
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
        f"patch -n {constants.OPENSHIFT_STORAGE_NAMESPACE} cm "
        f"{constants.VAULT_KMS_CSI_CONNECTION_DETAILS} --type json -p " + patch
    )
    ocp_obj.exec_oc_cmd(command=patch_cmd)
    kmsid_list = get_encryption_kmsid()
    if kmsid in kmsid_list:
        raise KMSResourceCleaneupError(f"KMS ID {kmsid} deletion failed")
    logger.info(f"KMS ID {kmsid} deleted")


def remove_token_reviewer_resources():
    """
    Delete the SA, clusterRole and clusterRoleBindings for token reviewer

    """

    run_cmd(
        f"oc delete sa {constants.RBD_CSI_VAULT_TOKEN_REVIEWER_NAME} -n {constants.OPENSHIFT_STORAGE_NAMESPACE}"
    )
    run_cmd(f"oc delete ClusterRole {constants.RBD_CSI_VAULT_TOKEN_REVIEWER_NAME}")
    run_cmd(
        f"oc delete ClusterRoleBinding {constants.RBD_CSI_VAULT_TOKEN_REVIEWER_NAME}"
    )
