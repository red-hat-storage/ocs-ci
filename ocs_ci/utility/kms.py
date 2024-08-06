"""
This module contains KMS related class and methods
currently supported KMSs: Vault and HPCS

"""

import logging
import os

import requests
import json
import platform
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
    KMIPDeploymentError,
    KMIPOperationError,
    UnsupportedOSType,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility import templating, version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    download_file,
    delete_file,
    load_auth_config,
    run_cmd,
    get_vault_cli,
    get_running_cluster_id,
    get_default_if_keyval_empty,
    get_cluster_name,
    get_ocp_version,
    encode,
    prepare_bin_dir,
)
from fauxfactory import gen_alphanumeric
from azure.identity import CertificateCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import AzureError
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs


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

        if config.ENV_DATA.get("vault_hcp"):
            self.vault_namespace = (
                f"{constants.VAULT_HCP_NAMESPACE}/{self.vault_namespace}"
            )
        os.environ["VAULT_NAMESPACE"] = self.vault_namespace

    def vault_namespace_exists(self, vault_namespace):
        """
        Check if vault namespace already exists

        Args:
            vault_namespace (str): name of the vault namespace

        Returns:
            bool: True if exists else False

        """
        if config.ENV_DATA.get("vault_hcp"):
            cmd = f"vault namespace lookup -namespace={constants.VAULT_HCP_NAMESPACE} {vault_namespace}"
        else:
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
        if config.ENV_DATA.get("vault_hcp"):
            cmd = f"vault namespace create -namespace={constants.VAULT_HCP_NAMESPACE} {vault_namespace}"
        else:
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
            self.client_cert_name = get_default_if_keyval_empty(
                config.ENV_DATA, "VAULT_CLIENT_CERT", defaults.VAULT_DEFAULT_CLIENT_CERT
            )
            self.client_key_name = get_default_if_keyval_empty(
                config.ENV_DATA, "VAULT_CLIENT_KEY", defaults.VAULT_DEFAULT_CLIENT_KEY
            )
            # create client cert secret
            client_cert_data = templating.load_yaml(
                constants.EXTERNAL_VAULT_CLIENT_CERT
            )
            client_cert_data["metadata"]["name"] = self.client_cert_name
            client_cert_data["data"]["cert"] = self.client_cert_base64
            self.create_resource(client_cert_data, prefix="clientcert")

            # create client key secert
            client_key_data = templating.load_yaml(constants.EXTERNAL_VAULT_CLIENT_KEY)
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
        cmd = f"create -n {config.ENV_DATA['cluster_namespace']} sa {sa_name}"
        ocp_obj.exec_oc_cmd(command=cmd)
        self.vault_cwd_kms_sa_name = sa_name
        logger.info(f"Created serviceaccount {sa_name}")

        cmd = (
            f"create -n {config.ENV_DATA['cluster_namespace']} "
            "clusterrolebinding vault-tokenreview-binding "
            "--clusterrole=system:auth-delegator "
            f"--serviceaccount={config.ENV_DATA['cluster_namespace']}:{sa_name}"
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
                namespace=config.ENV_DATA["cluster_namespace"],
                role_name=self.vault_kube_auth_role,
                sa_name="rook-ceph-system,rook-ceph-osd,noobaa",
            )
            self.create_vault_kube_auth_role(
                namespace=config.ENV_DATA["cluster_namespace"],
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
            if config.ENV_DATA.get("use_vault_namespace"):
                vault_conf = load_auth_config()["vault_hcp"]
            else:
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
            namespace=config.ENV_DATA["cluster_namespace"],
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
                namespace=config.ENV_DATA["cluster_namespace"],
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
                namespace=config.ENV_DATA["cluster_namespace"],
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

    def remove_vault_backend_path(self, vault_namespace=None):
        """
        remove vault path

        Args:
            vault_namespace (str): Namespace in Vault, if exists, where the backend path is created
        """

        if vault_namespace:
            cmd = f"vault secrets disable -namespace={vault_namespace} {self.vault_backend_path}"
        else:
            cmd = f"vault secrets disable {self.vault_backend_path}"
        subprocess.check_output(shlex.split(cmd))
        # Check if path doesn't appear in the list
        if vault_namespace:
            cmd = f"vault secrets list -namespace={vault_namespace} --format=json"
        else:
            cmd = "vault secrets list --format=json"
        out = subprocess.check_output(shlex.split(cmd))
        json_out = json.loads(out)
        for path in json_out.keys():
            if self.vault_backend_path in path:
                raise KMSResourceCleaneupError(
                    f"Path {self.vault_backend_path} not deleted"
                )
        logger.info(f"Vault path {self.vault_backend_path} deleted")

    def remove_vault_policy(self, vault_namespace=None):
        """
        Cleanup the policy we used

        Args:
            vault namespace (str): Namespace in Vault, if exists, where the backend path is created
        """

        if vault_namespace:
            cmd = f"vault policy delete -namespace={vault_namespace} {self.vault_policy_name} "
        else:
            cmd = f"vault policy delete {self.vault_policy_name}"
        subprocess.check_output(shlex.split(cmd))

        # Check if policy still exists
        if vault_namespace:
            cmd = f"vault policy list -namespace={vault_namespace} --format=json"
        else:
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
        if os.environ.get("VAULT_NAMESPACE"):
            os.environ.pop("VAULT_NAMESPACE")

        if config.ENV_DATA.get("vault_hcp"):
            self.vault_namespace = self.vault_namespace.replace("admin/", "")
            cmd = f"vault namespace delete -namespace={constants.VAULT_HCP_NAMESPACE} {self.vault_namespace}/"
        else:
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

    def create_vault_csi_kms_token(self, namespace=None):
        """
        create vault specific csi kms secret resource

        Args:
            namespace (str) the namespace of the resource. If None is provided
                then value from config will be used.

        """
        if namespace is None:
            namespace = config.ENV_DATA["cluster_namespace"]
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
        namespace=None,
    ):
        """
        Create vault specific csi kms connection details
        configmap resource

        """
        if namespace is None:
            namespace = config.ENV_DATA["cluster_namespace"]

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
            buf["vaultCAFromSecret"] = get_default_if_keyval_empty(
                config.ENV_DATA, "VAULT_CACERT", defaults.VAULT_DEFAULT_CA_CERT
            )
            if not config.ENV_DATA.get("VAULT_CA_ONLY", None):
                buf["vaultClientCertFromSecret"] = get_default_if_keyval_empty(
                    config.ENV_DATA,
                    "VAULT_CLIENT_CERT",
                    defaults.VAULT_DEFAULT_CLIENT_CERT,
                )
                buf["vaultClientCertKeyFromSecret"] = get_default_if_keyval_empty(
                    config.ENV_DATA,
                    "VAULT_CLIENT_KEY",
                    defaults.VAULT_DEFAULT_CLIENT_KEY,
                )
            else:
                buf.pop("vaultClientCertFromSecret")
                buf.pop("vaultClientCertKeyFromSecret")

            if self.vault_namespace:
                buf["vaultNamespace"] = self.vault_namespace
            if self.vault_kube_auth_path:
                buf["vaultAuthPath"] = f"/v1/auth/{self.vault_kube_auth_path}/login"
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
        if Version.coerce(get_ocp_version()) < Version.coerce("4.11"):
            logger.info("Retrieving secret name from serviceaccount ")
            cmd = (
                f"oc get sa {token_reviewer_name} -o jsonpath='{{.secrets[*].name}}'"
                f" -n {config.ENV_DATA['cluster_namespace']}"
            )
            secrets = run_cmd(cmd=cmd).split()
            secret_name = ""
            for secret in secrets:
                if "-token-" in secret and "docker" not in secret:
                    secret_name = secret
            if not secret_name:
                raise NotFoundError("Secret name not found")
        else:
            secret_name = helpers.create_sa_token_secret(sa_name=token_reviewer_name)

        # Get token from secrets
        logger.info(f"Retrieving token from {secret_name}")
        cmd = (
            rf"oc get secret {secret_name} -o jsonpath=\"{{.data[\'token\']}}\""
            f" -n {config.ENV_DATA['cluster_namespace']}"
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
        cmd = f'oc get secret -n {config.ENV_DATA["cluster_namespace"]} {secret_name} -o jsonpath="{ca_regex}"'
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
            self.vault_kube_auth_path = auth_path
            cmd = f"vault auth enable -namespace={auth_namespace} -path={auth_path} kubernetes"

        elif auth_path:
            self.vault_kube_auth_path = auth_path
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

    def create_ibm_kp_kms_secret(self, namespace=config.ENV_DATA["cluster_namespace"]):
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
        self, namespace=config.ENV_DATA["cluster_namespace"]
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


class KMIP(KMS):
    """
    A class which handles deployment and other
    configs related to KMIP (Thales CipherTrust Manager)

    """

    def __init__(self):
        super().__init__("kmip")
        self.kmip_secret_name = None
        self.kmip_key_identifier = None
        self.kmsid = None
        logger.info("Loading KMIP details from auth config")
        self.kmip_conf = load_auth_config()["kmip"]
        self.kmip_endpoint = self.kmip_conf["KMIP_ENDPOINT"]
        self.kmip_ciphertrust_user = self.kmip_conf["KMIP_CTM_USER"]
        self.kmip_ciphertrust_pwd = self.kmip_conf["KMIP_CTM_PWD"]
        self.kmip_port = self.kmip_conf["KMIP_PORT"]
        self.kmip_ca_cert_base64 = self.kmip_conf["KMIP_CA_CERT_BASE64"]
        self.kmip_client_cert_base64 = self.kmip_conf["KMIP_CLIENT_CERT_BASE64"]
        self.kmip_client_key_base64 = self.kmip_conf["KMIP_CLIENT_KEY_BASE64"]
        self.kmip_tls_server_name = self.kmip_conf["KMIP_TLS_SERVER_NAME"]

    def deploy(self):
        """
        This function delegates the deployment of KMS using KMIP.
        Thales CipherTrust Manager is the only supported vendor for now.

        """
        if version.get_semantic_ocs_version_from_config() >= version.VERSION_4_12:
            self.deploy_kmip_ciphertrust()
        else:
            raise KMIPDeploymentError(
                "Use of KMIP is not supported on clusters below ODF 4.12"
            )

    def deploy_kmip_ciphertrust(self):
        """
        This function configures the resources required to use Thales CipherTrust Manager with ODF

        """
        self.update_kmip_env_vars()
        get_ksctl_cli()
        self.create_odf_kmip_resources()

    def update_kmip_env_vars(self):
        """
        Set the environment variable for CipherTrust to allow running ksctl CLI cmds

        """
        logger.info("Updating environment variables for KMIP")
        os.environ["KSCTL_USERNAME"] = self.kmip_ciphertrust_user
        os.environ["KSCTL_PASSWORD"] = self.kmip_ciphertrust_pwd
        os.environ["KSCTL_URL"] = f"https://{self.kmip_endpoint}"
        os.environ["KSCTL_NOSSLVERIFY"] = "true"

    def create_odf_kmip_resources(self):
        """
        Create secret containing certs and the ocs-kms-connection-details confignmap

        """
        self.kmip_secret_name = self.create_kmip_secret()
        connection_data = templating.load_yaml(
            constants.KMIP_OCS_KMS_CONNECTION_DETAILS
        )
        connection_data["data"][
            "KMIP_ENDPOINT"
        ] = f"{self.kmip_endpoint}:{self.kmip_port}"
        connection_data["data"]["KMIP_SECRET_NAME"] = self.kmip_secret_name
        connection_data["data"]["TLS_SERVER_NAME"] = self.kmip_tls_server_name
        self.create_resource(connection_data, prefix="kms-connection")

    def create_kmip_secret(self, type="ocs"):
        """
        Create secret containing the certificates and unique identifier (only for PV encryption)
        from CipherTrust Manager KMS

        Args:
            type (str): csi, if the secret is being created for PV/Storageclass encryption
                        ocs, if the secret is for clusterwide encryption

        Returns:
            (str): name of the kmip secret

        """

        logger.info(f"Creating {type} KMIP secret ")
        if type == "csi":
            kmip_kms_secret = templating.load_yaml(constants.KMIP_CSI_KMS_SECRET)
            self.kmip_key_identifier = self.create_ciphertrust_key(key_name=self.kmsid)
            kmip_kms_secret["data"]["UNIQUE_IDENTIFIER"] = encode(
                self.kmip_key_identifier
            )

        elif type == "ocs":
            kmip_kms_secret = templating.load_yaml(constants.KMIP_OCS_KMS_SECRET)

        else:
            raise ValueError("The value should be either 'ocs' or 'csi'")

        kmip_kms_secret["data"]["CA_CERT"] = self.kmip_ca_cert_base64
        kmip_kms_secret["data"]["CLIENT_CERT"] = self.kmip_client_cert_base64
        kmip_kms_secret["data"]["CLIENT_KEY"] = self.kmip_client_key_base64
        kmip_kms_secret["metadata"]["name"] = helpers.create_unique_resource_name(
            "thales-kmip", type
        )
        self.create_resource(kmip_kms_secret, prefix="thales-kmip-secret")
        logger.info(f"KMIP secret {kmip_kms_secret['metadata']['name']} created")
        return kmip_kms_secret["metadata"]["name"]

    def create_ciphertrust_key(self, key_name):
        """
        Create a key in Ciphertrust Manager to be used for PV encryption

        Args:
            key_name (str): Name of the key to be created

        Returns:
            (str): ID of the key created in CipherTrust

        """
        logger.info(f"Creating key {key_name} in CipherTrust")
        cmd = f"ksctl keys create --name {key_name} --alg AES --size 256"
        proc = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if proc.returncode:
            raise KMIPOperationError("KMIP: Key creation failed", err)
        key_info = json.loads(out)
        return key_info["id"]

    def delete_ciphertrust_key(self, key_id):
        """
        Delete key from CipherTrust Manager

        Args:
            key_id (str): ID of the key to be deleted
        """

        logger.info(f"Deleting key with ID {key_id}")
        cmd = f"ksctl keys delete --type id --name {key_id}"
        proc = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if proc.returncode:
            if "Resource not found" in err.decode():
                logger.warning(f"Key with ID {key_id} does not exist")
                return
            else:
                raise KMIPOperationError

        if self.check_key_exists_in_ciphertrust(key_id):
            raise KMSResourceCleaneupError(f"Key deletion failed for ID {key_id}")
        logger.info(f"Key deletion successful for key ID: {key_id}")

    def check_key_exists_in_ciphertrust(self, key_id):
        """
        Check if a key with the key_id is present in CipherTrust Manager

        Args:
            key_id (str): ID of the key to be checked for

        Returns:
            (bool): True, if the key exists in CipherTrust Manager

        """
        cmd = f"ksctl keys get --type id --name {key_id}"
        proc = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if proc.returncode:
            if "Resource not found" in err.decode():
                return False
        return True

    def get_key_info_ciphertrust(self, key_id):
        """
        Retrieve information about a given key

        Args:
            key_id (str): ID of the key in CipherTrust

        Returns
            (dict): Dictionary with key details

        """
        logger.info(f"Retrieving key info for key ID {key_id}")
        cmd = f"ksctl keys get info --type id --name {key_id}"
        proc = subprocess.Popen(
            shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if proc.returncode:
            if "Resource not found" in err.decode():
                raise NotFoundError(f"KMIP: Key with ID {key_id} not found")
        else:
            return json.loads(out)

    def get_key_list_ciphertrust(self, limit=100):
        """
        Lists all keys in CipherTrust Manager

        Args:
            limit (int): number of entries to limit the results

        Returns:
            (list): list containing the IDs of the keys

        """
        key_id_list = []
        total = None
        while len(key_id_list) != total:
            cmd = f"ksctl keys list --limit {limit} --skip {len(key_id_list)}"
            out = subprocess.check_output(shlex.split(cmd))
            json_out = json.loads(out)
            total = json_out["total"]
            if total == 0:
                raise NotFoundError("No keys found")
            else:
                for key in json_out["resources"]:
                    key_id_list.append(key["id"])
                return key_id_list

    def get_osd_key_ids(self):
        """
        Retrieve the key ID used for OSD encryption stored in their respective secrets

        Returns:
            (list): list of key IDs from all OSDs

        """
        key_ids = []
        osds = pod.get_osd_pods()
        for osd in osds:
            pvc = (
                osd.get()
                .get("metadata")
                .get("labels")
                .get(constants.CEPH_ROOK_IO_PVC_LABEL)
            )
            cmd = (
                rf"oc get secret rook-ceph-osd-encryption-key-{pvc} -o jsonpath=\"{{.data[\'dmcrypt-key\']}}\""
                f" -n {config.ENV_DATA['cluster_namespace']}"
            )
            key_ids.append(base64.b64decode(run_cmd(cmd=cmd)).decode())
        return key_ids

    def get_noobaa_key_id(self):
        """
        Retrieve the key ID used for encryption by Noobaa

        Returns:
            (str): Key ID used by NooBaa for encryption

        """
        cmd = (
            rf"oc get cm ocs-kms-connection-details -o jsonpath=\"{{.data[\'KMIP_SECRET_NAME\']}}\""
            f" -n {config.ENV_DATA['cluster_namespace']}"
        )
        secret_name = run_cmd(cmd=cmd)

        cmd = (
            rf"oc get secret {secret_name} -o jsonpath=\"{{.data[\'UniqueIdentifier\']}}\""
            f" -n {config.ENV_DATA['cluster_namespace']}"
        )
        noobaa_key_id = base64.b64decode(run_cmd(cmd=cmd)).decode()
        return noobaa_key_id

    def post_deploy_verification(self):
        """
        Verify ODF deployment using KMIP

        """
        self.validate_ciphertrust_deployment()

    @retry(NotFoundError, tries=2, delay=30)
    def validate_ciphertrust_deployment(self):
        """
        Verify whether OSD and NooBaa keys are stored in CipherTrust Manager

        """
        self.update_kmip_env_vars()

        # Check for OSD keys
        osd_key_ids = self.get_osd_key_ids()
        # Loading key list after gathering OSD pods to avoid mismatch.
        key_id_list = self.get_key_list_ciphertrust()
        if all(id in key_id_list for id in osd_key_ids):
            logger.info("KMIP: All OSD keys found in CipherTrust Manager")
        else:
            logger.error(
                "KMIP: Only some or no OSD keys found in CipherTrust Manager"
                f"Keys found in CipherTrust Manager: {key_id_list}"
                f"OSD keys from the ODF cluster: {osd_key_ids}"
            )
            raise NotFoundError(
                "KMIP: Only some or no OSD keys found in CipherTrust Manager"
            )

        # Check for NOOBAA key
        noobaa_key_id = self.get_noobaa_key_id()
        if noobaa_key_id in key_id_list:
            logger.info("KMIP: Noobaa encryption key found in CipherTrust Manager")
        else:
            raise NotFoundError(
                "KMIP: Noobaa encryption key found in CipherTrust Manager"
            )

        # Check kms enabled
        if not is_kms_enabled():
            logger.error("KMS not enabled on storage cluster")
            raise NotFoundError("KMS flag not found")

    def create_kmip_csi_kms_connection_details(
        self, namespace=config.ENV_DATA["cluster_namespace"]
    ):
        """
        Create KMIP specific csi-kms-connection-details
        configmap resource

        """

        csi_kms_conn_details = templating.load_yaml(
            constants.KMIP_CSI_KMS_CONNECTION_DETAILS
        )
        conn_str = csi_kms_conn_details["data"]["1-kmip"]
        buf = json.loads(conn_str)
        buf["KMIP_ENDPOINT"] = f"{self.kmip_endpoint}:{self.kmip_port}"
        buf["KMIP_SECRET_NAME"] = self.kmip_secret_name
        buf["TLS_SERVER_NAME"] = self.kmip_tls_server_name

        csi_kms_conn_details["data"]["1-kmip"] = json.dumps(buf)
        csi_kms_conn_details["metadata"]["namespace"] = namespace
        self.create_resource(csi_kms_conn_details, prefix="csikmsconn")

    def cleanup(self):
        """
        Cleanup for KMIP

        """
        self.update_kmip_env_vars()

        # Retrieve OSD and NooBaa keys for deletion and delete
        key_ids = self.get_osd_key_ids()
        key_ids.append(self.get_noobaa_key_id())
        if not key_ids:
            logger.warning("No keys found to be deleted from CipherTrust")

        else:
            for key in key_ids:
                self.delete_ciphertrust_key(key)
            logger.info("Keys deleted from CipherTrust Manager")


class AzureKV(KMS):
    """
    Represents an Azure Key Vault implementation of KMS.
    """

    def __init__(self, namespace=config.ENV_DATA["cluster_namespace"]):
        super().__init__(constants.AZURE_KV_PROVIDER_NAME)
        self.namespace = namespace
        self.kms_provider = constants.AZURE_KV_PROVIDER_NAME
        self.azure_kms_connection_name = (
            f"azure-kv-conn-{gen_alphanumeric(length=5).lower()}"
        )
        azure_auth = config.AUTH.get("azure_auth")
        self.azure_kv_name = azure_auth.get("AZURE_KV_NAME")
        self.azure_kv_certificate = azure_auth.get("AZURE_CERTIFICATE")
        self.vault_url = azure_auth.get("AZURE_KV_URL")
        self.vault_client_id = azure_auth.get("AZURE_KV_CLIENT_ID")
        self.vault_tenant_id = azure_auth.get("AZURE_KV_TENANT_ID")
        self.vault_cert_path = self._azure_kv_cert_path()

        self.conn_data = {
            "KMS_PROVIDER": self.kms_provider,
            "KMS_SERVICE_NAME": self.azure_kms_connection_name,
            "AZURE_CLIENT_ID": self.vault_client_id,
            "AZURE_VAULT_URL": self.vault_url,
            "AZURE_TENANT_ID": self.vault_tenant_id,
        }

    def deploy(self):
        """
        This Function will create the Azure KV connection details in the ConfigMap.
        """
        if not config.ENV_DATA.get("platform") == "azure":
            raise VaultDeploymentError(
                "Azure_KV deployment only supports on Azure platform."
            )

        self.create_azure_kv_csi_kms_connection_details()
        if config.ENV_DATA.get("encryption_at_rest"):
            self.create_azure_kv_ocs_csi_kms_connection_details()

    def post_deploy_verification(self):
        """
        Post Deploy Verification For Azure Key Vault.
        """
        if config.ENV_DATA.get("encryption_at_rest"):
            if not self.verify_osd_keys_present_on_azure_kv():
                raise ValueError("OSD keys Not present on Azure Key Vault.")
            logger.info("OSD Keys Are present on Azure Key Vault.")

    def is_azure_kv_connection_exists(self):
        """
        Checks if the Azure KV connection exists in the ConfigMap
        """

        csi_kms_configmap = ocp.OCP(
            kind=constants.CONFIGMAP,
            resource_name=constants.VAULT_KMS_CSI_CONNECTION_DETAILS,
            namespace=self.namespace,
        )

        if not csi_kms_configmap.is_exist():
            raise ValueError(
                f"ConfigMap {csi_kms_configmap.resource_name} Not found in the namespace {self.namespace}"
            )

        if self.azure_kms_connection_name not in csi_kms_configmap.data["data"]:
            raise ValueError(
                f"Azure Key vault connection {self.azure_kms_connection_name} not exists."
            )

    def create_azure_kv_secrets(self, prefix="azure-ocs-"):
        """
        Creates Azure KV secrets.
        """
        secret_name = gen_alphanumeric(length=18, start=prefix).lower()
        client_secret = templating.load_yaml(constants.AZURE_CLIENT_SECRETS)

        client_secret["metadata"]["name"] = secret_name
        client_secret["metadata"]["namespace"] = self.namespace
        client_secret["data"]["CLIENT_CERT"] = base64.b64encode(
            self.azure_kv_certificate.encode()
        ).decode()
        logger.info(f"Creating a Azure Secret : {secret_name}")
        self.create_resource(client_secret, prefix=prefix)
        return secret_name

    def create_azure_kv_csi_kms_connection_details(self):
        """
        Create Azure specific csi-kms-connection-details
        configmap resource
        """

        # Check is already configmap exists
        csi_kms_configmap = ocp.OCP(
            kind=constants.CONFIGMAP,
            resource_name=constants.AZURE_KV_CSI_CONNECTION_DETAILS,
            namespace=self.namespace,
        )

        # Create a Connection data.
        azure_conn = self.conn_data
        azure_conn["AZURE_CERT_SECRET_NAME"] = self.create_azure_kv_secrets(
            prefix="azure-csi-"
        )

        if not csi_kms_configmap.is_exist():
            logger.info(
                f"Creating Configmap {constants.AZURE_KV_CSI_CONNECTION_DETAILS}"
            )

            csi_kms_conn_details = templating.load_yaml(
                constants.AZURE_CSI_KMS_CONNECTION_DETAILS
            )

            # Updating Templet data.
            csi_kms_conn_details["data"] = {
                self.azure_kms_connection_name: json.dumps(azure_conn)
            }

            csi_kms_conn_details["metadata"]["namespace"] = self.namespace
            self.create_resource(csi_kms_conn_details, prefix="csiazureconn")
        else:
            # Append the connection details to existing ConfigMap.
            logger.info(
                f"Adding Azure connection to existing ConfigMap {constants.AZURE_KV_CSI_CONNECTION_DETAILS}"
            )

            param = json.dumps(
                [
                    {
                        "op": "add",
                        "path": f"/data/{self.azure_kms_connection_name}",
                        "value": json.dumps(azure_conn),
                    }
                ]
            )

            csi_kms_configmap.patch(params=param, format_type="json")

        # verifying ConfigMap is created or not.
        self.is_azure_kv_connection_exists()

    def create_azure_kv_ocs_csi_kms_connection_details(self):
        """
        Creates Azure KV OCS CSI KMS connection details ConfigMap.
        """

        # Creating ConfigMap for OCS CSI KMS connection details.
        azure_data = self.conn_data
        azure_data["AZURE_CERT_SECRET_NAME"] = self.create_azure_kv_secrets(
            prefix="azure-ocs-"
        )

        # loading ConfigMap template
        ocs_kms_conn_details = templating.load_yaml(
            constants.AZURE_OCS_KMS_CONNECTION_DETAILS
        )
        ocs_kms_conn_details["metadata"]["namespace"] = self.namespace
        ocs_kms_conn_details["data"] = azure_data

        # creating ConfigMap Rsource
        logger.info(
            f"creating ConfigMap resource for {constants.AZURE_KV_CONNECTION_DETAILS_RESOURCE}"
        )
        self.create_resource(ocs_kms_conn_details, prefix="ocsazureconn")

        # Verify ConfigMap is created or not.
        ocs_kms_configmap = ocp.OCP(
            kind=constants.CONFIGMAP,
            resource_name=constants.AZURE_KV_CONNECTION_DETAILS_RESOURCE,
            namespace=self.namespace,
        )

        if not ocs_kms_configmap.is_exist():
            raise ValueError(
                f"ConfigMap Resource {constants.AZURE_KV_CONNECTION_DETAILS_RESOURCE}"
                f" is not created in namespace {self.namespace}"
            )

        logger.info(
            f"Successfully Created configmap {constants.AZURE_KV_CONNECTION_DETAILS_RESOURCE} "
            f"in {self.namespace} namespace"
        )

    def _azure_kv_cert_path(self):
        """
        Create a temporary certificate file and write the Azure Key Vault certificate to it.
        """
        try:
            temp_dir = tempfile.mkdtemp()
            cert_file = os.path.join(temp_dir, "certificate.pem")

            with open(cert_file, "w") as fd:
                fd.write(self.azure_kv_certificate)

            return cert_file
        except Exception as ex:
            raise ValueError(f"Error Creating Azure certificate file : {ex}")

    def azure_kv_secrets(self):
        """
        List the secrets in the Azure Key Vault.
        """
        try:
            # Create a CertificateCredential using the certificate
            credential = CertificateCredential(
                vault_url=self.vault_url,
                tenant_id=self.vault_tenant_id,
                client_id=self.vault_client_id,
                certificate_path=self.vault_cert_path,
            )

            # Create a SecretClient using the certificate for authentication
            secret_client = SecretClient(
                vault_url=self.vault_url, credential=credential
            )

            # Get the list of secrets
            secrets = secret_client.list_properties_of_secrets()

            # Extract and return the list of secret names
            secret_names = [secret.name for secret in secrets]
            return secret_names

        except AzureError as az_error:
            print(f"AzureError occurred: {az_error.message}")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def azure_kv_osd_keys(self):
        """
        List of OSD keys found in Azure Key Vault
        """
        azure_kv_secrets = self.azure_kv_secrets()
        deviceset = [pvc.name for pvc in get_deviceset_pvcs()]

        found_osd_keys = [
            kv_secret
            for kv_secret in azure_kv_secrets
            if [dev for dev in deviceset if dev in kv_secret]
        ]

        logger.info(f"OSD Keys on Azure KV: {found_osd_keys}")

        return found_osd_keys

    def verify_osd_keys_present_on_azure_kv(self):
        """
        Verify if all OSD keys are present in Azure Key Vault
        """

        osd_keys = self.azure_kv_osd_keys()
        deviceset = [pvc.name for pvc in get_deviceset_pvcs()]

        if len(osd_keys) != len(deviceset):
            logger.info("Not all OSD keys present in the Azure KV")
            return False

        logger.info("All OSD keys are present in the Azure KV ")
        return True

    def remove_kmsid(self):
        """
        Removing azure kmsid from the configmap `csi-kms-connection-details`.

        Returns:
            bool: True if KMS ID is successfully removed, otherwise False.
        """
        if not self.is_azure_kv_connection_exists():
            logger.info(
                f"There is no KMS connection {self.azure_kms_connection_name} available in the configmap"
            )
            return False

        csi_kms_configmap = ocp.OCP(
            kind=constants.CONFIGMAP,
            resource_name=constants.VAULT_KMS_CSI_CONNECTION_DETAILS,
            namespace=self.namespace,
        )

        if len(get_encryption_kmsid()) <= 1:
            # removing configmap csi-kms-connection-details.
            csi_kms_configmap.delete()
        else:
            params = json.dumps(
                [{"op": "remove", "path": f"/data/{self.azure_kms_connection_name}"}]
            )
            csi_kms_configmap.patch(params=params, format_type="json")
        return True

    def verify_pv_secrets_present_in_azure_kv(self, vol_handle):
        """
        Verify Azure KV has the secrets for given volume handle.

        Returns:
            bool: True if PV secrets are found in the Azure KV, otherwise False.
        """
        secrets = self.azure_kv_secrets()
        if vol_handle in secrets:
            logger.info(f"PV sceret for {vol_handle} is found in the Azure KV.")
            return True

        logger.info(f"PV secret for {vol_handle} not found in the Azure KV.")
        return False


kms_map = {"vault": Vault, "hpcs": HPCS, "kmip": KMIP, "azure-kv": AzureKV}


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
        namespace=config.ENV_DATA["cluster_namespace"],
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
    if not config.ENV_DATA.get("encryption_at_rest"):
        raise KMSNotSupported(
            "Encryption should be enabled for KMS Deployments. "
            "Choose vault config file from https://github.com/red-hat-storage/ocs-ci/tree/master/conf/ocsci"
        )
    try:
        return kms_map[provider]()
    except KeyError:
        raise KMSNotSupported(f"Not a supported KMS deployment , provider: {provider}")


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
        namespace=config.ENV_DATA["cluster_namespace"],
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
        f"patch -n {config.ENV_DATA['cluster_namespace']} cm "
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
        f"oc delete sa {constants.RBD_CSI_VAULT_TOKEN_REVIEWER_NAME} -n {config.ENV_DATA['cluster_namespace']}"
    )
    run_cmd(f"oc delete ClusterRole {constants.RBD_CSI_VAULT_TOKEN_REVIEWER_NAME}")
    run_cmd(
        f"oc delete ClusterRoleBinding {constants.RBD_CSI_VAULT_TOKEN_REVIEWER_NAME}"
    )


def get_ksctl_cli(bin_dir=None):
    """
    Download ksctl to interact with CipherTrust Manager via CLI

    Args:
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])

    """

    bin_dir = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
    system = platform.system()
    if "Darwin" not in system and "Linux" not in system:
        raise UnsupportedOSType("Not a supported platform")

    system = system.lower()
    zip_file = "ksctl_images.zip"
    ksctl_cli_filename = "ksctl"
    ksctl_binary_path = os.path.join(bin_dir, ksctl_cli_filename)
    if os.path.isfile(ksctl_binary_path):
        logger.info(
            f"ksctl CLI binary already exists {ksctl_binary_path}, skipping download."
        )
    else:
        logger.info("Downloading ksctl cli")
        prepare_bin_dir()
        url = f"https://{load_auth_config()['kmip']['KMIP_ENDPOINT']}/downloads/{zip_file}"
        download_file(url, zip_file, verify=False)
        run_cmd(f"unzip -d {bin_dir} {zip_file}")
        run_cmd(f"mv {bin_dir}/ksctl-{system}-amd64 {bin_dir}/{ksctl_cli_filename}")
        delete_file(zip_file)

    ksctl_ver = run_cmd(f"{ksctl_binary_path} version")
    logger.info(f"ksctl cli version: {ksctl_ver}")


def get_kms_endpoint():
    """
    Fetch VAULT_ADDR or KMIP_ENDPOINT if kmip provider from
    ocs-kms-connection-details configmap.

    Returns:
        str: KMS endpoint address

    """
    ocs_kms_configmap = ocp.OCP(
        kind="ConfigMap",
        resource_name=constants.VAULT_KMS_CONNECTION_DETAILS_RESOURCE,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    if ocs_kms_configmap.get().get("data").get("KMS_PROVIDER") == "kmip":
        return ocs_kms_configmap.get().get("data")["KMIP_ENDPOINT"]
    else:
        return ocs_kms_configmap.get().get("data")["VAULT_ADDR"]


def set_kms_endpoint(address):
    """
    Set VAULT_ADDR or KMIP_ENDPOINT if kmip provider in
    ocs-kms-connection-details configmap to provided value

    Args:
        address (str): Address to be set in KMS configuration

    """
    ocs_kms_configmap = ocp.OCP(
        kind="ConfigMap",
        resource_name=constants.VAULT_KMS_CONNECTION_DETAILS_RESOURCE,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    if ocs_kms_configmap.get().get("data").get("KMS_PROVIDER") == "kmip":
        addr_attribute = "KMIP_ENDPOINT"
    else:
        addr_attribute = "VAULT_ADDR"
    params = f'{{"data": {{"{addr_attribute}": "{address}"}}}}'
    ocs_kms_configmap.patch(params=params, format_type="merge")
    return ocs_kms_configmap.get().get("data")[addr_attribute]
