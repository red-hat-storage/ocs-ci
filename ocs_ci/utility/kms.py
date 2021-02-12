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
import base64

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    VaultDeploymentError,
    VaultOperationError,
    KMSNotSupported,
    KMSConnectionDetailsError,
    KMSTokenError,
    KMSResourceCleaneupError,
)
from ocs_ci.utility import templating
from ocs_ci.utility.utils import (
    load_auth_config,
    run_cmd,
    get_vault_cli,
)

logger = logging.getLogger(__name__)


class KMS(object):
    """
    This is base class for any KMS integration

    """

    def __init__(self, provider=None):
        self.kms_provider = provider

    def deploy(self):
        raise NotImplementedError()


class Vault(KMS):
    """
    A class which handles deployment and other
    configs related to vault

    """

    def __init__(self):
        super().__init__("vault")
        self.vault_server = None
        self.port = None
        # Name of kubernetes resources
        # for ca_cert, client_cert, client_key
        self.ca_cert_name = None
        self.client_cert_name = None
        self.client_key_name = None
        self.vault_root_token = None
        self.vault_namespace = None
        self.vault_deploy_mode = config.ENV_DATA.get("vault_deploy_mode")
        self.vault_backend_path = None
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

    def update_vault_env_vars(self):
        """
        In order to run vault CLI we need following env vars
        VAULT_ADDR and VAULT_TOKEN

        """
        os.environ["VAULT_ADDR"] = f"https://{self.vault_server}:{self.port}"
        os.environ["VAULT_TOKEN"] = self.vault_root_token

    def create_ocs_vault_resources(self):
        """
        This function takes care of creating ocp resources for
        secrets like ca cert, client cert, client key and vault token
        Assumption is vault section in AUTH file contains base64 encoded
        (with padding) ca, client certs, client key and vault path token

        """
        if not config.ENV_DATA.get("VAULT_SKIP_VERIFY"):
            # create ca cert secret
            ca_data = templating.load_yaml(constants.EXTERNAL_VAULT_CA_CERT)
            self.ca_cert_name = config.ENV_DATA.get(
                "VAULT_CACERT", constants.VAULT_DEFAULT_CA_CERT
            )
            ca_data["metadata"]["name"] = self.ca_cert_name
            ca_data["data"]["cert"] = self.ca_cert_base64
            self.create_resource(ca_data, prefix="ca")

            # create client cert secret
            client_cert_data = templating.load_yaml(
                constants.EXTERNAL_VAULT_CLIENT_CERT
            )
            self.client_cert_name = config.ENV_DATA.get(
                "VAULT_CLIENT_CERT", constants.VAULT_DEFAULT_CLIENT_CERT
            )
            client_cert_data["metadata"]["name"] = self.client_cert_name
            client_cert_data["data"]["cert"] = self.client_cert_base64
            self.create_resource(client_cert_data, prefix="clientcert")

            # create client key secert
            client_key_data = templating.load_yaml(constants.EXTERNAL_VAULT_CLIENT_KEY)
            self.client_key_name = config.ENV_DATA.get(
                "VAULT_CLIENT_KEY", constants.VAULT_DEFAULT_CLIENT_KEY
            )
            self.client_key_name["metadata"]["name"] = self.client_key_name
            client_key_data["data"]["key"] = self.client_key_base64
            self.create_resource(client_key_data, prefix="clientkey")

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
        self.vault_namespace = config.ENV_DATA.get(
            "VAULT_NAMESPACE", constants.VAULT_DEFAULT_NAMESPACE
        )
        connection_data["data"]["VAULT_NAMESPACE"] = self.vault_namespace
        connection_data["data"]["VAULT_TLS_SERVER_NAME"] = self.vault_tls_server
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

    def vault_create_backend_path(self):
        """
        create vault path to be used by OCS

        Raises:
            VaultOperationError exception
        """
        if config.ENV_DATA.get("VAULT_BACKEND_PATH"):
            self.vault_backend_path = config.ENV_DATA.get("VAULT_BACKEND_PATH")
        else:
            # Generate backend path name using prefix "ocs"
            # "ocs-<cluster-id>"
            self.cluster_id = self.get_cluster_id()
            self.vault_backend_path = (
                f"{constants.VAULT_DEFAULT_PATH_PREFIX}-{self.cluster_id}"
            )
        cmd = f"vault secrets enable -path={self.vault_backend_path} kv"
        out = subprocess.check_output(shlex.split(cmd))
        if "Success" in out.decode():
            logger.info(f"vault path {self.vault_backend_path} created")
        else:
            raise VaultOperationError(
                f"Failed to create path f{self.vault_backend_path}"
            )
        self.vault_create_policy()

    def vault_create_policy(self):
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

        if not config.ENV_DATA.get("VAULT_POLICY"):
            self.vault_policy_name = (
                f"{constants.VAULT_DEFAULT_POLICY_PREFIX}-" f"{self.cluster_id}"
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

    def get_cluster_id(self):
        """
        Get cluster UUID
        Not relying on metadata.json as user sometimes want to run
        only with kubeconfig for some tests

        Returns:
            str: cluster UUID

        """
        cluster_id = run_cmd(
            "oc get clusterversion version -o jsonpath='{.spec.clusterID}'"
        )
        return cluster_id

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
            connection_details = ocp.OCP(
                kind="ConfigMap",
                resource_name=constants.VAULT_KMS_CONNECTION_DETAILS_RESOURCE,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            try:
                self.vault_backend_path = connection_details.get().get("data")[
                    "VAULT_BACKEND_PATH"
                ]
            except IndexError:
                raise KMSConnectionDetailsError("KMS connection details not available")

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
            try:
                token = vault_token.get().get()["data"]["token"]
                self.vault_path_token = base64.b64decode(token).decode()
            except IndexError:
                raise KMSTokenError("Couldn't find KMS token")

    def get_vault_policy(self):
        """
        Get the policy name based on token from vault

        """
        if not self.vault_policy_name:
            cmd = f"vault token lookup {self.vault_path_token}"
            out = subprocess.check_output(shlex.split(cmd))
            json_out = json.loads(out)
            for policy in json_out["data"]["policies"]:
                if self.cluster_id in policy:
                    self.vault_policy_name = policy

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

    def cleanup(self):
        """
        Cleanup the backend resources in case of external

        """
        if not self.vault_server:
            self.gather_init_vault_conf()
        # TODO:
        # get vault path
        self.get_vault_backend_path()
        # from token secret get token
        self.get_vault_path_token()
        # from token get policy
        if not self.cluster_id:
            self.cluster_id = self.get_cluster_id()
        self.get_vault_policy()
        # Delete the policy and backend path from vault
        # we need root token of vault in the env
        self.update_vault_env_vars()
        self.remove_vault_backend_path()
        self.remove_vault_policy()


kms_map = {"vault": Vault}


def get_kms_deployment():
    provider = config.DEPLOYMENT["kms_provider"]
    try:
        return kms_map[provider]()
    except KeyError:
        raise KMSNotSupported("Not a supported KMS deployment")
