import logging
from time import sleep
import requests

from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import (
    storagecluster_independent_check,
    create_unique_resource_name,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler, run_cmd, exec_cmd
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError, CommandFailed

logger = logging.getLogger(__name__)


class QuayOperator(object):
    """
    Quay operator class

    """

    def __init__(self):
        """
        Quay operator initializer function

        """
        self.namespace = constants.OPENSHIFT_OPERATORS
        self.ocp_obj = ocp.OCP(namespace=self.namespace)
        self.quay_operator = None
        self.quay_registry = None
        self.quay_registry_secret = None
        self.quay_pod_obj = OCP(kind=constants.POD, namespace=self.namespace)
        self.quay_registry_name = ""
        self.quay_operator_csv = ""
        self.quay_registry_secret_name = ""
        self.sc_default = False
        self.sc_name = (
            constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            if storagecluster_independent_check()
            else constants.DEFAULT_STORAGECLASS_RBD
        )

    def get_quay_default_channel(self):
        """
        Retrieves the default channel of the Quay operator from the
        PackageManifest in the openshift-marketplace namespace.

        Returns:
            str: The default channel of the Quay operator.

        """
        package_manifest_obj = OCP(
            kind="PackageManifest", namespace=constants.MARKETPLACE_NAMESPACE
        )
        quay_package_manifest = package_manifest_obj.get(
            resource_name=constants.QUAY_OPERATOR
        )
        default_channel = quay_package_manifest.get("status").get("defaultChannel")
        return default_channel

    def setup_quay_operator(self, channel=None):
        """
        Deploys the Quay operator using the specified channel or the default channel if not provided

        Args:
            channel (str, optional): The channel of the Quay operator to deploy. If not provided, the default channel
                will be used.

        Raises:
            TimeoutError: If the Quay operator pod fails to reach the 'Running' state
            within the timeout.

        """
        channel = channel if channel else self.get_quay_default_channel()
        quay_operator_data_dict = templating.load_yaml(file=constants.QUAY_SUB)
        quay_operator_data_dict["spec"]["channel"] = channel
        self.quay_operator = OCS(**quay_operator_data_dict)
        logger.info(f"Installing Quay operator: {self.quay_operator.name}")
        self.quay_operator.create()
        for quay_pod in TimeoutSampler(
            300, 10, get_pod_name_by_pattern, constants.QUAY_OPERATOR, self.namespace
        ):
            if quay_pod:
                self.quay_pod_obj.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    resource_name=quay_pod[0],
                    sleep=30,
                    timeout=600,
                )
                break
        self.quay_operator_csv = get_csvs_start_with_prefix(
            csv_prefix=constants.QUAY_OPERATOR,
            namespace=self.namespace,
        )[0]["metadata"]["name"]

    def create_quay_registry(self):
        """
        Creates Quay registry

        """
        if not helpers.get_default_storage_class():
            patch = ' \'{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}\' '
            run_cmd(
                f"oc patch storageclass {self.sc_name} "
                f"-p {patch} "
                f"--request-timeout=120s"
            )
            self.sc_default = True
        self.quay_registry_secret_name = create_unique_resource_name(
            "quay-user", "secret"
        )
        logger.info(
            f"Creating Quay registry config for super-user access: {self.quay_registry_secret_name}"
        )
        self.quay_registry_secret = self.ocp_obj.exec_oc_cmd(
            command=f"create secret generic --from-file config.yaml={constants.QUAY_SUPER_USER} "
            f"{self.quay_registry_secret_name}"
        )
        quay_registry_data = templating.load_yaml(file=constants.QUAY_REGISTRY)
        self.quay_registry_name = quay_registry_data["metadata"]["name"]
        quay_registry_data["spec"][
            "configBundleSecret"
        ] = self.quay_registry_secret_name
        self.quay_registry = OCS(**quay_registry_data)
        logger.info(f"Creating Quay registry: {self.quay_registry.name}")
        self.quay_registry.create()
        logger.info("Waiting for 15s for registry to get initialized")
        sleep(15)
        self.wait_for_quay_endpoint()

    def wait_for_quay_endpoint(self):
        """
        Waits for quay registry endpoint

        """
        logger.info("Waiting for quay registry endpoint to be up")
        sample = TimeoutSampler(
            timeout=300,
            sleep=15,
            func=self.check_quay_registry_endpoint,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutExpiredError("Quay registry endpoint did not get created.")
        else:
            logger.info("Quay registry endpoint is up")

    def check_quay_registry_endpoint(self):
        """
        Checks if quay registry endpoint is up

        Returns:
            bool: True if quay endpoint is up else False

        """
        return (
            True
            if self.quay_registry.get().get("status").get("registryEndpoint")
            else False
        )

    def get_quay_endpoint(self):
        """
        Returns quay endpoint

        """
        return self.quay_registry.get().get("status").get("registryEndpoint")

    def teardown(self):
        """
        Quay operator teardown

        """
        if self.sc_default:
            patch = ' \'{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}\' '
            run_cmd(
                f"oc patch storageclass {self.sc_name} "
                f"-p {patch} "
                f"--request-timeout=120s"
            )
        if self.quay_registry_secret:
            self.ocp_obj.exec_oc_cmd(f"delete secret {self.quay_registry_secret_name}")
        if self.quay_registry:
            self.quay_registry.delete()
        if self.quay_operator:
            self.quay_operator.delete()
        if self.quay_operator_csv:
            self.ocp_obj.exec_oc_cmd(
                f"delete {constants.CLUSTER_SERVICE_VERSION} "
                f"{self.quay_operator_csv}"
            )


def get_super_user_token(endpoint):
    """
    Gets the initialized super user token.
    This is one time, cant get the token again once initialized.

    Args:
        endpoint (str): Quay Endpoint url

    Returns:
        str: Super user token
    """
    data = (
        f'{{"username": "{constants.QUAY_SUPERUSER}", "password": "{constants.QUAY_PW}", '
        f'"email": "quayadmin@example.com", "access_token": true}}'
    )
    r = requests.post(
        f"{endpoint}/{constants.QUAY_USER_INIT}",
        headers={"content-type": "application/json"},
        data=data,
        verify=False,
    )
    if r.status_code != 200:
        raise Exception(
            f"Error code [{r.status_code}]: Failed to fetch super user token!!"
        )
    return r.json()["access_token"]


def check_super_user(endpoint, token):
    """
    Validates the super user based on the token

    Args:
        endpoint (str): Quay Endpoint url
        token (str): Super user token

    Returns:
        bool: True in case token is from a super user
    """
    r = requests.get(
        f"{endpoint}/{constants.QUAY_USER_GET}",
        headers={
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        verify=False,
    )
    return True if r.json()["users"][0]["super_user"] else False


def create_quay_org(endpoint, token, org_name):
    """
    Creates an organization in quay

    Args:
        endpoint (str): Quay endpoint url
        token (str): Super user token
        org_name (str): Organization name

    Returns:
        bool: True in case org creation is successful
    """
    data = f'{{"recaptcha_response": "string", "name": "{org_name}", "email": "{org_name}@test.com"}}'
    r = requests.post(
        f"{endpoint}/{constants.QUAY_ORG_POST}",
        headers={
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        data=data,
        verify=False,
    )
    return True if "Created" in r.json() else False


def create_quay_repository(
    endpoint, token, repo_name, org_name, description="new_repo"
):
    """
    Creates a quay repository

    Args:
        endpoint (str): Quay Endpoint url
        token (str): Super user token
        org_name (str): Organization name
        repo_name (str): Repository name
        description (str): Description of the repo

    Returns:
        bool: True in case repo creation is successful
    """
    data = (
        f'{{"namespace": "{org_name}", "repository": "{repo_name}", '
        f'"description":"{description}", "visibility": "public"}}'
    )
    r = requests.post(
        f"{endpoint}/{constants.QUAY_REPO_POST}",
        headers={
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        data=data,
        verify=False,
    )
    return True if "Created" in r.json() else False


def delete_quay_repository(endpoint, token, org_name, repo_name):
    """
    Deletes the quay repository

    Args:
        endpoint (str): Quay Endpoint url
        token (str): Super user token
        org_name (str): Organization name
        repo_name (str): Repository name

    Returns:
        bool: True in case repo is delete successfully
    """
    r = requests.delete(
        f"{endpoint}/{constants.QUAY_REPO_POST}/{org_name}/{repo_name}",
        headers={
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        verify=False,
    )
    return True if "204" in str(r.status_code) else False


@retry(CommandFailed, tries=10, delay=5, backoff=1)
def quay_super_user_login(endpoint_url):
    """
    Logins in to quay endpoint

    Args:
        endpoint_url (str): External endpoint of quay
    """
    exec_cmd(
        f"podman login {endpoint_url} -u {constants.QUAY_SUPERUSER} -p {constants.QUAY_PW} --tls-verify=false"
    )
