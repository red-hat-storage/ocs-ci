import base64
import json
import logging
import os
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.ocs.constants import (
    MS_CONSUMER_TYPE,
    MS_PROVIDER_TYPE,
    NON_MS_CLUSTER_TYPE,
    HCI_PROVIDER,
    HCI_CLIENT,
)
from ocs_ci.ocs.resources.catalog_source import CatalogSource, disable_specific_source
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_pods_having_label, Pod
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd, run_cmd


logger = logging.getLogger(__name__)


def get_pagerduty_secret_name():
    """
    Get name of the PagerDuty secret for currently used addon.

    Returns:
        string: name of the secret
    """
    return config.ENV_DATA["addon_name"] + constants.MANAGED_PAGERDUTY_SECRET_SUFFIX


def get_smtp_secret_name():
    """
    Get name of the SMTP secret for currently used addon.

    Returns:
        string: name of the secret
    """
    return config.ENV_DATA["addon_name"] + constants.MANAGED_SMTP_SECRET_SUFFIX


def get_dms_secret_name():
    """
    Get name of the Dead Man's Snitch secret for currently used addon.

    Returns:
        string: name of the secret
    """
    return (
        config.ENV_DATA["addon_name"] + constants.MANAGED_DEADMANSSNITCH_SECRET_SUFFIX
    )


def get_parameters_secret_name():
    """
    Get name of the addon parameters secret for currently used addon.

    Returns:
        string: name of the secret
    """
    return (
        constants.MANAGED_PARAMETERS_SECRET_PREFIX
        + config.ENV_DATA["addon_name"]
        + constants.MANAGED_PARAMETERS_SECRET_SUFFIX
    )


def update_pull_secret():
    """
    Update pull secret with extra quay.io/rhceph-dev credentials.

    Note: This is a hack done to allow odf to odf deployment before full addon is available.
    """
    oc = ocp.OCP(kind=constants.SECRET, namespace="openshift-config")
    logger.info("Update pull secret")
    pull_secret = oc.exec_oc_cmd("get -n openshift-config secret/pull-secret -o yaml")
    secret_data = pull_secret["data"][".dockerconfigjson"]
    secret_data = base64.b64decode(secret_data).decode()
    rhceph_dev_key = config.AUTH["quay-rhceph-dev-auth"]
    secret_data = json.loads(secret_data)
    secret_data["auths"]["quay.io"] = {"auth": rhceph_dev_key}
    secret_data = str.encode(json.dumps(secret_data))
    with tempfile.NamedTemporaryFile() as secret_file:
        secret_file.write(secret_data)
        secret_file.flush()
        exec_cmd(
            f"oc set data secret/pull-secret -n openshift-config --from-file=.dockerconfigjson={secret_file.name}"
        )


def get_consumer_names():
    """
    Get the names of all consumers connected to this provider cluster.
    Runs on provider cluster

    Returns:
        list: names of all connected consumers, empty list if there are none
    """
    consumer = ocp.OCP(
        kind="StorageConsumer", namespace=config.ENV_DATA["cluster_namespace"]
    )
    consumer_yamls = consumer.get().get("items")
    return [consumer["metadata"]["name"] for consumer in consumer_yamls]


def patch_consumer_toolbox(ceph_admin_key=None, consumer_tools_pod=None):
    """
    Patch the rook-ceph-tools deployment with ceph.admin key. Applicable for MS platform only to enable rook-ceph-tools
    to run ceph commands.

    Args:
        ceph_admin_key (str): The ceph admin key which should be used to patch rook-ceph-tools deployment on consumer
        consumer_tools_pod (OCS): The rook-ceph-tools pod object.

    Returns:
        OCS: The new pod object after patching the rook-ceph-tools deployment. If it fails to patch, it returns None.

    """

    # Get the admin key if available
    ceph_admin_key = (
        ceph_admin_key
        or os.environ.get("CEPHADMINKEY")
        or config.AUTH.get("external", {}).get("ceph_admin_key")
        or get_admin_key_from_provider()
    )

    if not ceph_admin_key:
        logger.warning(
            "Ceph admin key not found to patch rook-ceph-tools deployment on consumer with ceph.admin key. "
            "Skipping the step."
        )
        return None

    if not consumer_tools_pod:
        consumer_tools_pod = get_ceph_tools_pod()

    # Check whether ceph command is working on tools pod. Patch is needed only if the error is "RADOS permission error"
    try:
        consumer_tools_pod.exec_ceph_cmd("ceph health")
        return consumer_tools_pod
    except Exception as exc:
        if not is_rados_connect_error_in_ex(exc):
            logger.warning(
                f"Ceph command on rook-ceph-tools deployment is failing with error {str(exc)}. "
                "This error cannot be fixed by patching the rook-ceph-tools deployment with ceph admin key."
            )
            return None

    consumer_tools_deployment = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name="rook-ceph-tools",
    )
    patch_value = (
        f'[{{"op": "replace", "path": "/spec/template/spec/containers/0/env", '
        f'"value":[{{"name": "ROOK_CEPH_USERNAME", "value": "client.admin"}}, '
        f'{{"name": "ROOK_CEPH_SECRET", "value": "{ceph_admin_key}"}}]}}]'
    )
    try:
        consumer_tools_deployment.patch(params=patch_value, format_type="json")
    except Exception as exe:
        logger.warning(
            "Failed to patch rook-ceph-tools deployment in consumer cluster. "
            f"The patch can be applied manually after deployment. Error {str(exe)}"
        )
        return None

    # Wait for the existing tools pod to delete
    consumer_tools_pod.ocp.wait_for_delete(resource_name=consumer_tools_pod.name)

    # Wait for the new tools pod to reach Running state
    new_tools_pod_info = get_pods_having_label(
        label=constants.TOOL_APP_LABEL,
        namespace=config.ENV_DATA["cluster_namespace"],
    )[0]
    new_tools_pod = Pod(**new_tools_pod_info)
    helpers.wait_for_resource_state(new_tools_pod, constants.STATUS_RUNNING)
    return new_tools_pod


def update_non_ga_version():
    """
    Update pull secret, catalog source, subscription and operators to consume
    ODF and deployer versions provided in configuration.

    """
    deployer_version = config.UPGRADE["deployer_version"]
    upgrade_ocs_version = config.UPGRADE["upgrade_ocs_version"]
    logger.info(f"Starting update to next version of deployer: {deployer_version}")
    logger.info("Update catalogsource")
    disable_specific_source(constants.OPERATOR_CATALOG_SOURCE_NAME)
    catalog_source_data = templating.load_yaml(constants.CATALOG_SOURCE_YAML)
    catalog_source_data["spec"]["image"] = config.DEPLOYMENT["ocs_registry_image"]
    catalog_source_manifest = tempfile.NamedTemporaryFile(
        mode="w+", prefix="catalog_source_manifest", delete=False
    )
    templating.dump_data_to_temp_yaml(catalog_source_data, catalog_source_manifest.name)
    run_cmd(f"oc apply -f {catalog_source_manifest.name}", timeout=2400)
    catalog_source = CatalogSource(
        resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    logger.info("Edit annotation on the deployer CSV")
    run_cmd(
        f"oc annotate csv --overwrite ocs-osd-deployer.v{deployer_version} "
        'operatorframework.io/properties=\'{"properties":[{"type":"olm.package",'
        '"value":{"packageName":"ocs-osd-deployer","version":'
        f'"{deployer_version}"'
        '}},{"type":"olm.gvk","value":{"group":"ocs.openshift.io","kind":'
        '"ManagedOCS","version":"v1alpha1"}},{"type":"olm.package.required",'
        '"value":{"packageName":"ose-prometheus-operator","versionRange":"4.10.0"}},'
        '{"type":"olm.package.required","value":{"packageName":"odf-operator",'
        f'"versionRange":"{upgrade_ocs_version}"'
        "}}]}' -n openshift-storage"
    )
    # Wait for catalog source is ready
    catalog_source.wait_for_state("READY")
    ocs_channel = config.UPGRADE["ocs_channel"]
    odf_operator_u = f"odf-operator.v{upgrade_ocs_version}"
    mplace = constants.MARKETPLACE_NAMESPACE

    logger.info("Edit subscriptions")
    oc = ocp.OCP(
        kind=constants.SUBSCRIPTION,
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    subscriptions = oc.get()["items"]
    if config.ENV_DATA.get("cluster_type").lower() == "provider":
        subscriptions_to_edit = {"odf-operator"}
        patch_changes = [
            f'[{{"op": "replace", "path": "/spec/channel", "value" : "{ocs_channel}"}}]',
            f'[{{"op": "replace", "path": "/spec/startingCSV", "value" : "{odf_operator_u}"}}]',
        ]
    elif config.ENV_DATA.get("cluster_type").lower() == "consumer":
        subscriptions_to_edit = {
            "ocs-operator",
            "odf-operator",
            "mcg-operator",
            "odf-csi-addons-operator",
        }
        patch_changes = [
            f'[{{"op": "replace", "path": "/spec/channel", "value" : "{ocs_channel}"}}]',
            f'[{{"op": "replace", "path": "/spec/sourceNamespace", "value" : "{mplace}"}}]',
            f'[{{"op": "replace", "path": "/spec/startingCSV", "value" : "{odf_operator_u}"}}]',
        ]
    for subscription in subscriptions:
        for to_edit in subscriptions_to_edit:
            sub = (
                subscription.get("metadata").get("name")
                if subscription.get("metadata").get("name").startswith(to_edit)
                else ""
            )
            if sub:
                for change in patch_changes:
                    oc.patch(
                        resource_name=sub,
                        params=change,
                        format_type="json",
                    )


def get_admin_key_from_provider():
    """
    Get admin key from rook-ceph-tools pod on provider

    Returns:
        str: The admin key obtained from rook-ceph-tools pod on provider.
            Return empty string if admin key is not obtained.

    """
    initial_cluster_index = config.cur_index
    config.switch_to_provider()
    admin_key = ""
    try:
        # Get the key from provider cluster rook-ceph-tools pod
        provider_tools_pod = get_ceph_tools_pod()
        admin_key = (
            provider_tools_pod.exec_cmd_on_pod("grep key /etc/ceph/keyring")
            .strip()
            .split()[-1]
        )
    except Exception as exc:
        logger.error(
            f"Couldn't find admin key from provider due to the error:\n{str(exc)}"
        )
    finally:
        config.switch_ctx(initial_cluster_index)
        return admin_key


def check_default_cluster_context_index_equal_to_current_index():
    """
    Check that the default cluster index is equal to the current cluster index

    Returns:
        bool: True, if the default cluster index is equal to the current cluster index

    """
    default_index = config.ENV_DATA["default_cluster_context_index"]
    logger.info(
        f"default cluster index = {default_index}, current cluster index = {config.cur_index}"
    )

    if default_index != config.cur_index:
        logger.warning(
            "The default cluster index is different from the current cluster index"
        )
        return False
    else:
        logger.info("The default cluster index is equal to the current cluster index")
        return True


def change_current_index_to_default_index():
    """
    Change the current cluster index to the default cluster index

    """
    default_index = config.ENV_DATA["default_cluster_context_index"]
    logger.info("Change the current cluster index to the default cluster index")
    config.switch_ctx(default_index)


def check_and_change_current_index_to_default_index():
    """
    Check that the default cluster index was equal to the current cluster index, and also change
    the current cluster index to the default cluster index if they are not equal.

    Returns:
        bool: True, if the default cluster index was equal to the current cluster index

    """
    is_equal = check_default_cluster_context_index_equal_to_current_index()
    if not is_equal:
        change_current_index_to_default_index()

    return is_equal


def get_managedocs_component_state(component):
    """
    Get the state of the given managedocs component:
    alertmanager, prometheus or storageCluster.

    Args:
        component (str): the component of managedocs resource
    Returns:
        str: the state of the component
    """
    managedocs_obj = ocp.OCP(
        kind="managedocs",
        resource_name="managedocs",
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    return managedocs_obj.get()["status"]["components"][component]["state"]


def is_rados_connect_error_in_ex(ex):
    """
    Check if the RADOS connect error is found in the exception

    Args:
        ex (Exception): The exception to check if the RADOS connect error is found

    Returns:
        bool: True, if the RADOS connect error is found in the exception. False otherwise

    """
    rados_errors = ("RADOS permission error", "RADOS I/O error")
    return any([rados_error in str(ex) for rados_error in rados_errors])


def check_switch_to_correct_cluster_at_setup(cluster_type=None):
    """
    Check that we switch to the correct cluster type at setup according to the 'cluster_type'
    parameter provided

    Args:
        cluster_type (str): The cluster type

    Raises:
        AssertionError: In case of switching to the wrong cluster type.

    """
    # Import here to avoid circular loop
    from ocs_ci.ocs.cluster import (
        is_ms_consumer_cluster,
        is_ms_provider_cluster,
        is_managed_service_cluster,
        is_hci_client_cluster,
        is_hci_provider_cluster,
    )

    logger.info(f"The cluster type is: {cluster_type}")
    if not cluster_type:
        assert check_default_cluster_context_index_equal_to_current_index(), (
            "The default cluster ctx index should be equal to the current index, if we don't pass "
            "the cluster type param "
        )
        return

    valid_cluster_types = [
        MS_CONSUMER_TYPE,
        MS_PROVIDER_TYPE,
        NON_MS_CLUSTER_TYPE,
        HCI_PROVIDER,
        HCI_CLIENT,
    ]
    assert (
        cluster_type in valid_cluster_types
    ), f"The cluster type {cluster_type} does not appear in the correct cluster types {valid_cluster_types}"

    if cluster_type == MS_CONSUMER_TYPE:
        assert is_ms_consumer_cluster(), "The cluster is not an MS consumer cluster"
        logger.info("The cluster is an MS consumer cluster as expected")
    elif cluster_type == MS_PROVIDER_TYPE and (
        config.ENV_DATA["platform"].lower()
        not in constants.HCI_PROVIDER_CLIENT_PLATFORMS
    ):
        # MS_PROVIDER_TYPE and HCI_PROVIDER are both "provider"
        assert is_ms_provider_cluster(), "The cluster is not an MS provider cluster"
        logger.info("The cluster is an MS provider cluster as expected")
    elif cluster_type == HCI_CLIENT:
        assert is_hci_client_cluster(), "The cluster is not an HCI client cluster"
        logger.info("The cluster is an HCI client cluster as expected")
    elif cluster_type == HCI_PROVIDER:
        assert is_hci_provider_cluster(), "The cluster is not an HCI provider cluster"
        logger.info("The cluster is an HCI provider cluster as expected")
    elif cluster_type == NON_MS_CLUSTER_TYPE:
        assert (
            not is_managed_service_cluster()
        ), "The cluster is a Managed Service cluster"
        logger.info("The cluster is not a Managed Service cluster as expected")


def get_provider_service_type():
    """
    Get the type of the ocs-provider-server Service(e.g., NodePort, LoadBalancer)

    Returns:
        str: The type of the ocs-provider-server Service

    """
    service_obj = ocp.OCP(
        kind=constants.SERVICE,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.MANAGED_PROVIDER_SERVER_SECRET,
    )
    service_type = service_obj.get().get("spec").get("type")
    logger.info(f"The type of the ocs-provider-server Service = {service_type}")
    return service_type
