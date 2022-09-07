import base64
import json
import logging
import os
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.helpers import helpers
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
        kind="StorageConsumer", namespace=defaults.ROOK_CLUSTER_NAMESPACE
    )
    consumer_yamls = consumer.get().get("items")
    return [consumer["metadata"]["name"] for consumer in consumer_yamls]


def patch_consumer_toolbox(ceph_admin_key=None):
    """
    Patch the rook-ceph-tools deployment with ceph.admin key. Applicable for MS platform only to enable rook-ceph-tools
    to run ceph commands.

    Args:
        ceph_admin_key (str): The ceph admin key which should be used to patch rook-ceph-tools deployment on consumer

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
        return

    consumer_tools_pod = get_ceph_tools_pod()

    # Check whether ceph command is working on tools pod. Patch is needed only if the error is "RADOS permission error"
    try:
        consumer_tools_pod.exec_ceph_cmd("ceph health")
        return
    except Exception as exc:
        if "RADOS permission error" not in str(exc):
            logger.warning(
                f"Ceph command on rook-ceph-tools deployment is failing with error {str(exc)}. "
                "This error cannot be fixed by patching the rook-ceph-tools deployment with ceph admin key."
            )
            return

    consumer_tools_deployment = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
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
        return

    # Wait for the existing tools pod to delete
    consumer_tools_pod.ocp.wait_for_delete(resource_name=consumer_tools_pod.name)

    # Wait for the new tools pod to reach Running state
    new_tools_pod_info = get_pods_having_label(
        label=constants.TOOL_APP_LABEL,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
    )[0]
    new_tools_pod = Pod(**new_tools_pod_info)
    helpers.wait_for_resource_state(new_tools_pod, constants.STATUS_RUNNING)


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
