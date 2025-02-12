"""
This module contains helpers functions needed for
ODF deployment.
"""

import ipaddress
import logging
import os
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.resources.pod import get_operator_pods, delete_pods
from ocs_ci.utility import templating, version
from ocs_ci.utility.deployment import (
    get_ocp_release_image_from_running_cluster,
    get_coredns_container_image,
)
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


def get_required_csvs():
    """
    Get the mandatory CSVs needed for the ODF cluster

    Returns:
        list: list of CSVs needed

    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    ocs_operator_names = [
        defaults.ODF_CSI_ADDONS_OPERATOR,
        defaults.ODF_OPERATOR_NAME,
        defaults.OCS_OPERATOR_NAME,
        defaults.MCG_OPERATOR,
    ]
    if ocs_version >= version.VERSION_4_16:
        operators_4_16_additions = [
            defaults.ROOK_CEPH_OPERATOR,
            defaults.ODF_PROMETHEUS_OPERATOR,
            defaults.ODF_CLIENT_OPERATOR,
            defaults.RECIPE_OPERATOR,
        ]
        ocs_operator_names.extend(operators_4_16_additions)
    if ocs_version >= version.VERSION_4_17:
        operators_4_17_additions = [defaults.CEPHCSI_OPERATOR]
        ocs_operator_names.extend(operators_4_17_additions)
    if ocs_version >= version.VERSION_4_18:
        operators_4_18_additions = [defaults.ODF_DEPENDENCIES]
        ocs_operator_names.extend(operators_4_18_additions)
    return ocs_operator_names


def configure_virtual_host_style_acess_for_rgw():
    """
    Enable access buckets with DNS subdomain style (Virtual host style) for RGW
    """
    if config.ENV_DATA.get("platform") not in constants.ON_PREM_PLATFORMS:
        logger.info(
            "Skipping configuration of access buckets with DNS subdomain style (Virtual host style) for RGW "
            f"because {config.ENV_DATA.get('platform')} platform is not between {constants.ON_PREM_PLATFORMS}"
        )
        return

    odf_dns_depl = ocp.OCP(
        kind=constants.DEPLOYMENT,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name="odf-dns",
    )
    if odf_dns_depl.get(dont_raise=True, silent=True):
        logger.info(
            "Deployment odf-dns already exists, configuration for access buckets with "
            "DNS subdomain style (Virtual host style) for RGW is already applied."
        )
        return

    logger.info(
        "Configuring access buckets with DNS subdomain style (Virtual host style) for RGW"
    )

    release_image = get_ocp_release_image_from_running_cluster()
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    coredns_image = get_coredns_container_image(release_image, pull_secret_path)
    coredns_deployment = templating.load_yaml(constants.COREDNS_DEPLOYMENT_YAML)
    coredns_deployment["spec"]["template"]["spec"]["containers"][0][
        "image"
    ] = coredns_image
    coredns_deployment_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="coredns_deployment", suffix=".yaml", delete=False
    )
    templating.dump_data_to_temp_yaml(coredns_deployment, coredns_deployment_yaml.name)

    logger.info("Creating ConfigMap for CoreDNS")
    exec_cmd(f"oc create -f {constants.COREDNS_CONFIGMAP_YAML}")
    logger.info("Creating CoreDNS Deployment")
    exec_cmd(f"oc create -f {coredns_deployment_yaml.name}")
    logger.info("Creating CoreDNS Service")
    exec_cmd(f"oc create -f {constants.COREDNS_SERVICE_YAML}")
    # get dns ip
    dns_ip = exec_cmd(
        f"oc get -n {config.ENV_DATA['cluster_namespace']} svc odf-dns -ojsonpath={{..clusterIP}}"
    ).stdout.decode()
    try:
        ipaddress.IPv4Address(dns_ip)
    except ipaddress.AddressValueError:
        logger.error("Failed to obtain IP of odf-dns Service")
        raise
    logger.info(
        f"Patching dns.operator/default to forward 'data.local' zone to {dns_ip}:53 (odf-dns Service)"
    )
    exec_cmd(
        "oc patch dns.operator/default --type=merge --patch '"
        '{"spec":{"servers":[{"forwardPlugin":{"upstreams":["'
        f"{dns_ip}:53"
        '"]},"name":"rook-dns","zones":["data.local"]'
        "}]}}'"
    )
    logger.info("Patching storagecluster/ocs-storagecluster to allow virtualHostnames")
    exec_cmd(
        "oc patch -n openshift-storage storagecluster/ocs-storagecluster --type=merge --patch '"
        '{"spec":{"managedResources":{"cephObjectStores":{"virtualHostnames":'
        '["rgw.data.local"]'
        "}}}}'"
    )
    # Restart rook-ceph-operator pod, not sure if this is required step or just workaround
    logger.info("Restarting rook-ceph-operator pod")
    rook_ceph_operator_pods = get_operator_pods()
    delete_pods(rook_ceph_operator_pods, wait=True)
    # wait for rook-ceph-operator pod starts
    pod_obj = ocp.OCP(
        kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
    )
    pod_obj.wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=constants.OPERATOR_LABEL,
        timeout=300,
        sleep=5,
    )
    logger.info("Pod rook-ceph-operator were successfully restarted")
