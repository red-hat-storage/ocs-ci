"""
This module contains helpers functions needed for
external cluster deployment.
"""

import logging
import tempfile

from ocs_ci.framework import config
from ocs_ci.ocs import defaults
from ocs_ci.ocs.exceptions import (
    ExternalClusterExporterRunFailed,
    ExternalClusterRGWEndPointMissing,
)
from ocs_ci.ocs.resources.packagemanifest import (
    PackageManifest,
    get_selector_for_ocs_operator,
)
from ocs_ci.utility.connection import Connection
from ocs_ci.utility.utils import upload_file, encode, decode

logger = logging.getLogger(__name__)


class ExternalCluster(object):
    """
    Helper for External RHCS cluster
    """

    def __init__(self, host, user, password):
        """
        Initialize the variables required for external RHCS cluster

        Args:
             host (str): Host name with FQDN or IP
             user (str): User name
             password (password): Password for the Host

        """
        self.host = host
        self.user = user
        self.password = password
        self.rhcs_conn = Connection(
            host=self.host, user=self.user, password=self.password
        )

    def get_external_cluster_details(self):
        """
        Gets the external RHCS cluster details and updates to config.EXTERNAL_MODE

        Raises:
            ExternalClusterExporterRunFailed: If exporter script failed to run on external RHCS cluster

        """
        # upload exporter script to external RHCS cluster
        script_path = self.upload_exporter_script()

        # get rgw endpoint port
        rgw_endpoint_port = self.get_rgw_endpoint_api_port()

        # get rgw endpoint
        rgw_endpoint = get_rgw_endpoint()
        rgw_endpoint_with_port = f"{rgw_endpoint}:{rgw_endpoint_port}"

        # run the exporter script on external RHCS cluster
        cmd = f"python {script_path} --rbd-data-pool-name {defaults.RBD_NAME} --rgw-endpoint {rgw_endpoint_with_port}"
        retcode, out, err = self.rhcs_conn.exec_cmd(cmd)
        if retcode != 0:
            logger.error(f"Failed to run {script_path}. Error: {err}")
            raise ExternalClusterExporterRunFailed

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
        script_path = generate_exporter_script()
        upload_file(self.host, script_path, script_path, self.user, self.password)
        return script_path

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
        Fetches rgw endpoint api port

        Returns:
            str: RGW endpoint port

        """
        cmd = "ceph dashboard get-rgw-api-port"
        _, out, _ = self.rhcs_conn.exec_cmd(cmd)
        logger.info(f"External cluster rgw endpoint api port: {out}")
        return out


def generate_exporter_script():
    """
    Generates exporter script for RHCS cluster

    Returns:
        str: path to the exporter script

    """
    # generate exporter script through packagemanifest
    ocs_operator_name = defaults.OCS_OPERATOR_NAME
    operator_selector = get_selector_for_ocs_operator()
    package_manifest = PackageManifest(
        resource_name=ocs_operator_name,
        selector=operator_selector,
    )
    ocs_operator_data = package_manifest.get()
    encoded_script = ocs_operator_data["status"]["channels"][0]["currentCSVDesc"][
        "annotations"
    ]["external.features.ocs.openshift.io/export-script"]

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


def get_rgw_endpoint():
    """
    Fetches rgw endpoint

    Returns:
        str: rgw endpoint

    Raises:
        ExternalClusterRGWEndPointMissing: in case of rgw endpoint missing

    """
    rgw_endpoint = None
    for each in config.EXTERNAL_MODE["external_cluster_node_roles"].values():
        if "rgw" in each["role"]:
            rgw_endpoint = each["ip_address"]
            return rgw_endpoint
    if not rgw_endpoint:
        raise ExternalClusterRGWEndPointMissing
