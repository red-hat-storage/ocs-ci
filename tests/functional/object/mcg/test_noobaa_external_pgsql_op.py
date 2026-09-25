import base64
import logging
from urllib.parse import urlparse

import yaml

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    red_squad,
    mcg,
    skipif_managed_service,
    skipif_noobaa_external_pgsql_not_set,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_pods_having_label, get_pod_logs
from ocs_ci.ocs.resources.storage_cluster import get_noobaa_external_pgsql_secret_name

logger = logging.getLogger(__name__)


def search_for_sensitive_values(text, sensitive_values):
    """
    Search a blob of text for sensitive values.

    Args:
        text (str): The text to search in
        sensitive_values (dict): Mapping of a human readable label (e.g. "password")
            to its sensitive value

    Returns:
        list: Labels of the sensitive values found in the text. The sensitive
            values themselves are never returned or logged, to avoid leaking them
            into the test logs.

    """
    if not text:
        return []
    return [
        label for label, value in sensitive_values.items() if value and value in text
    ]


@red_squad
@mcg
@tier2
@skipif_managed_service
@skipif_noobaa_external_pgsql_not_set
class TestNoobaaExternalPgsqlOp(MCGTest):
    """
    Verify that the external PostgreSQL credentials configured for NooBaa are not
    leaked in plaintext anywhere on the cluster (operator/pod logs, events, the
    NooBaa CR, or ConfigMaps). The credential is expected to live ONLY in the
    dedicated (base64-encoded) Secret.
    """

    def test_noobaa_external_pgsql_credential_not_leaked(self):
        """
        Steps:
            1. NooBaa is configured with external PostgreSQL credentials (deployment).
            2. Inspect the PG Secret - the credential must be base64-encoded in
               ``data`` and must NOT appear in plaintext in the manifest.
            3. Search the NooBaa operator logs for the password / connection string.
            4. Search the namespace events for the password / connection string.
            5. Search the NooBaa core and endpoint pod logs.
            6. Verify the NooBaa CR references the Secret by name and does not embed
               the credential inline.
            7. Verify no ConfigMap in the namespace carries the credential.
            8. Search every source above for the full connection-string (db_url) form.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        logger.info(
            "NooBaa is configured with external PostgreSQL credentials "
            f"(namespace: {namespace})"
        )

        # Step 2: inspect the PG Secret.
        logger.info("Inspecting the external PostgreSQL Secret")
        pg_secret_name = get_noobaa_external_pgsql_secret_name()
        assert (
            pg_secret_name == constants.NOOBAA_POSTGRES_SECRET
        ), f"Unexpected external PostgreSQL secret name: {pg_secret_name}"
        secret_obj = OCP(
            kind=constants.SECRET,
            namespace=namespace,
            resource_name=pg_secret_name,
        )
        secret_data = secret_obj.get()
        db_url_b64 = secret_data.get("data", {}).get("db_url")
        assert (
            db_url_b64
        ), f"External PostgreSQL secret '{pg_secret_name}' is missing 'db_url'"
        db_url = base64.b64decode(db_url_b64).decode("utf-8")

        password = urlparse(db_url).password
        assert (
            password
        ), "Could not parse the password from the external PostgreSQL db_url"

        sensitive_values = {"password": password, "db_url": db_url}

        # The Secret manifest itself must not carry the credential in plaintext -
        # only the base64-encoded ``data`` field is allowed to hold it.
        secret_manifest = yaml.safe_dump(secret_data)
        assert not search_for_sensitive_values(
            secret_manifest, sensitive_values
        ), f"Secret '{pg_secret_name}' exposes the credential in plaintext"
        logger.info(
            f"Secret '{pg_secret_name}' holds the credential base64-encoded "
            "only; no plaintext exposure in the manifest"
        )

        # Collect (source_name -> text) for every place the credential could leak
        sources = {}

        # Steps 3 & 5: operator, core and endpoint pod logs
        logger.info("Collecting NooBaa operator, core and endpoint pod logs")
        pod_log_labels = {
            "noobaa-operator logs": constants.NOOBAA_OPERATOR_POD_LABEL,
            "noobaa-core logs": constants.NOOBAA_CORE_POD_LABEL,
            "noobaa-endpoint logs": constants.NOOBAA_ENDPOINT_POD_LABEL,
        }
        for source_name, label in pod_log_labels.items():
            pods = get_pods_having_label(label, namespace=namespace)
            for pod in pods:
                pod_name = pod["metadata"]["name"]
                sources[f"{source_name} ({pod_name})"] = get_pod_logs(
                    pod_name, namespace=namespace, all_containers=True
                )
        logger.info("Collected logs from NooBaa operator, core and endpoint pods")

        # Step 4: namespace events
        logger.info(f"Collecting events from namespace {namespace}")
        events = OCP(kind="Event", namespace=namespace).get()
        sources["events"] = yaml.safe_dump(events)

        # Step 6: the NooBaa CR must reference the Secret by name, not embed inline
        logger.info(
            "Verifying the NooBaa CR references the Secret and does not "
            "embed the credential inline"
        )
        noobaa_cr = OCP(kind=constants.NOOBAA_RESOURCE_NAME, namespace=namespace).get(
            resource_name=constants.NOOBAA_RESOURCE_NAME
        )
        external_pg = (
            noobaa_cr.get("spec", {}).get("externalPgConfig", {})
            if isinstance(noobaa_cr, dict)
            else {}
        )
        assert external_pg.get("dbURL") is None, (
            "NooBaa CR embeds the external PostgreSQL connection string inline "
            "instead of referencing the Secret"
        )
        sources["noobaa CR"] = yaml.safe_dump(noobaa_cr)
        logger.info("NooBaa CR does not embed the credential inline")

        # Step 7: no ConfigMap in the namespace may carry the credential
        logger.info(f"Collecting ConfigMaps from namespace {namespace}")
        configmaps = OCP(kind=constants.CONFIGMAP, namespace=namespace).get()
        for cm in configmaps.get("items", []):
            cm_name = cm["metadata"]["name"]
            sources[f"configmap/{cm_name}"] = yaml.safe_dump(cm)

        # Steps 3-8: search every collected source for the sensitive values
        logger.info(
            "Searching all collected sources for the password and the "
            f"full connection-string (db_url) form ({len(sources)} sources)"
        )
        leaks = {}
        for source_name, text in sources.items():
            found = search_for_sensitive_values(text, sensitive_values)
            if found:
                leaks[source_name] = found

        assert not leaks, (
            "External PostgreSQL credential leaked in plaintext. "
            f"Affected sources and leaked value types: {leaks}"
        )
        logger.info(
            f"No external PostgreSQL credential leak found across {len(sources)} "
            "sources (operator/core/endpoint logs, events, NooBaa CR, ConfigMaps)"
        )
