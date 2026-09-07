"""
Automated coverage for RHSTOR-8718 - "Support Changes in Target Endpoint for
BackingStores (and NamespaceStores)".

The feature adds the CLI command::

    noobaa connection update --old-endpoint <OLD> --new-endpoint <NEW>

which performs an endpoint-scoped, all-or-nothing bulk update: it matches every
BackingStore (and NamespaceStore) whose spec endpoint == OLD, pre-validates each
one against NEW, and only then patches the specs and updates the underlying
NooBaa-core connections (rolling back automatically on any failure).

The coverage is organised into three test classes:

  * ``TestBackingStoreEndpointUpdate`` - positive / core behaviour: single-store
    switch and revert, bulk switch with connection de-duplication, endpoint
    matching semantics (no-match, idempotent no-op, selective matching),
    switching under active I/O, and the default backingstore.
  * ``TestConnectionUpdateNegative`` - failure paths: pre-validation aborts the
    whole batch (unreachable endpoint, non-NooBaa bucket, bad credentials, one
    bad store in a batch), CLI argument handling, and the direct-CR-edit path.
  * ``TestNamespaceStoreEndpointUpdate`` - NamespaceStore endpoint update and
    the mixed BackingStore + NamespaceStore batch (regression guards for
    DFBUGS-10744 / DFBUGS-10743).

Deferred scenarios that need infrastructure not available in a plain MCG job
(core-connection fault injection, ODF upgrade, provider mode) are present as
skipped placeholders so the coverage matrix stays complete.

ENVIRONMENT PREREQUISITES
-------------------------
Most tests need TWO reachable S3 endpoints and a secret with credentials valid
on both.  Prefer a *same-backend* endpoint pair (two routes / LB VIPs fronting
the SAME bucket, e.g. two RGW endpoints) so data-path continuity can be asserted
without the two-backend "stranding" simulation artifact described in the test
plan.  Provide them via ``config.ENV_DATA``::

    ENV_DATA:
      mcg_endpoint_pair:
        old: https://<old-endpoint>
        new: https://<new-endpoint>
        target_bucket: <bucket-that-is-a-valid-noobaa-location-on-both>
        secret: <k8s-secret-with-AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY>
        signature_version: v4        # v2 for plain-http endpoints

Tests requiring this configuration skip cleanly when it is absent.
"""

import logging
import re

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    tier1,
    tier2,
    tier3,
    jira,
)
from ocs_ci.helpers.helpers import create_resource, create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

NOOBAA_API_VERSION = "noobaa.io/v1alpha1"


# ---------------------------------------------------------------------------
# CLI helper + output parser (reusable by every test)
# ---------------------------------------------------------------------------
def run_connection_update(
    mcg_obj, old_endpoint, new_endpoint, namespace=None, extra_args=""
):
    """
    Run ``noobaa connection update`` via the MCG CLI (odf-cli / mcg-cli, resolved
    automatically by :meth:`MCG.exec_mcg_cmd`) and return a parsed result.

    Args:
        mcg_obj (MCG): The MCG fixture object.
        old_endpoint (str): Value passed to ``--old-endpoint``.
        new_endpoint (str): Value passed to ``--new-endpoint``.
        namespace (str): Namespace override (defaults to the MCG namespace).
        extra_args (str): Any additional raw CLI arguments.

    Returns:
        dict: Parsed output, see :func:`parse_connection_update_output`.
    """
    cmd = (
        f"connection update --old-endpoint {old_endpoint} "
        f"--new-endpoint {new_endpoint} {extra_args}"
    ).strip()
    logger.info(f"Running MCG CLI: {cmd}")
    result = mcg_obj.exec_mcg_cmd(cmd, namespace=namespace, ignore_error=True)
    parsed = parse_connection_update_output(result)
    summary = {k: v for k, v in parsed.items() if k != "raw"}
    logger.info(f"connection update parsed result: {summary}")
    return parsed


def _search_int(pattern, text):
    match = re.search(pattern, text)
    return int(match.group(1)) if match else None


def parse_connection_update_output(result):
    """
    Parse the ``connection update`` output into a structured dict.

    The CLI logs to both stdout and stderr (logrus), so both are combined.

    Returns:
        dict: {
            "raw": str,                    # combined stdout+stderr
            "returncode": int | None,
            "matched": int | None,         # "Found N store(s) matching endpoint"
            "stores_updated": int | None,  # "Stores updated: N"
            "connections_updated": int | None,
            "unique_connections": int | None,  # "Found N unique connection(s)"
            "prevalidation_passed": bool,
            "prevalidation_failed": bool,
            "aborted": bool,               # "No changes have been made"
            "no_match": bool,              # "no matching stores"
            "webhook_denied": bool,
            "status_codes": list[str],     # e.g. ["INVALID_ENDPOINT"]
        }
    """
    out = "\n".join(
        [
            (getattr(result, "stdout", "") or ""),
            (getattr(result, "stderr", "") or ""),
        ]
    )
    return {
        "raw": out,
        "returncode": getattr(result, "returncode", None),
        "matched": _search_int(r"Found (\d+) store", out),
        "stores_updated": _search_int(r"Stores updated:\s*(\d+)", out),
        "connections_updated": _search_int(r"Connections updated:\s*(\d+)", out),
        "unique_connections": _search_int(r"Found (\d+) unique connection", out),
        "prevalidation_passed": "passed pre-validation" in out,
        "prevalidation_failed": "Pre-validation failed" in out,
        "aborted": "No changes have been made" in out,
        "no_match": "no matching stores" in out.lower(),
        "webhook_denied": (
            "admissionwebhook.noobaa.io" in out and "denied the request" in out
        ),
        "status_codes": re.findall(r"status=([A-Z_]+)", out),
    }


# ---------------------------------------------------------------------------
# Resource helpers
# ---------------------------------------------------------------------------
def _store_dict(
    kind, name, namespace, endpoint, target_bucket, secret_name, signature_version
):
    """Build an s3-compatible BackingStore/NamespaceStore CR dict."""
    return {
        "apiVersion": NOOBAA_API_VERSION,
        "kind": kind,
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "type": "s3-compatible",
            "s3Compatible": {
                "endpoint": endpoint,
                "targetBucket": target_bucket,
                "signatureVersion": signature_version,
                "secret": {"name": secret_name, "namespace": namespace},
            },
        },
    }


def get_store_endpoint(kind, name, namespace):
    """Return the current spec endpoint of a BackingStore/NamespaceStore."""
    obj = OCP(kind=kind, namespace=namespace, resource_name=name).get()
    return obj["spec"]["s3Compatible"]["endpoint"]


def get_store_pause_annotation(kind, name, namespace):
    """Return the value of the noobaa.io/pause-reconcile annotation (or None)."""
    annotations = (
        OCP(kind=kind, namespace=namespace, resource_name=name)
        .get()
        .get("metadata", {})
        .get("annotations", {})
    )
    return annotations.get("noobaa.io/pause-reconcile")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def endpoint_conf():
    """
    Return the configured endpoint pair / bucket / secret, or skip.

    See the module docstring for the expected ``ENV_DATA['mcg_endpoint_pair']``
    structure.
    """
    conf = config.ENV_DATA.get("mcg_endpoint_pair")
    required = ("old", "new", "target_bucket", "secret")
    if not conf or any(not conf.get(k) for k in required):
        pytest.skip(
            "Endpoint-update tests require ENV_DATA['mcg_endpoint_pair'] "
            "with keys: old, new, target_bucket, secret."
        )
    conf.setdefault("signature_version", "v4")
    return conf


@pytest.fixture
def store_factory(request):
    """
    Factory that creates s3-compatible BackingStore / NamespaceStore CRs on a
    given endpoint, waits for Ready, and cleans them up afterwards.

    Usage::

        bs = store_factory(constants.BACKINGSTORE, endpoint, bucket, secret)
    """
    namespace = config.ENV_DATA["cluster_namespace"]
    created = []

    def _factory(
        kind,
        endpoint,
        target_bucket,
        secret_name,
        signature_version="v4",
        name=None,
        wait=True,
    ):
        name = name or create_unique_resource_name("eps", kind.lower())
        body = _store_dict(
            kind,
            name,
            namespace,
            endpoint,
            target_bucket,
            secret_name,
            signature_version,
        )
        ocs_obj = create_resource(**body)
        created.append((kind, name))
        if wait:
            OCP(kind=kind, namespace=namespace, resource_name=name).wait_for_resource(
                condition=constants.STATUS_READY,
                column="PHASE",
                timeout=300,
                sleep=10,
            )
        return ocs_obj

    yield _factory

    for kind, name in reversed(created):
        try:
            store_ocp = OCP(kind=kind, namespace=namespace)
            # A paused NamespaceStore (DFBUGS-10743) must have the annotation
            # removed before it can be reconciled/deleted cleanly.
            if get_store_pause_annotation(kind, name, namespace):
                store_ocp.annotate(
                    annotation="noobaa.io/pause-reconcile-", resource_name=name
                )
            store_ocp.delete(resource_name=name, wait=True)
        except Exception as ex:  # noqa - best-effort teardown
            logger.warning(f"Teardown of {kind}/{name} failed: {ex}")


@pytest.fixture
def bad_creds_secret(request):
    """Create a secret carrying invalid S3 credentials (for the wrong-credentials case)."""
    namespace = config.ENV_DATA["cluster_namespace"]
    name = create_unique_resource_name("eps-badcreds", "secret")
    body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace},
        "type": "Opaque",
        "stringData": {
            "AWS_ACCESS_KEY_ID": "not-a-real-access-key",
            "AWS_SECRET_ACCESS_KEY": "not-a-real-secret-key",
        },
    }
    create_resource(**body)

    def _finalizer():
        try:
            OCP(kind="Secret", namespace=namespace).delete(
                resource_name=name, wait=False
            )
        except Exception as ex:  # noqa
            logger.warning(f"Teardown of secret {name} failed: {ex}")

    request.addfinalizer(_finalizer)
    return name


# ===========================================================================
# Module A - positive / core behaviour
# ===========================================================================
@mcg
@red_squad
@jira("RHSTOR-8718")
class TestBackingStoreEndpointUpdate:
    """Happy-path and matching-semantics coverage for the endpoint update CLI."""

    @tier1
    # TODO: assign polarion id
    def test_switch_and_revert_single_bs(self, mcg_obj, endpoint_conf, store_factory):
        """
        Switch a single s3-compatible BackingStore's endpoint from OLD to NEW,
        then revert it back.

        Flow:
            1. Create a BackingStore on the OLD endpoint and wait for Ready.
            2. Run the connection update OLD -> NEW and assert exactly one store
               was updated, its spec endpoint now points at NEW, and the
               transient pause-reconcile annotation was cleaned up.
            3. Run the reverse update NEW -> OLD and assert the store is switched
               back to its original endpoint.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        bs = store_factory(
            constants.BACKINGSTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )

        # --- switch OLD -> NEW ---
        result = run_connection_update(mcg_obj, old, new)
        assert not result["aborted"], f"Update aborted unexpectedly:\n{result['raw']}"
        assert (
            result["stores_updated"] == 1
        ), f"Expected exactly 1 store updated, got {result['stores_updated']}"
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new
        # No lingering pause annotation after a successful update
        assert not get_store_pause_annotation(
            constants.BACKINGSTORE, bs.name, ns
        ), "pause-reconcile annotation was not cleaned up after a successful switch"
        OCP(
            kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name
        ).wait_for_resource(
            condition=constants.STATUS_READY, column="PHASE", timeout=180
        )

        # --- revert NEW -> OLD ---
        revert = run_connection_update(mcg_obj, new, old)
        assert not revert["aborted"], f"Revert aborted:\n{revert['raw']}"
        assert revert["stores_updated"] == 1
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old

    @tier2
    # TODO: assign polarion id
    def test_bulk_switch_and_connection_dedup(
        self, mcg_obj, endpoint_conf, store_factory
    ):
        """
        Several BackingStores sharing the same endpoint switch together in a
        single command, and the underlying core connections are de-duplicated.

        Flow:
            1. Create multiple BackingStores on the OLD endpoint, all using the
               same credentials (identity).
            2. Run one connection update OLD -> NEW.
            3. Assert every store was updated and moved to NEW, while the
               connections are de-duplicated by (identity, endpoint) - stores
               that share an identity and endpoint collapse to a single unique
               connection.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        stores = [
            store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_bucket"],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            for _ in range(2)
        ]

        result = run_connection_update(mcg_obj, old, new)
        assert not result["aborted"], result["raw"]
        assert result["stores_updated"] == len(
            stores
        ), f"Expected {len(stores)} stores updated, got {result['stores_updated']}"
        # Same identity + endpoint -> a single unique connection.
        assert (result["unique_connections"] or result["connections_updated"]) == 1, (
            "Connections were not de-duplicated by (identity, endpoint): "
            f"{result['raw']}"
        )
        for bs in stores:
            assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new

    @tier2
    # TODO: assign polarion id
    @pytest.mark.parametrize(
        "variant",
        ["wrong_endpoint", "trailing_slash", "rerun_after_success"],
    )
    def test_no_match_endpoint_variants(
        self, mcg_obj, endpoint_conf, store_factory, variant
    ):
        """
        An old-endpoint string that matches no store must be rejected without
        touching anything (matching is on the exact endpoint string).

        Variants:
            * wrong_endpoint      - an endpoint value that no store uses.
            * trailing_slash      - the correct endpoint plus a trailing "/",
                                    which is not an exact match.
            * rerun_after_success - re-running OLD -> NEW after a successful
                                    switch, when no store sits on OLD any more.

        In every variant the command reports no matching stores, updates nothing,
        and leaves the store's endpoint unchanged.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        bs = store_factory(
            constants.BACKINGSTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )

        if variant == "wrong_endpoint":
            bad_old = "https://no-such-endpoint.example.invalid:9000"
            result = run_connection_update(mcg_obj, bad_old, new)
        elif variant == "trailing_slash":
            result = run_connection_update(mcg_obj, old.rstrip("/") + "/", new)
        else:  # rerun_after_success
            first = run_connection_update(mcg_obj, old, new)
            assert first["stores_updated"] == 1, first["raw"]
            result = run_connection_update(mcg_obj, old, new)

        assert (
            result["no_match"] or result["matched"] == 0
        ), f"Expected no matching stores for variant '{variant}':\n{result['raw']}"
        assert not result["stores_updated"], "No store should have been updated"
        # The store must be untouched (or on NEW only for the rerun variant).
        current = get_store_endpoint(constants.BACKINGSTORE, bs.name, ns)
        expected = new if variant == "rerun_after_success" else old
        assert current == expected

    @tier2
    # TODO: assign polarion id
    def test_only_old_endpoint_stores_matched(
        self, mcg_obj, endpoint_conf, store_factory
    ):
        """
        Only stores on the OLD endpoint are matched; a store already on the NEW
        endpoint is left untouched.

        Flow:
            1. Create one BackingStore on OLD and one already on NEW.
            2. Run the connection update OLD -> NEW.
            3. Assert exactly one store (the one on OLD) was updated, both stores
               now sit on NEW, and there was no conflict or re-update of the
               store that already used NEW.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        bs_old = store_factory(
            constants.BACKINGSTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )
        bs_new = store_factory(
            constants.BACKINGSTORE,
            new,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )

        result = run_connection_update(mcg_obj, old, new)
        assert (
            result["stores_updated"] == 1
        ), f"Only the OLD-endpoint store should be updated:\n{result['raw']}"
        assert get_store_endpoint(constants.BACKINGSTORE, bs_old.name, ns) == new
        assert get_store_endpoint(constants.BACKINGSTORE, bs_new.name, ns) == new

    @tier2
    # TODO: assign polarion id
    def test_idempotent_no_op(self, mcg_obj, endpoint_conf, store_factory):
        """
        Running the update with ``--new-endpoint`` equal to ``--old-endpoint``
        is a safe no-op.

        Flow:
            1. Create a BackingStore on the OLD endpoint.
            2. Run the connection update with new-endpoint == old-endpoint.
            3. Assert the store is matched and pre-validated, its endpoint is
               unchanged, and no pause-reconcile annotation is left behind.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old = endpoint_conf["old"]
        bs = store_factory(
            constants.BACKINGSTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )

        result = run_connection_update(mcg_obj, old, old)
        assert not result["aborted"], result["raw"]
        assert result["matched"] and result["matched"] >= 1
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old
        assert not get_store_pause_annotation(constants.BACKINGSTORE, bs.name, ns)

    @tier2
    # TODO: assign polarion id
    def test_switch_during_active_io(
        self,
        mcg_obj,
        endpoint_conf,
        store_factory,
        bucket_factory,
        awscli_pod,
        test_directory_setup,
    ):
        """
        Fire the endpoint switch while I/O is running through the store.

        Flow:
            1. Create a BackingStore on OLD, back it with a bucket/OBC and start
               a continuous write loop through it.
            2. Trigger the connection update OLD -> NEW while the loop runs.
            3. Assert the store ends up Ready on NEW with no lingering pause, and
               that I/O issued after the endpoint-config propagation window
               succeeds.

        NOTE: assert continuity only on post-switch I/O because of the
        endpoint-config propagation window; a same-backend endpoint pair avoids
        the two-backend stranding artifact.
        """
        pytest.skip(
            "Needs an OBC bound to the store under test plus a background I/O "
            "loop; wire this to the data-path harness (bucketclass -> OBC -> "
            "write/read) once the same-backend endpoint pair is available."
        )

    @tier2
    # TODO: assign polarion id
    def test_default_backingstore_endpoint_update(self, mcg_obj):
        """
        The default backingstore participates in endpoint update like any other
        store. Run as a SAFE idempotent same-endpoint update so live
        default-placement data is never moved.

        Flow:
            1. Read the default backingstore's type and endpoint; skip (N/A) if
               it has no endpoint (e.g. a PV-Pool default store).
            2. Run the connection update with new-endpoint == its current
               endpoint.
            3. Assert the default store is matched, its endpoint is unchanged,
               and it stays Ready.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        default = OCP(
            kind=constants.BACKINGSTORE,
            namespace=ns,
            resource_name=constants.DEFAULT_NOOBAA_BACKINGSTORE,
        ).get()
        spec = default.get("spec", {})
        store_type = spec.get("type")
        endpoint = spec.get("s3Compatible", {}).get("endpoint") or spec.get(
            "ibmCos", {}
        ).get("endpoint")
        if not endpoint:
            pytest.skip(
                f"Default backingstore type={store_type} has no endpoint "
                "(e.g. PV-Pool) - N/A for endpoint update."
            )

        result = run_connection_update(mcg_obj, endpoint, endpoint)
        assert (
            result["matched"] and result["matched"] >= 1
        ), f"Default backingstore was not matched:\n{result['raw']}"
        # Endpoint must be unchanged and the store still Ready.
        after = (
            get_store_endpoint(
                constants.BACKINGSTORE, constants.DEFAULT_NOOBAA_BACKINGSTORE, ns
            )
            if store_type == "s3-compatible"
            else endpoint
        )
        assert after == endpoint
        OCP(
            kind=constants.BACKINGSTORE,
            namespace=ns,
            resource_name=constants.DEFAULT_NOOBAA_BACKINGSTORE,
        ).wait_for_resource(
            condition=constants.STATUS_READY, column="PHASE", timeout=180
        )


# ===========================================================================
# Module B - pre-validation / all-or-nothing / input validation
# ===========================================================================
@mcg
@red_squad
@jira("RHSTOR-8718")
class TestConnectionUpdateNegative:
    """Failure paths: pre-validation aborts, argument handling, direct-edit."""

    @tier2
    # TODO: assign polarion id
    @pytest.mark.parametrize(
        "failure",
        ["unreachable", "not_noobaa_location", "wrong_creds", "one_bad_in_batch"],
    )
    def test_prevalidation_failure_aborts_batch(
        self, mcg_obj, endpoint_conf, store_factory, bad_creds_secret, failure
    ):
        """
        Any pre-validation failure aborts the WHOLE batch before any spec change
        - nothing is patched and no pause-reconcile annotation is left behind
        (all-or-nothing).

        Failure variants:
            * unreachable         - the NEW endpoint does not resolve/connect.
            * not_noobaa_location - the NEW bucket is not a valid NooBaa location.
            * wrong_creds         - the store's secret has invalid credentials.
            * one_bad_in_batch    - two stores are matched and one of them fails
                                    pre-validation.

        Each variant asserts the command aborts with the expected status code and
        that every matched store stays on OLD with no pause annotation.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]

        if failure == "unreachable":
            bs = store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_bucket"],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            target_new = "https://unreachable.example.invalid:9000"
            result = run_connection_update(mcg_obj, old, target_new)
            expected_status = "INVALID_ENDPOINT"
            stores = [bs]
        elif failure == "not_noobaa_location":
            bs = store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_bucket"],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            # Point the store at a NEW-endpoint bucket that is NOT a noobaa loc.
            bad_bucket = create_unique_resource_name("not-noobaa", "bucket")
            OCP(kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name).patch(
                params=(
                    f'{{"spec":{{"s3Compatible":{{"targetBucket":"{bad_bucket}"}}}}}}'
                ),
                format_type="merge",
            )
            result = run_connection_update(mcg_obj, old, new)
            expected_status = "INVALID_ENDPOINT"
            stores = [bs]
        elif failure == "wrong_creds":
            bs = store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_bucket"],
                bad_creds_secret,
                endpoint_conf["signature_version"],
                wait=False,
            )
            result = run_connection_update(mcg_obj, old, new)
            expected_status = "INVALID_CREDENTIALS"
            stores = [bs]
        else:  # one_bad_in_batch
            good = store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_bucket"],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            bad = store_factory(
                constants.BACKINGSTORE,
                old,
                create_unique_resource_name("not-noobaa", "bucket"),
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
                wait=False,
            )
            result = run_connection_update(mcg_obj, old, new)
            expected_status = "INVALID_ENDPOINT"
            stores = [good, bad]

        assert result["aborted"], (
            f"Expected the batch to abort ('No changes have been made'):\n"
            f"{result['raw']}"
        )
        assert result["prevalidation_failed"]
        assert (
            expected_status in result["status_codes"]
        ), f"Expected status {expected_status}, got {result['status_codes']}"
        assert not result["stores_updated"]
        # All-or-nothing: every store stays on OLD with no pause annotation.
        for bs in stores:
            assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old
            assert not get_store_pause_annotation(constants.BACKINGSTORE, bs.name, ns)

    @tier3
    # TODO: assign polarion id
    @pytest.mark.parametrize(
        "case",
        ["missing_flag", "whitespace_trimmed", "malformed_url"],
    )
    def test_cli_input_validation(self, mcg_obj, endpoint_conf, store_factory, case):
        """
        CLI argument handling for the connection update command.

        Cases:
            * missing_flag       - omitting --new-endpoint produces a usage error
                                   and changes nothing.
            * whitespace_trimmed - flags padded with surrounding spaces are
                                   trimmed and still match the store, so the
                                   switch succeeds.
            * malformed_url      - a bare / malformed URL is rejected with a clean
                                   abort and changes nothing.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        bs = store_factory(
            constants.BACKINGSTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )

        if case == "missing_flag":
            result = mcg_obj.exec_mcg_cmd(
                f"connection update --old-endpoint {old}", ignore_error=True
            )
            assert getattr(result, "returncode", 1) != 0
            assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old
        elif case == "whitespace_trimmed":
            # Pad both flags with surrounding spaces; the CLI must trim them.
            result = run_connection_update(mcg_obj, f'"  {old}  "', f'"  {new}  "')
            assert not result["aborted"], result["raw"]
            assert result["stores_updated"] == 1
            assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new
        else:  # malformed_url
            result = run_connection_update(mcg_obj, old, "http://")
            assert (
                result["aborted"] or getattr(result, "returncode", 1) != 0
            ), f"Malformed URL was not rejected:\n{result.get('raw')}"
            assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old

    @tier2
    @jira("DFBUGS-10346")
    # TODO: assign polarion id
    def test_direct_cr_edit_does_not_switch_datapath(
        self, mcg_obj, endpoint_conf, store_factory
    ):
        """
        A DIRECT ``oc patch`` of the endpoint (bypassing the CLI) updates the CR
        spec but does NOT switch the data path - regression guard for
        DFBUGS-10346 (only the CLI path performs the full connection update).
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        bs = store_factory(
            constants.BACKINGSTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )
        OCP(kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name).patch(
            params=f'{{"spec":{{"s3Compatible":{{"endpoint":"{new}"}}}}}}',
            format_type="merge",
        )
        # The spec now shows NEW, but the underlying connection / data path is
        # NOT switched. Full data-path verification (write via OBC, count blocks
        # per backend) should be added with the data-path harness.
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new
        pytest.skip(
            "Spec patched; add data-path assertions (OBC write + per-backend "
            "block count) to prove the data path did NOT switch - DFBUGS-10346."
        )


# ===========================================================================
# Module C - NamespaceStore + mixed batch (currently defective on 5.0)
# ===========================================================================
@mcg
@red_squad
@jira("RHSTOR-8718")
class TestNamespaceStoreEndpointUpdate:
    """
    NamespaceStore endpoint update is rejected by the admission webhook on this
    build (DFBUGS-10744) and its broken rollback leaves the store paused
    (DFBUGS-10743). These tests are regression guards for that behaviour and
    should be inverted to positive assertions once the bugs are fixed.
    """

    @tier2
    @jira("DFBUGS-10744")
    # TODO: assign polarion id
    def test_namespacestore_endpoint_update_rejected(
        self, mcg_obj, endpoint_conf, store_factory
    ):
        """
        Updating a NamespaceStore endpoint via the CLI is denied by the
        admission webhook (DFBUGS-10744); the broken rollback leaves the store
        with pause-reconcile=true (DFBUGS-10743).

        Uses signatureVersion v2 (required for a plain-http NamespaceStore).
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        sig = "v2" if old.startswith("http://") else endpoint_conf["signature_version"]
        nss = store_factory(
            constants.NAMESPACESTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            sig,
        )

        result = run_connection_update(mcg_obj, old, new)
        assert result["webhook_denied"], (
            f"Expected the NamespaceStore endpoint change to be webhook-denied "
            f"(DFBUGS-10744):\n{result['raw']}"
        )
        # Endpoint unchanged...
        assert get_store_endpoint(constants.NAMESPACESTORE, nss.name, ns) == old
        # ...but the broken rollback leaves it paused (DFBUGS-10743).
        assert (
            get_store_pause_annotation(constants.NAMESPACESTORE, nss.name, ns) == "true"
        ), (
            "Expected DFBUGS-10743: NamespaceStore left paused after failed "
            "rollback. If this assertion fails, the bug may be fixed - convert "
            "this test to the positive (successful update) case."
        )

    @tier2
    @jira("DFBUGS-10744")
    # TODO: assign polarion id
    def test_mixed_batch_blast_radius(self, mcg_obj, endpoint_conf, store_factory):
        """
        A mixed batch (BackingStore + NamespaceStore on the same endpoint)
        demonstrates the blast radius - the NamespaceStore webhook denial
        (DFBUGS-10744) rolls the (valid) BackingStore back too, so neither store
        ends up switched, and the NamespaceStore is left paused (DFBUGS-10743).

        Sub-case (b): if the BackingStore fails pre-validation, the batch aborts
        cleanly BEFORE any patch (both stay on OLD, no pause).
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        sig = "v2" if old.startswith("http://") else endpoint_conf["signature_version"]
        bs = store_factory(
            constants.BACKINGSTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )
        nss = store_factory(
            constants.NAMESPACESTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            sig,
        )

        # Happy-path attempt: both pass pre-validation, then NS denial rolls the
        # BackingStore back.
        result = run_connection_update(mcg_obj, old, new)
        assert result["webhook_denied"], result["raw"]
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old, (
            "Blast radius: the valid BackingStore should have been rolled back "
            "to OLD after the NamespaceStore denial."
        )
        assert not get_store_pause_annotation(constants.BACKINGSTORE, bs.name, ns)
        assert (
            get_store_pause_annotation(constants.NAMESPACESTORE, nss.name, ns) == "true"
        )  # DFBUGS-10743

        # Clear the stuck pause so the all-or-nothing sub-case starts clean.
        OCP(kind=constants.NAMESPACESTORE, namespace=ns).annotate(
            annotation="noobaa.io/pause-reconcile-", resource_name=nss.name
        )

        # Sub-case (b): make the BackingStore fail pre-validation -> clean abort.
        OCP(kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name).patch(
            params=(
                f'{{"spec":{{"s3Compatible":{{"targetBucket":'
                f'"{create_unique_resource_name("not-noobaa", "bucket")}"}}}}}}'
            ),
            format_type="merge",
        )
        abort = run_connection_update(mcg_obj, old, new)
        assert abort["aborted"] and abort["prevalidation_failed"], abort["raw"]
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old
        assert get_store_endpoint(constants.NAMESPACESTORE, nss.name, ns) == old
        assert not get_store_pause_annotation(constants.NAMESPACESTORE, nss.name, ns)


# ===========================================================================
# Deferred - require infrastructure not present in a standard MCG job
# ===========================================================================
@mcg
@red_squad
@jira("RHSTOR-8718")
class TestEndpointUpdateDeferred:
    """Placeholders that keep the coverage matrix complete."""

    @tier3
    @pytest.mark.skip(reason="needs fault injection of the core-connection update")
    def test_rollback_on_connection_update_failure(self):
        """
        If a NooBaa-core connection update fails mid-batch, patched stores are
        rolled back; a manual remediation step is required.
        """

    @tier3
    @pytest.mark.skip(reason="needs an ODF upgrade in the job")
    def test_endpoint_update_after_upgrade(self):
        """Update the endpoint of a store created on the prior ODF version."""

    @tier3
    @pytest.mark.skip(reason="needs provider mode + a second RGW endpoint")
    def test_provider_mode_reject_unreachable_rgw(self):
        """Provider mode rejects switching the default store to an unreachable
        RGW endpoint."""
