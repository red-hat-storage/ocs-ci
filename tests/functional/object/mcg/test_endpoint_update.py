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
  * ``TestNamespaceStoreEndpointUpdate`` - NamespaceStore switch and revert, and
    the mixed BackingStore + NamespaceStore batch. Both currently skipped while
    DFBUGS-10744 (webhook denies the change) is open.

ENVIRONMENT PREREQUISITES
-------------------------
Most tests need TWO S3 endpoint strings that both address the SAME backend, plus
a secret with credentials valid on both.  A same-backend pair keeps data-path
continuity assertable and avoids the two-backend "stranding" simulation artifact
described in the test plan.

The ``endpoint_conf`` fixture resolves such a pair automatically, so the suite
runs unattended on a standard MCG job:

  1. ``config.ENV_DATA["mcg_endpoint_pair"]`` if set - an explicit override, used
     for labs with two genuinely separate S3 services (e.g. two MinIO routes).
  2. Otherwise the pair is derived from an S3 service that already runs on the
     cluster - RGW first, then MCG's own S3 (self-ref).  A second Service is
     created in front of the very same pods, so the cluster hands out a second
     ClusterIP for one unchanged backend, and the two ``http://<ip>:80`` strings
     become a genuine endpoint pair.  The target bucket comes from
     ``cloud_uls_factory`` and is seeded with a ``noobaa_blocks/`` marker object,
     which is what CLI pre-validation looks for when it checks that the bucket is
     "a valid location used by noobaa"; the credentials secret comes from the
     cloud manager.  Everything is torn down with the fixtures that made it.
  3. Only if neither is available do the tests skip.

Plain http against a ClusterIP is deliberate: pre-validation resolves the target
bucket virtual-hosted style, so a DNS endpoint fails (no wildcard DNS for
in-cluster services), and an https ClusterIP fails the certificate check because
service-serving certificates carry DNS SANs only.

The explicit override looks like::

    ENV_DATA:
      mcg_endpoint_pair:
        old: https://<old-endpoint>
        new: https://<new-endpoint>
        target_bucket: <bucket-that-is-a-valid-noobaa-location-on-both>
        secret: <k8s-secret-with-AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY>
        signature_version: v4        # v2 for plain-http endpoints
        target_buckets:              # optional; the tests that stand several
          - <bucket-1>               # stores on one endpoint take one bucket
          - <bucket-2>               # each. Defaults to [target_bucket].
"""

import logging
import re
import threading
import time

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
from ocs_ci.ocs.bucket_utils import (
    list_objects_from_bucket,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.bucketclass import BucketClass
from ocs_ci.utility.utils import TimeoutSampler

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
# ``constants.BACKINGSTORE`` / ``NAMESPACESTORE`` are the lower-cased spellings
# that "oc get" accepts; a manifest handed to "oc create" needs the CR's exact
# kind, so map one onto the other.
_CR_KIND = {
    constants.BACKINGSTORE: "BackingStore",
    constants.NAMESPACESTORE: "NamespaceStore",
}


def _store_dict(
    kind, name, namespace, endpoint, target_bucket, secret_name, signature_version
):
    """Build an s3-compatible BackingStore/NamespaceStore CR dict."""
    return {
        "apiVersion": NOOBAA_API_VERSION,
        "kind": _CR_KIND[kind],
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


def get_pool_endpoint(mcg_obj, store_name):
    """
    Return the endpoint NooBaa core actually serves a store's pool from.

    This is the core's own view, read over the management RPC, and it is not
    necessarily what the CR spec says: a direct spec edit moves one without the
    other (DFBUGS-10346).

    Args:
        mcg_obj (MCG): The MCG fixture object.
        store_name (str): Name of the BackingStore / its pool.

    Returns:
        str: The endpoint, or None if the pool has none / does not exist.
    """
    for pool in mcg_obj.read_system().get("pools", []):
        if pool.get("name") == store_name:
            return pool.get("cloud_info", {}).get("endpoint")
    return None


def _try_write(io_pod, bucket_name, file_dir, pattern, mcg_obj):
    """Attempt a single write through a bucket; True when it goes through."""
    try:
        write_random_test_objects_to_bucket(
            io_pod, bucket_name, file_dir, amount=1, pattern=pattern, mcg_obj=mcg_obj
        )
        return True
    except Exception as ex:  # noqa - the caller retries
        logger.info(f"Write has not come through yet: {ex}")
        return False


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
# S3 services on the cluster that can supply an endpoint pair, in preference
# order: (ULS platform key, cloud-manager client attribute, Service name,
# signature version).
_DERIVABLE_SERVICES = (
    ("rgw", "rgw_client", constants.RGW_SERVICE_INTERNAL_MODE, "v2"),
    ("self-ref-mcg", "self_ref_mcg_client", "s3", "v4"),
)

# The derived endpoints are plain-http ClusterIPs, which is what keeps
# pre-validation on path-style addressing. Verified on a live cluster:
#   * a DNS endpoint fails, because pre-validation resolves the bucket
#     virtual-hosted style ("<bucket>.<host>") and in-cluster service names have
#     no wildcard DNS -> "getaddrinfo ENOTFOUND <bucket>.s3.openshift-storage...";
#   * an https ClusterIP fails the certificate check, since service-serving
#     certificates carry DNS SANs only -> "IP: <ip> is not in the cert's list".
_S3_HTTP_PORT = 80

# Pre-validation also requires the target bucket to be "a valid location used by
# noobaa". A single object under the block prefix is enough to satisfy it.
NOOBAA_LOCATION_MARKER_KEY = "noobaa_blocks/ocs-ci-endpoint-update-marker"

# How many BackingStores the bulk test puts on the shared endpoint. Each one
# needs a target bucket of its own - ocs-ci's own backingstore factory mints one
# ULS per store - so the fixture provisions this many buckets up front.
BULK_STORE_COUNT = 2

# An endpoint change takes a while to reach the running endpoint pods, so
# post-switch I/O is retried over this window rather than asserted outright.
ENDPOINT_PROPAGATION_TIMEOUT = 300

# How long a "nothing should happen" assertion waits before it is believed. It
# is deliberately longer than the propagation window above: if the operator were
# going to act on a direct CR edit, it would have done so well within it.
CORE_SETTLE_WINDOW = 120


def _create_alt_service(base_svc, name, namespace):
    """
    Put a second Service in front of the pods an existing S3 Service selects,
    which yields a second ClusterIP - a distinct endpoint for the same backend.

    Args:
        base_svc (dict): The existing Service definition.
        name (str): Name for the new Service.
        namespace (str): Namespace to create it in.

    Returns:
        tuple: (OCS object of the new Service, its ClusterIP)
    """
    http_port = next(p for p in base_svc["spec"]["ports"] if p["port"] == _S3_HTTP_PORT)
    svc_obj = create_resource(
        **{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "selector": base_svc["spec"]["selector"],
                "ports": [
                    {
                        "name": "s3",
                        "port": _S3_HTTP_PORT,
                        "targetPort": http_port["targetPort"],
                        "protocol": "TCP",
                    }
                ],
            },
        }
    )
    cluster_ip = OCP(kind="Service", namespace=namespace, resource_name=name).get()[
        "spec"
    ]["clusterIP"]
    return svc_obj, cluster_ip


def derive_endpoint_pair(request, cld_mgr, cloud_uls_factory):
    """
    Derive a same-backend endpoint pair from an S3 service that already runs on
    the cluster, so the suite needs no pre-provisioned lab.

    For the first usable service this creates a second Service in front of the
    same pods (giving two ClusterIPs for one backend), creates a target bucket
    through ``cloud_uls_factory``, and seeds the NooBaa-location marker that
    CLI pre-validation looks for.

    Args:
        request (FixtureRequest): Used to register the Service teardown.
        cld_mgr (CloudManager): The cluster's cloud manager.
        cloud_uls_factory (function): ULS (bucket) factory fixture.

    Returns:
        dict: The same structure as ``ENV_DATA['mcg_endpoint_pair']``, or None
            if no service on this cluster can supply a pair.
    """
    namespace = config.ENV_DATA["cluster_namespace"]
    for platform, client_attr, svc_name, signature_version in _DERIVABLE_SERVICES:
        client = getattr(cld_mgr, client_attr, None)
        if client is None:
            logger.info(f"No {platform} client on this cluster, trying the next one")
            continue
        try:
            base_svc = OCP(
                kind="Service", namespace=namespace, resource_name=svc_name
            ).get()
        except CommandFailed as ex:
            logger.info(f"Service {svc_name} is not available ({ex})")
            continue
        if not any(p["port"] == _S3_HTTP_PORT for p in base_svc["spec"]["ports"]):
            logger.info(f"Service {svc_name} exposes no http port {_S3_HTTP_PORT}")
            continue

        alt_svc_name = create_unique_resource_name("eps-alt", "svc")
        try:
            alt_svc, alt_ip = _create_alt_service(base_svc, alt_svc_name, namespace)
            request.addfinalizer(lambda obj=alt_svc: obj.delete(wait=False))
            target_buckets = sorted(
                cloud_uls_factory({platform: [(BULK_STORE_COUNT, None)]})[platform]
            )
            for bucket in target_buckets:
                client.client.Bucket(bucket).put_object(
                    Key=NOOBAA_LOCATION_MARKER_KEY, Body=b""
                )
        except Exception as ex:  # noqa - fall through to the next service
            logger.warning(f"Could not build an endpoint pair from {platform}: {ex}")
            continue

        pair = {
            "old": f"http://{base_svc['spec']['clusterIP']}:{_S3_HTTP_PORT}",
            "new": f"http://{alt_ip}:{_S3_HTTP_PORT}",
            "target_bucket": target_buckets[0],
            "target_buckets": target_buckets,
            "secret": client.secret.name,
            "signature_version": signature_version,
        }
        logger.info(f"Derived a same-backend endpoint pair from {platform}: {pair}")
        return pair
    return None


@pytest.fixture(scope="class")
def endpoint_pair(request, cld_mgr, cloud_uls_factory):
    """
    Resolve the endpoint pair once per test class - the explicit
    ``ENV_DATA['mcg_endpoint_pair']`` override if it is complete, otherwise a
    pair derived from the cluster.

    Returns:
        dict: The endpoint pair, or None if neither source is available.
    """
    conf = config.ENV_DATA.get("mcg_endpoint_pair")
    required = ("old", "new", "target_bucket", "secret")
    if conf and all(conf.get(k) for k in required):
        logger.info("Using the endpoint pair from ENV_DATA['mcg_endpoint_pair']")
        pair = {"signature_version": "v4", **conf}
        # A lab that lists only one bucket keeps working; the bulk test then
        # reuses it rather than spreading over several.
        pair.setdefault("target_buckets", [pair["target_bucket"]])
        return pair
    if conf:
        logger.warning(
            "ENV_DATA['mcg_endpoint_pair'] is incomplete (needs "
            f"{', '.join(required)}); deriving a pair from the cluster instead"
        )
    return derive_endpoint_pair(request, cld_mgr, cloud_uls_factory)


@pytest.fixture
def endpoint_conf(endpoint_pair):
    """
    Return the endpoint pair / bucket / secret used by the endpoint-update
    tests, or skip if the cluster cannot supply one.

    See the module docstring for details.
    """
    if not endpoint_pair:
        pytest.skip(
            "No endpoint pair available: this cluster exposes neither an RGW nor "
            "a self-ref MCG S3 service endpoint. Set "
            "ENV_DATA['mcg_endpoint_pair'] (old, new, target_bucket, secret) to "
            "run these tests."
        )
    return endpoint_pair


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

    def _finalizer():
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

    # Registered before the factory can create anything, so every store made is
    # torn down even if a later call raises (see docs/fixture_usage.md).
    request.addfinalizer(_finalizer)

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

    return _factory


@pytest.fixture
def bucketclass_over_store(request, mcg_obj):
    """
    Factory that wraps a BackingStore in a single-tier BucketClass, so a bucket
    created on it drives its I/O through that one store.

    Usage::

        bucket = bucket_factory(
            interface="OC", bucketclass=bucketclass_over_store(bs)
        )[0]
    """
    created = []

    def _finalizer():
        for bucketclass in reversed(created):
            try:
                bucketclass.delete()
            except Exception as ex:  # noqa - best-effort teardown
                logger.warning(
                    f"Teardown of BucketClass/{bucketclass.name} failed: {ex}"
                )

    # Registered before the factory can create anything (docs/fixture_usage.md).
    request.addfinalizer(_finalizer)

    def _factory(store):
        name = create_unique_resource_name("eps-bc", "bucketclass")
        mcg_obj.oc_create_bucketclass(name, [store], "Spread", None, None)
        bucketclass = BucketClass(name, [store], None, "Spread", None, None)
        created.append(bucketclass)
        return bucketclass

    return _factory


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
               same credentials (identity) but each with its own target bucket.
            2. Run one connection update OLD -> NEW.
            3. Assert every store was updated and moved to NEW, while the
               connections are de-duplicated by (identity, endpoint) - stores
               that share an identity and endpoint collapse to a single unique
               connection.

        The stores deliberately do NOT share a target bucket: de-duplication
        keys on (identity, endpoint) alone, so giving each store its own bucket
        keeps the assertion honest instead of letting identical stores make it
        look true by accident.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        buckets = endpoint_conf["target_buckets"]
        stores = [
            store_factory(
                constants.BACKINGSTORE,
                old,
                buckets[i % len(buckets)],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            for i in range(BULK_STORE_COUNT)
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
            * trailing_slash      - the correct endpoint with its trailing "/"
                                    toggled, which is not an exact match.
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
            # Toggle the slash rather than always appending one - an endpoint
            # that already ends in "/" would otherwise be an exact match and
            # the store really would be updated.
            variant_endpoint = old.rstrip("/") if old.endswith("/") else f"{old}/"
            result = run_connection_update(mcg_obj, variant_endpoint, new)
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
        buckets = endpoint_conf["target_buckets"]
        bs_old = store_factory(
            constants.BACKINGSTORE,
            old,
            buckets[0],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )
        bs_new = store_factory(
            constants.BACKINGSTORE,
            new,
            buckets[1 % len(buckets)],
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
        bucketclass_over_store,
        bucket_factory,
        awscli_pod_session,
        test_directory_setup,
    ):
        """
        Fire the endpoint switch while I/O is running through the store.

        Flow:
            1. Create a BackingStore on OLD, put an OBC in front of it through a
               single-tier BucketClass, and prove the data path works.
            2. Start a continuous write loop and trigger the connection update
               OLD -> NEW while it runs.
            3. Assert the store ends up Ready/OPTIMAL on NEW with no lingering
               pause, and that I/O issued after the endpoint-config propagation
               window succeeds and the pre-switch objects are still readable.

        NOTE: continuity is asserted only on post-switch I/O because of the
        endpoint-config propagation window; writes issued mid-switch are allowed
        to fail. A same-backend endpoint pair avoids the two-backend stranding
        artifact, so the pre-switch objects must remain visible throughout.
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
        bucket = bucket_factory(interface="OC", bucketclass=bucketclass_over_store(bs))[
            0
        ]

        pre_objects = write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket.name,
            test_directory_setup.origin_dir,
            amount=2,
            pattern="pre-switch-",
            mcg_obj=mcg_obj,
        )

        # Keep writing while the endpoint moves underneath the store.
        stop_io = threading.Event()
        io_errors = []

        def _write_loop():
            round_nr = 0
            while not stop_io.is_set():
                try:
                    write_random_test_objects_to_bucket(
                        awscli_pod_session,
                        bucket.name,
                        test_directory_setup.origin_dir,
                        amount=1,
                        pattern=f"during-switch-{round_nr}-",
                        mcg_obj=mcg_obj,
                    )
                except Exception as ex:  # noqa - tolerated, see the NOTE above
                    io_errors.append(ex)
                round_nr += 1

        writer = threading.Thread(target=_write_loop, daemon=True)
        writer.start()
        try:
            result = run_connection_update(mcg_obj, old, new)
        finally:
            stop_io.set()
            writer.join(timeout=300)
        if io_errors:
            logger.info(
                f"{len(io_errors)} write(s) failed during the switch window, "
                f"first: {io_errors[0]}"
            )

        assert not result["aborted"], result["raw"]
        assert (
            result["stores_updated"] == 1
        ), f"Expected the store under I/O to be updated:\n{result['raw']}"
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new
        assert not get_store_pause_annotation(constants.BACKINGSTORE, bs.name, ns)
        assert mcg_obj.check_backingstore_state(
            bs.name, constants.BS_OPTIMAL, timeout=ENDPOINT_PROPAGATION_TIMEOUT
        ), f"BackingStore {bs.name} did not return to OPTIMAL after the switch"

        # Post-switch I/O must succeed once the endpoint config has propagated.
        for sample in TimeoutSampler(
            ENDPOINT_PROPAGATION_TIMEOUT,
            15,
            _try_write,
            awscli_pod_session,
            bucket.name,
            test_directory_setup.origin_dir,
            "post-switch-",
            mcg_obj,
        ):
            if sample:
                break

        listed = set(
            list_objects_from_bucket(awscli_pod_session, bucket.name, s3_obj=mcg_obj)
        )
        missing = set(pre_objects) - listed
        assert not missing, (
            "Objects written before the switch are no longer readable through "
            f"the store: {sorted(missing)}"
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

        The divergence is asserted against NooBaa core's own view of the pool
        rather than against where data lands: the endpoint pair addresses a
        single backend on purpose, so both endpoints reach the same bucket and
        the destination of the data cannot tell the two apart. What can be
        observed is that the CR says NEW while core still serves OLD.
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
        assert (
            get_pool_endpoint(mcg_obj, bs.name) == old
        ), "Core did not pick up the store's original endpoint - bad starting state"

        OCP(kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name).patch(
            params=f'{{"spec":{{"s3Compatible":{{"endpoint":"{new}"}}}}}}',
            format_type="merge",
        )
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new

        # Give the operator more than the usual propagation window to act on the
        # edit - the point of the test is that it never does.
        time.sleep(CORE_SETTLE_WINDOW)
        core_endpoint = get_pool_endpoint(mcg_obj, bs.name)
        assert core_endpoint == old, (
            "A direct CR edit switched the core connection to "
            f"{core_endpoint!r}. DFBUGS-10346 may be fixed - if so, update this "
            "test to assert the new behaviour."
        )


# ===========================================================================
# Module C - NamespaceStore + mixed batch
# ===========================================================================
DFBUGS_10744_SKIP = pytest.mark.skip(
    "NamespaceStore endpoint update is denied by the admission webhook - "
    "https://redhat.atlassian.net/browse/DFBUGS-10744"
)


@mcg
@red_squad
class TestNamespaceStoreEndpointUpdate:
    """
    Endpoint update for NamespaceStores, on their own and in a batch alongside
    BackingStores.

    Both tests assert the intended behaviour - a successful switch. They are
    skipped on builds where DFBUGS-10744 is open: the admission webhook denies
    the NamespaceStore change, and the broken rollback that follows leaves the
    store annotated pause-reconcile=true (DFBUGS-10743). Drop the skip marker
    once DFBUGS-10744 is fixed.
    """

    @tier2
    @jira("DFBUGS-10744")
    @DFBUGS_10744_SKIP
    # TODO: assign polarion id
    def test_switch_and_revert_single_nss(self, mcg_obj, endpoint_conf, store_factory):
        """
        Switch a single s3-compatible NamespaceStore's endpoint from OLD to NEW,
        then revert it back.

        Mirrors :meth:`TestBackingStoreEndpointUpdate.
        test_switch_and_revert_single_bs` for the NamespaceStore kind.

        Flow:
            1. Create a NamespaceStore on the OLD endpoint and wait for Ready.
            2. Run the connection update OLD -> NEW and assert exactly one store
               was updated, its spec endpoint now points at NEW, and the
               transient pause-reconcile annotation was cleaned up.
            3. Run the reverse update NEW -> OLD and assert the store is switched
               back to its original endpoint.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        nss = store_factory(
            constants.NAMESPACESTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )

        # --- switch OLD -> NEW ---
        result = run_connection_update(mcg_obj, old, new)
        assert not result["webhook_denied"], (
            f"The NamespaceStore endpoint change was denied by the admission "
            f"webhook (DFBUGS-10744):\n{result['raw']}"
        )
        assert not result["aborted"], f"Update aborted unexpectedly:\n{result['raw']}"
        assert (
            result["stores_updated"] == 1
        ), f"Expected exactly 1 store updated, got {result['stores_updated']}"
        assert get_store_endpoint(constants.NAMESPACESTORE, nss.name, ns) == new
        # No lingering pause annotation after a successful update (DFBUGS-10743)
        assert not get_store_pause_annotation(
            constants.NAMESPACESTORE, nss.name, ns
        ), "pause-reconcile annotation was not cleaned up after a successful switch"
        OCP(
            kind=constants.NAMESPACESTORE, namespace=ns, resource_name=nss.name
        ).wait_for_resource(
            condition=constants.STATUS_READY, column="PHASE", timeout=180
        )

        # --- revert NEW -> OLD ---
        revert = run_connection_update(mcg_obj, new, old)
        assert not revert["aborted"], f"Revert aborted:\n{revert['raw']}"
        assert revert["stores_updated"] == 1
        assert get_store_endpoint(constants.NAMESPACESTORE, nss.name, ns) == old

    @tier2
    @jira("DFBUGS-10744")
    @DFBUGS_10744_SKIP
    # TODO: assign polarion id
    def test_mixed_batch_switch_bs_and_nss(self, mcg_obj, endpoint_conf, store_factory):
        """
        A mixed batch - a BackingStore and a NamespaceStore sharing one endpoint
        - switches together in a single command.

        Flow:
            1. Create both stores on the OLD endpoint, each with its own target
               bucket.
            2. Run one connection update OLD -> NEW; both stores must move and
               neither may be left paused.
            3. All-or-nothing check: make the BackingStore fail pre-validation
               and run the reverse update. The batch must abort BEFORE any spec
               is patched, so both stores stay on NEW and the NamespaceStore is
               not left paused.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        buckets = endpoint_conf["target_buckets"]
        bs = store_factory(
            constants.BACKINGSTORE,
            old,
            buckets[0],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )
        nss = store_factory(
            constants.NAMESPACESTORE,
            old,
            buckets[1 % len(buckets)],
            endpoint_conf["secret"],
            endpoint_conf["signature_version"],
        )

        # --- both stores switch in one command ---
        result = run_connection_update(mcg_obj, old, new)
        assert not result["webhook_denied"], (
            f"The NamespaceStore half of the batch was denied by the admission "
            f"webhook (DFBUGS-10744):\n{result['raw']}"
        )
        assert not result["aborted"], f"Mixed batch aborted:\n{result['raw']}"
        assert (
            result["stores_updated"] == 2
        ), f"Expected both stores updated, got {result['stores_updated']}"
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new
        assert get_store_endpoint(constants.NAMESPACESTORE, nss.name, ns) == new
        assert not get_store_pause_annotation(constants.BACKINGSTORE, bs.name, ns)
        assert not get_store_pause_annotation(constants.NAMESPACESTORE, nss.name, ns)

        # --- all-or-nothing: a pre-validation failure aborts before any patch ---
        OCP(kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name).patch(
            params=(
                f'{{"spec":{{"s3Compatible":{{"targetBucket":'
                f'"{create_unique_resource_name("not-noobaa", "bucket")}"}}}}}}'
            ),
            format_type="merge",
        )
        abort = run_connection_update(mcg_obj, new, old)
        assert abort["aborted"] and abort["prevalidation_failed"], abort["raw"]
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new
        assert get_store_endpoint(constants.NAMESPACESTORE, nss.name, ns) == new
        assert not get_store_pause_annotation(constants.NAMESPACESTORE, nss.name, ns)
