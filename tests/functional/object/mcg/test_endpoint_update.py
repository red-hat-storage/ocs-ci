"""
Automated coverage for RHSTOR-8718 - "Support Changes in Target Endpoint for
BackingStores (and NamespaceStores)".

The feature adds the CLI command::

    noobaa connection update --old-endpoint <OLD> --new-endpoint <NEW>

which performs an endpoint-scoped, all-or-nothing bulk update: it matches every
BackingStore (and NamespaceStore) whose spec endpoint == OLD, pre-validates each
one against NEW, and only then patches the specs and updates the underlying
NooBaa-core connections (rolling back automatically on any failure).

ENVIRONMENT PREREQUISITES
-------------------------
Most tests need TWO S3 endpoint strings that both address the SAME backend, plus
a secret with credentials valid on both.  A same-backend pair keeps data-path
continuity assertable and avoids the two-backend "stranding" simulation artifact
described in the test plan.

The ``endpoint_pair`` fixture resolves such a pair automatically, so the suite
runs unattended on a standard MCG job.  See its docstring for the resolution
order and for the ``ENV_DATA['mcg_endpoint_pair']`` override schema.
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
    polarion_id,
)
from ocs_ci.helpers.helpers import create_resource, create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    list_objects_from_bucket,
    write_random_test_objects_to_bucket,
)
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.bucketclass import BucketClass
from ocs_ci.ocs.resources.pod import get_noobaa_operator_pod, get_pod_logs
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# The MCG CR templates the store factory builds on. They already carry the
# apiVersion, the exact kind, the app=noobaa label and the noobaa.io finalizer,
# so only metadata and spec have to be filled in. See :func:`_store_dict`.
_STORE_TEMPLATE = {
    constants.BACKINGSTORE: constants.MCG_BACKINGSTORE_YAML,
    constants.NAMESPACESTORE: constants.MCG_NAMESPACESTORE_YAML,
}

# A store that has only just reached Ready is still being written to by the
# operator (status, conditions, finalizers). The CLI lists the stores up front
# and then writes back the copies it holds, with no re-read on conflict
# (DFBUGS-10937), so tests that are not about that bug wait for the writes to
# die down first. See :func:`wait_for_stores_quiesced`.
STORE_QUIESCE_TIMEOUT = 120
STORE_QUIESCE_INTERVAL = 5
STORE_QUIESCE_STABLE_SAMPLES = 3

# The mirror image of the above: an annotation rewritten in a loop to keep
# bumping resourceVersion on purpose, so that
# TestConnectionUpdateNegative.test_switch_survives_concurrent_store_writes can
# hold the stores in contention for the whole run. It carries no meaning to the
# operator and is removed in teardown.
CHURN_ANNOTATION = "ocs-ci.qe/endpoint-update-churn"
CHURN_INTERVAL = 1
CHURN_JOIN_TIMEOUT = 30

# The admission webhook refuses to delete a store while NooBaa is still removing
# objects from it. That clears on its own once the deletion finishes, so
# ``store_factory`` retries instead of leaving the store behind for the next
# test to trip over.
STORE_DELETE_TIMEOUT = 300
STORE_DELETE_INTERVAL = 15
STORE_DELETE_RETRY_MARKER = "are still being deleted"

# How long test_switch_during_active_io waits for its writer thread to notice
# that it has been asked to stop. A write already in flight is an exec into the
# awscli pod and cannot be cancelled, so this is generous enough that only a
# genuinely stuck exec reaches it.
WRITER_JOIN_TIMEOUT = 300

# The annotation the CLI sets on every matched store for the duration of the
# update, so the operator does not reconcile a store while its endpoint is being
# moved underneath it.
PAUSE_ANNOTATION = "noobaa.io/pause-reconcile"

# While the annotation is set the operator logs a skip line for the store and
# requeues a few seconds later. As of noobaa-operator 301d6e98 the line is
#   BackingStore "<name>" reconciliation paused. Skipping reconcile.
# emitted from pkg/backingstore/reconciler.go (pkg/namespacestore/reconciler.go
# has the NamespaceStore twin). The pattern is deliberately loose - it pins the
# store name and the word "paused" and nothing else - because the surrounding
# wording is an operator log string and may be reworded at any time.
PAUSE_SKIP_LOG_PATTERN = r"{name}.*paused"

# How long to watch the operator log for that line before giving up. The
# operator requeues a paused store every ~5s, so this is many chances over.
PAUSE_LOG_TIMEOUT = 90
PAUSE_LOG_INTERVAL = 10

# After the annotation is removed, how long to let the operator pick the store
# back up before asserting the skip lines have stopped. Several requeue periods,
# so an in-flight skip logged just before the removal cannot fail the check.
PAUSE_RESUME_SETTLE = 20

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

# One extra bucket is provisioned alongside them and deliberately left WITHOUT
# the marker object, so it exists on both endpoints but is not a valid NooBaa
# location. That is what the not_noobaa_location pre-validation case needs, and
# it is a different condition from a bucket that does not exist at all (see
# missing_target_bucket).
EMPTY_BUCKET_COUNT = 1

# An endpoint change takes a while to reach the running endpoint pods, so
# post-switch I/O is retried over this window rather than asserted outright.
ENDPOINT_PROPAGATION_TIMEOUT = 300

# Host length used by the long-URL input case. Long enough that resolution fails
# on the length itself (getaddrinfo EINVAL) rather than on the name not
# existing, which is the point - it probes the CLI's handling of an absurd but
# syntactically plausible endpoint.
MALFORMED_LONG_HOST_LEN = 2048

# NooBaa core's check_external_connection() reports a coarse UNKNOWN_FAILURE for
# DNS failures, unreachable hosts and missing buckets alike - its AWS error map
# is keyed on SDK v2 error names while the client is v3, so everything
# client-side falls through to the catch-all (DFBUGS-10938). The real reason is
# only in the error text, which is why the tests below assert on the message as
# well as the code. INVALID_ENDPOINT is accepted too, so the tests keep passing
# once DFBUGS-10938 is fixed and core starts classifying these properly.
# TODO(DFBUGS-10938): once core reports precise codes, drop the message
# assertions and pin each variant to its own status code.
UNREACHABLE_STATUSES = ("UNKNOWN_FAILURE", "INVALID_ENDPOINT")

_DFBUGS_10975_REASON = (
    "connection update cannot read a Ceph object-user secret that uses the "
    "AccessKey/SecretKey names, so every s3-compatible store backed by RGW "
    "fails pre-validation - https://redhat.atlassian.net/browse/DFBUGS-10975"
)

# The pre-validation cases whose asserted failure reason DFBUGS-10975 masks.
# "wrong_creds" is deliberately absent: see the parametrize list for why.
_DFBUGS_10975_BLOCKED_PREVALIDATION = frozenset(
    {
        "unreachable",
        "missing_target_bucket",
        "not_noobaa_location",
        "one_bad_in_batch",
    }
)


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
    """
    Return the first capture group of ``pattern`` in ``text`` as an int.

    Args:
        pattern (str): Regex with a single capturing group.
        text (str): Text to search.

    Returns:
        int: The captured value, or None if the pattern does not match.
    """
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
            "conflict": bool,              # a Kubernetes resource-version
                                           # conflict killed the command
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
        "conflict": ("Conflict:" in out or "the object has been modified" in out),
        "status_codes": re.findall(r"status=([A-Z_]+)", out),
    }


# ---------------------------------------------------------------------------
# Resource helpers
# ---------------------------------------------------------------------------
def _store_dict(
    kind, name, namespace, endpoint, target_bucket, secret_name, signature_version
):
    """
    Build an s3-compatible BackingStore/NamespaceStore CR from the MCG template.

    Args:
        kind (str): ``constants.BACKINGSTORE`` or ``constants.NAMESPACESTORE``.
        name (str): Name for the new store.
        namespace (str): Namespace to create it in.
        endpoint (str): S3 endpoint the store points at.
        target_bucket (str): Bucket on that endpoint.
        secret_name (str): Secret holding the credentials.
        signature_version (str): ``v2`` or ``v4``.

    Returns:
        dict: The CR body, ready for :func:`create_resource`.
    """
    body = templating.load_yaml(_STORE_TEMPLATE[kind])
    body["metadata"]["name"] = name
    body["metadata"]["namespace"] = namespace
    body["spec"] = {
        "type": constants.BACKINGSTORE_TYPE_S3_COMP,
        "s3Compatible": {
            "endpoint": endpoint,
            "targetBucket": target_bucket,
            "signatureVersion": signature_version,
            "secret": {"name": secret_name, "namespace": namespace},
        },
    }
    return body


def get_store_endpoint(kind, name, namespace):
    """Return the current spec endpoint of a BackingStore/NamespaceStore."""
    obj = OCP(kind=kind, namespace=namespace, resource_name=name).get()
    return obj["spec"]["s3Compatible"]["endpoint"]


def get_pool_endpoint(mcg_obj, store_name):
    """
    Return the endpoint NooBaa core actually serves a store's pool from.

    This is the core's own view, read over the management RPC, and it is what
    the data path actually uses - which is not the same thing as what the CR
    spec says, so the two are worth asserting separately.

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


def _write_dir(base_dir, label):
    """
    A dedicated source directory for one write, under the test's origin dir.

    ``write_random_test_objects_to_bucket`` syncs the whole directory it is
    given, not only the files it has just generated. Reusing one directory
    across writes therefore makes every later write re-upload the earlier
    ones' objects into whichever bucket it is aimed at - which shows up as
    objects "leaking" between buckets that never actually saw each other's
    data. Tests here compare what each bucket holds, so every write gets its
    own directory.

    Args:
        base_dir (str): The test's origin directory.
        label (str): A name unique to this write within the test.

    Returns:
        str: The path to use as ``file_dir``.
    """
    return f"{base_dir}/{label}"


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


def wait_for_stores_quiesced(
    stores,
    namespace,
    timeout=STORE_QUIESCE_TIMEOUT,
    interval=STORE_QUIESCE_INTERVAL,
    stable_samples=STORE_QUIESCE_STABLE_SAMPLES,
):
    """
    Block until a set of stores stops being written to.

    Every write to a CR bumps its ``metadata.resourceVersion``, so the stores
    count as settled once all of their resourceVersions have held still across
    ``stable_samples`` consecutive polls.

    This is a mitigation, not a guarantee - the operator is free to write again
    a moment later. It only narrows the window in which ``connection update``
    would hit the un-retried resource-version conflict of DFBUGS-10937, so that
    tests about other behaviour are not derailed by that bug.
    :meth:`TestConnectionUpdateNegative.test_switch_survives_concurrent_store_writes`
    is the test that deliberately does NOT settle, and pins the bug instead.

    Args:
        stores (list): (kind, name) tuples to watch.
        namespace (str): Namespace the stores live in.
        timeout (int): Give up after this many seconds and return anyway.
        interval (int): Seconds between polls.
        stable_samples (int): Consecutive identical samples required.

    Returns:
        bool: True if the stores settled, False if the timeout was hit first
            (in which case the caller carries on regardless).
    """

    def _versions():
        return {
            (kind, name): OCP(kind=kind, namespace=namespace, resource_name=name).get()[
                "metadata"
            ]["resourceVersion"]
            for kind, name in stores
        }

    previous = None
    stable = 0
    try:
        for current in TimeoutSampler(timeout, interval, _versions):
            stable = stable + 1 if current == previous else 0
            if stable >= stable_samples:
                logger.info(f"Stores settled at resourceVersions {current}")
                return True
            previous = current
    except TimeoutExpiredError:
        logger.warning(
            f"Stores were still being written to after {timeout}s; running anyway"
        )
    return False


def get_store_pause_annotation(kind, name, namespace):
    """Return the value of the noobaa.io/pause-reconcile annotation (or None)."""
    return (
        OCP(kind=kind, namespace=namespace, resource_name=name)
        .get()
        .get("metadata", {})
        .get("annotations", {})
        .get(PAUSE_ANNOTATION)
    )


def operator_logged_pause_skip(operator_pod, namespace, store_name, since):
    """
    Report whether the operator logged a reconcile skip for a paused store.

    Args:
        operator_pod (str): Name of the noobaa-operator pod.
        namespace (str): Namespace the operator runs in.
        store_name (str): Store whose skip line to look for.
        since (str): Only consider log lines newer than this relative duration
            (e.g. "2m"), so an earlier pause cannot satisfy a later assertion.

    Returns:
        bool: True if a skip line for this store was logged in the window.
    """
    matched = get_pod_logs(
        pod_name=operator_pod,
        namespace=namespace,
        since=since,
        grep=PAUSE_SKIP_LOG_PATTERN.format(name=re.escape(store_name)),
        regex=True,
        first_match_only=False,
    )
    return bool(matched and matched.strip())


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
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
    same pods (giving two ClusterIPs for one backend), creates the target
    buckets through ``cloud_uls_factory``, and seeds the NooBaa-location marker
    that CLI pre-validation looks for into all but one of them - the odd one out
    becomes ``empty_bucket``, a bucket that exists but is not a valid NooBaa
    location.

    The pair also carries ``old_dns``: the in-cluster DNS name of the SAME
    service ``old`` addresses by ClusterIP. It is never used as a working
    endpoint (a DNS endpoint fails pre-validation, see ``_S3_HTTP_PORT``) -
    only as an ``--old-endpoint`` that resolves to the right backend yet must
    still match no store, which is what the ``dns_form`` no-match case needs.

    Teardown: the alternate Service is registered with ``request``; the buckets
    belong to ``cloud_uls_factory`` and are removed with it.

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
            # cloud_uls_factory owns the teardown of every bucket it mints.
            buckets = sorted(
                cloud_uls_factory(
                    {platform: [(BULK_STORE_COUNT + EMPTY_BUCKET_COUNT, None)]}
                )[platform]
            )
            target_buckets = buckets[:BULK_STORE_COUNT]
            # Everything but the last bucket gets the marker; the last one is
            # left empty on purpose so it is a real bucket that is not a valid
            # NooBaa location.
            empty_bucket = buckets[-1]
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
            # The SAME backend as "old", addressed by its service DNS name
            # instead of its ClusterIP. Only ever passed as --old-endpoint, to
            # show that matching is an exact string comparison and an equivalent
            # spelling matches nothing - so it is never connected to, and the
            # virtual-hosted DNS limitation noted above does not apply.
            "old_dns": (
                f"http://{svc_name}.{namespace}.svc.cluster.local:{_S3_HTTP_PORT}"
            ),
            "target_bucket": target_buckets[0],
            "target_buckets": target_buckets,
            "empty_bucket": empty_bucket,
            "secret": client.secret.name,
            "signature_version": signature_version,
        }
        logger.info(f"Derived a same-backend endpoint pair from {platform}: {pair}")
        return pair
    return None


@pytest.fixture(scope="class")
def endpoint_pair(request, cld_mgr, cloud_uls_factory):
    """
    Resolve the endpoint pair once per test class, so the suite runs unattended
    on a standard MCG job:

      1. ``config.ENV_DATA["mcg_endpoint_pair"]`` if it is complete - an explicit
         override, used for labs with two genuinely separate S3 services (e.g.
         two MinIO routes).
      2. Otherwise the pair is derived from an S3 service that already runs on
         the cluster - RGW first, then MCG's own S3 (self-ref). A second Service
         is created in front of the very same pods, so the cluster hands out a
         second ClusterIP for one unchanged backend, and the two
         ``http://<ip>:80`` strings become a genuine endpoint pair. The target
         buckets come from ``cloud_uls_factory`` and are seeded with a
         ``noobaa_blocks/`` marker object, which is what CLI pre-validation looks
         for when it checks that the bucket is "a valid location used by noobaa";
         one further bucket is provisioned and deliberately left unseeded, to
         serve the negative case of a bucket that exists but is not a NooBaa
         location. The credentials secret comes from the cloud manager.
         Everything is torn down with the fixtures that made it. See
         :func:`derive_endpoint_pair`.
      3. Only if neither is available do the tests skip (in ``endpoint_conf``).

    The explicit override looks like::

        ENV_DATA:
          mcg_endpoint_pair:
            old: https://<old-endpoint>
            new: https://<new-endpoint>
            target_bucket: <bucket-that-is-a-valid-noobaa-location-on-both>
            secret: <k8s-secret-with-AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY>
            signature_version: v4        # BackingStores. NamespaceStores on a
                                         # plain-http endpoint are forced to v2
                                         # by ``endpoint_conf``.
            target_buckets:              # optional; the tests that stand several
              - <bucket-1>               # stores on one endpoint take one bucket
              - <bucket-2>               # each. Defaults to [target_bucket].
            empty_bucket: <bucket>       # optional; a bucket that exists on both
                                         # endpoints but holds NO noobaa_blocks/
                                         # prefix.
            old_dns: http://<host>:<port>
                                         # optional; a second, equivalent
                                         # spelling of `old`.

    The override only has to supply ``old``, ``new``, ``target_bucket`` and
    ``secret``; ``target_buckets``, ``empty_bucket`` and ``old_dns`` are optional.
    There is no sensible default for ``empty_bucket`` - it has to be a bucket that
    really exists on both endpoints and really has no ``noobaa_blocks/`` prefix,
    which cannot be conjured from the other keys - so the ``not_noobaa_location``
    pre-validation case skips when a lab does not name one. Nor for ``old_dns``,
    which has to be a second, equivalent spelling of ``old`` (its DNS name where
    ``old`` is an IP, or the reverse); the ``dns_form`` no-match case skips
    without it. A derived pair always has both, because the fixture provisions
    them deliberately.

    Teardown: nothing of its own. The override path creates no resources, and the
    derived path registers its teardown inside ``derive_endpoint_pair``.

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

    See :func:`endpoint_pair` for how the pair is resolved.

    Adds one derived key, ``signature_version_nss``. The admission webhook
    rejects a NamespaceStore that pairs a non-secure endpoint with signature
    version v4 ("Non-secure endpoint works only with signature-version v2"),
    while a BackingStore on the very same endpoint is accepted with v4. So the
    NamespaceStore tests cannot simply reuse ``signature_version``.
    """
    if not endpoint_pair:
        pytest.skip(
            "No endpoint pair available: this cluster exposes neither an RGW nor "
            "a self-ref MCG S3 service endpoint. Set "
            "ENV_DATA['mcg_endpoint_pair'] (old, new, target_bucket, secret) to "
            "run these tests."
        )
    plain_http = endpoint_pair["old"].startswith("http://")
    return {
        **endpoint_pair,
        "signature_version_nss": (
            "v2" if plain_http else endpoint_pair["signature_version"]
        ),
    }


@pytest.fixture
def store_factory(request):
    """
    Factory that creates s3-compatible BackingStore / NamespaceStore CRs on a
    given endpoint, waits for Ready, and cleans them up afterwards.

    Usage::

        bs = store_factory(constants.BACKINGSTORE, endpoint, bucket, secret)

    Teardown clears a leftover pause-reconcile annotation first, then deletes,
    retrying while the admission webhook reports that objects in the store are
    still being deleted.
    """
    namespace = config.ENV_DATA["cluster_namespace"]
    created = []

    def _delete_store(kind, name):
        """Delete one store, waiting out the webhook's "try later" denial."""
        store_ocp = OCP(kind=kind, namespace=namespace)
        # A paused NamespaceStore (DFBUGS-10743) must have the annotation
        # removed before it can be reconciled/deleted cleanly. This is a
        # best-effort nicety, not a precondition for deleting: letting a
        # transient API error here propagate would skip the delete below
        # entirely and leak the store, which is how orphaned Rejected stores
        # have been left on test clusters before.
        try:
            if get_store_pause_annotation(kind, name, namespace):
                store_ocp.annotate(
                    annotation="noobaa.io/pause-reconcile-", resource_name=name
                )
        except CommandFailed as ex:
            logger.warning(f"Could not clear pause annotation on {kind}/{name}: {ex}")
        deadline = time.time() + STORE_DELETE_TIMEOUT
        while True:
            try:
                store_ocp.delete(resource_name=name, wait=True)
                return
            except CommandFailed as ex:
                if STORE_DELETE_RETRY_MARKER not in str(ex):
                    raise
                if time.time() >= deadline:
                    raise
                logger.info(
                    f"{kind}/{name} still has objects being deleted, retrying "
                    f"in {STORE_DELETE_INTERVAL}s"
                )
                time.sleep(STORE_DELETE_INTERVAL)

    def _finalizer():
        for kind, name in reversed(created):
            try:
                _delete_store(kind, name)
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
        # Registered before the CR is created, not after: if create_resource
        # raises once the object already exists, an append placed after it
        # would never run and the store would be invisible to teardown. If the
        # create did fail outright the delete just reports NotFound, which the
        # finalizer already downgrades to a warning.
        created.append((kind, name))
        ocs_obj = create_resource(**body)
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


def cluster_has_rook_keyed_object_user_secret():
    """
    Report whether the cluster carries a Ceph object-user secret that names its
    credentials ``AccessKey``/``SecretKey`` instead of ``AWS_ACCESS_KEY_ID``/
    ``AWS_SECRET_ACCESS_KEY``.

    This is the precondition for DFBUGS-10975. Rook generates the object-user
    secret with its own key names, and the operator repoints a store's
    ``secretRef`` at it whenever an existing secret holds identical credentials
    (``CheckForIdenticalSecretsCreds``), so a store this suite creates from a
    correctly-keyed secret still ends up reading the Rook-keyed one. The CLI's
    credential lookup does not map the alternate names, so pre-validation is
    handed empty credentials and the whole update aborts.

    Testing the key names rather than "is this an RGW cluster" means the tests
    un-skip by themselves if Rook ever adopts the AWS names.

    Returns:
        bool: True if such a secret exists, False otherwise - including when the
            secrets cannot be listed, so an inaccessible cluster does not
            silently skip the coverage.
    """
    namespace = config.ENV_DATA["cluster_namespace"]
    try:
        secrets = OCP(kind=constants.SECRET, namespace=namespace).get()["items"]
    except (CommandFailed, KeyError) as ex:
        logger.warning(f"Could not list secrets in {namespace}: {ex}")
        return False
    for secret in secrets:
        if not secret["metadata"]["name"].startswith("rook-ceph-object-user-"):
            continue
        data = secret.get("data") or {}
        if "AccessKey" in data and "AWS_ACCESS_KEY_ID" not in data:
            return True
    return False


def skip_if_dfbugs_10975():
    """
    Skip the calling test when DFBUGS-10975's precondition holds.

    Parametrized cases call this from the test body instead of carrying
    ``DFBUGS_10975_SKIP``: ``pytest.mark.usefixtures`` has no effect when it is
    applied through ``pytest.param(marks=...)``, so the marker form only works
    on a whole test.
    """
    if cluster_has_rook_keyed_object_user_secret():
        pytest.skip(_DFBUGS_10975_REASON)


@pytest.fixture
def skip_if_rook_keyed_secret():
    """Skip when DFBUGS-10975's precondition holds on the cluster under test."""
    skip_if_dfbugs_10975()


# A runtime fixture rather than a pytest.mark.skipif because the condition has
# to query the cluster, which is not reachable at collection time - a skipif
# would break --collect-only.
#
# Deliberately NOT paired with @jira("DFBUGS-10975"), unlike the other bug
# markers in this module: pytest-jira skips on the marker alone, unconditionally
# and on every platform, which would delete the coverage everywhere. The bug
# only bites where a Rook-keyed object-user secret exists, so gating on that
# keeps these tests running on clusters without RGW (e.g. IBM Cloud, where the
# endpoint pair is derived from MCG's own S3). The skip reason carries the bug
# URL, so traceability does not depend on the marker.
#
# Drop this once DFBUGS-10975 is fixed.
DFBUGS_10975_SKIP = pytest.mark.usefixtures("skip_if_rook_keyed_secret")

# DFBUGS-10938 is not platform-conditional: check external connection maps a
# genuine credentials rejection onto UNKNOWN FAILURE on every backend, so this
# one is a plain unconditional skip.
DFBUGS_10938_SKIP = pytest.mark.skip(
    "check external connection reports a credentials rejection as "
    "UNKNOWN FAILURE - https://redhat.atlassian.net/browse/DFBUGS-10938"
)


# ===========================================================================
# Module A - positive / core behaviour
# ===========================================================================
@mcg
@red_squad
class TestBackingStoreEndpointUpdate:
    """
    Happy-path and matching-semantics coverage for the endpoint update CLI.

    Single-store switch and revert, bulk switch with connection de-duplication,
    endpoint matching semantics (no-match, idempotent no-op, selective matching
    plus the data path of two connections left sharing one endpoint), the
    pause-reconcile annotation's lifecycle, switching under active I/O, and the
    default backingstore.

    The seven that need the update to succeed are skipped on clusters carrying a
    Rook-keyed Ceph object-user secret, where DFBUGS-10975 aborts every update;
    see ``DFBUGS_10975_SKIP``. They still run where no such secret exists, so
    non-RGW clusters keep the coverage.
    """

    @tier1
    @polarion_id("OCS-8278")
    @DFBUGS_10975_SKIP
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

        Teardown: ``store_factory`` deletes the store. The revert in step 3 is
        part of the assertion, not cleanup - the store is removed either way.
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
    @polarion_id("OCS-8279")
    @DFBUGS_10975_SKIP
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

        The stores are left to settle before the CLI runs. Reaching Ready does
        not mean the operator has finished writing to them, and the CLI does not
        re-read on a resource-version conflict (DFBUGS-10937), so firing
        immediately makes this test fail for a reason that has nothing to do
        with de-duplication. That bug has a test of its own -
        :meth:`TestConnectionUpdateNegative.test_switch_survives_concurrent_store_writes`.

        Teardown: ``store_factory`` deletes every store it created, clearing the
        pause-reconcile annotation first if one is still set.
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
        wait_for_stores_quiesced(
            [(constants.BACKINGSTORE, bs.name) for bs in stores], ns
        )

        result = run_connection_update(mcg_obj, old, new)
        assert not result["conflict"], (
            "The CLI hit a resource-version conflict even though the stores had "
            f"settled - see DFBUGS-10937:\n{result['raw']}"
        )
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
    @pytest.mark.parametrize(
        "variant",
        [
            pytest.param("wrong_endpoint", marks=polarion_id("OCS-8280")),
            pytest.param("trailing_slash", marks=polarion_id("OCS-8281")),
            pytest.param("dns_form", marks=polarion_id("OCS-8282")),
            # Performs a real update before the re-run, so DFBUGS-10975 blocks it.
            pytest.param("rerun_after_success", marks=polarion_id("OCS-8283")),
        ],
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
            * dns_form            - the SAME backend the store really sits on,
                                    spelled as its DNS name instead of its IP.
                                    It resolves to the very same service, which
                                    is the point: matching is a string
                                    comparison, not an address comparison, so an
                                    equivalent spelling still matches nothing.
                                    The value is only ever a match key here - the
                                    command aborts before pre-validation - so the
                                    fact that a DNS endpoint would fail
                                    pre-validation is beside the point.
            * rerun_after_success - re-running OLD -> NEW after a successful
                                    switch, when no store sits on OLD any more.

        In every variant the command reports no matching stores, updates nothing,
        and leaves the store's endpoint unchanged.

        The ``dns_form`` case skips unless the endpoint pair supplies ``old_dns``.
        A derived pair always does; an explicit lab override need not.

        Teardown: ``store_factory`` deletes the store. The rerun variant leaves it
        on NEW, which is immaterial - it is deleted either way.
        """
        if variant == "rerun_after_success":
            skip_if_dfbugs_10975()
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        old_dns = endpoint_conf.get("old_dns")
        if variant == "dns_form" and not old_dns:
            pytest.skip(
                "No old_dns in the endpoint pair: this case needs a second, "
                "equivalent spelling of the OLD endpoint (its DNS name where "
                "old is an IP, or the reverse). Add 'old_dns' to "
                "ENV_DATA['mcg_endpoint_pair'] to run it against a "
                "pre-provisioned lab."
            )
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
        elif variant == "dns_form":
            # Guard the premise: if the pair ever hands back the same string for
            # both spellings there is nothing to test, and the assertion below
            # would fail for the wrong reason.
            assert old_dns != old, (
                f"old_dns ({old_dns}) is identical to old ({old}), so this "
                "variant would be testing an exact match, not a mismatch"
            )
            result = run_connection_update(mcg_obj, old_dns, new)
        elif variant == "trailing_slash":
            # Toggle the slash rather than always appending one - an endpoint
            # that already ends in "/" would otherwise be an exact match and
            # the store really would be updated.
            variant_endpoint = old.rstrip("/") if old.endswith("/") else f"{old}/"
            result = run_connection_update(mcg_obj, variant_endpoint, new)
        else:  # rerun_after_success
            # The only variant that performs a real update, so the only one that
            # can hit DFBUGS-10937: the other three match nothing and never
            # write. store_factory waits for Ready, which does not mean the
            # operator has finished writing status, so settle the store first or
            # the setup update aborts on a resource-version conflict and this
            # test fails for a reason that has nothing to do with re-run
            # matching.
            wait_for_stores_quiesced([(constants.BACKINGSTORE, bs.name)], ns)
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
    @polarion_id("OCS-8284")
    @DFBUGS_10975_SKIP
    def test_only_old_endpoint_stores_matched(
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
        Only stores on the OLD endpoint are matched; a store already on the NEW
        endpoint is left untouched - and afterwards the two stores, now sharing
        one endpoint, still serve their own data independently.

        Two connections on one endpoint is not a conflict: NooBaa core treats
        each connection as a separate entity, and the command deliberately does
        not check for one. Asserting that the store which was already on NEW is
        not re-updated only covers the command's bookkeeping, so the data path is
        exercised too - otherwise nothing here would notice the two connections
        colliding once they landed on the same endpoint.

        Flow:
            1. Create one BackingStore on OLD and one already on NEW, each with
               its own target bucket, and put an OBC in front of each.
            2. Write distinct objects through both, so each bucket holds data
               that is identifiable as its own. Every write gets its own source
               directory (:func:`_write_dir`) - the cross-talk check in step 6
               is only meaningful if no write can carry another one's objects
               along with it.
            3. Let both stores quiesce - creating them and putting an OBC in
               front keeps the operator writing to them, and the CLI does not
               re-read on a resource-version conflict (DFBUGS-10937).
            4. Run the connection update OLD -> NEW.
            5. Assert exactly one store (the one on OLD) was updated, both now
               sit on NEW, neither is left paused, and both are OPTIMAL.
            6. Write again through both - the moved store retried over the
               endpoint-config propagation window, as elsewhere in this module -
               then list both buckets and assert two things: every object is
               still readable through the store that wrote it, and neither
               store's objects appear in the other's bucket. Both checks cover
               the post-switch writes as well as the pre-switch ones; the
               post-switch pair is the only data written while the two
               connections shared an endpoint.

        Teardown: ``store_factory`` deletes both stores, ``bucketclass_over_store``
        the BucketClasses, and ``bucket_factory`` / ``test_directory_setup`` the
        OBCs and scratch directory.
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
        obc_old = bucket_factory(
            interface="OC", bucketclass=bucketclass_over_store(bs_old)
        )[0]
        obc_new = bucket_factory(
            interface="OC", bucketclass=bucketclass_over_store(bs_new)
        )[0]

        # Distinct prefixes per store, so an object turning up in the wrong
        # bucket names the store it leaked from - and a directory per write, so
        # that a shared source directory cannot fake such a leak
        # (see :func:`_write_dir`).
        pre_old = set(
            write_random_test_objects_to_bucket(
                awscli_pod_session,
                obc_old.name,
                _write_dir(test_directory_setup.origin_dir, "pre-old"),
                amount=2,
                pattern="from-old-store-",
                mcg_obj=mcg_obj,
            )
        )
        pre_new = set(
            write_random_test_objects_to_bucket(
                awscli_pod_session,
                obc_new.name,
                _write_dir(test_directory_setup.origin_dir, "pre-new"),
                amount=2,
                pattern="from-new-store-",
                mcg_obj=mcg_obj,
            )
        )

        # Both stores have just been created and had an OBC put in front of
        # them, so the operator is still writing to them. The CLI does not
        # re-read on a resource-version conflict (DFBUGS-10937), and that race
        # has a test of its own - let them settle so this one fails only for its
        # own reasons.
        wait_for_stores_quiesced(
            [
                (constants.BACKINGSTORE, bs_old.name),
                (constants.BACKINGSTORE, bs_new.name),
            ],
            ns,
        )

        result = run_connection_update(mcg_obj, old, new)
        assert (
            result["stores_updated"] == 1
        ), f"Only the OLD-endpoint store should be updated:\n{result['raw']}"
        assert get_store_endpoint(constants.BACKINGSTORE, bs_old.name, ns) == new
        assert get_store_endpoint(constants.BACKINGSTORE, bs_new.name, ns) == new
        for store in (bs_old, bs_new):
            assert not get_store_pause_annotation(
                constants.BACKINGSTORE, store.name, ns
            )
            assert mcg_obj.check_backingstore_state(
                store.name, constants.BS_OPTIMAL, timeout=ENDPOINT_PROPAGATION_TIMEOUT
            ), f"BackingStore {store.name} is not OPTIMAL after the switch"

        # Both stores now share endpoint NEW. Write through each again - the
        # moved store only once its endpoint config has propagated - and prove
        # the two connections still address their own buckets.
        #
        # These two objects carry the weight of the cross-talk check below: they
        # are the only ones written while both connections point at the same
        # endpoint, which is when a mix-up could happen. ``_try_write`` reports
        # whether the write went through rather than what it wrote, but it
        # writes exactly one object, so the name is the pattern with index 0.
        post_old_pattern = "post-switch-old-store-"
        post_new_pattern = "post-switch-new-store-"
        post_old = {f"{post_old_pattern}0"}
        post_new = {f"{post_new_pattern}0"}

        for sample in TimeoutSampler(
            ENDPOINT_PROPAGATION_TIMEOUT,
            15,
            _try_write,
            awscli_pod_session,
            obc_old.name,
            _write_dir(test_directory_setup.origin_dir, "post-old"),
            post_old_pattern,
            mcg_obj,
        ):
            if sample:
                break
        assert _try_write(
            awscli_pod_session,
            obc_new.name,
            _write_dir(test_directory_setup.origin_dir, "post-new"),
            post_new_pattern,
            mcg_obj,
        ), (
            f"The store already on {new} stopped serving writes after the other "
            "store joined it on the same endpoint"
        )

        listed_old = set(
            list_objects_from_bucket(awscli_pod_session, obc_old.name, s3_obj=mcg_obj)
        )
        listed_new = set(
            list_objects_from_bucket(awscli_pod_session, obc_new.name, s3_obj=mcg_obj)
        )
        assert not pre_old - listed_old, (
            "Objects written before the switch are no longer readable through "
            f"the moved store: {sorted(pre_old - listed_old)}"
        )
        assert not pre_new - listed_new, (
            "Objects written before the switch are no longer readable through "
            f"the store that never moved: {sorted(pre_new - listed_new)}"
        )
        assert not post_old - listed_old, (
            "The object written through the moved store after the switch is not "
            f"readable back through it: {sorted(post_old - listed_old)}"
        )
        assert not post_new - listed_new, (
            "The object written through the store that never moved is not "
            f"readable back through it: {sorted(post_new - listed_new)}"
        )
        # The real cross-talk check: neither store's data shows up in the other's
        # bucket now that both connections sit on the same endpoint. The
        # post-switch objects are in scope here, not just the pre-switch ones -
        # they were written after the two connections converged, so leaving them
        # out would miss exactly the case this test exists for.
        leaked_into_old = listed_old & (pre_new | post_new)
        assert not leaked_into_old, (
            "Objects written through the store already on NEW leaked into the "
            f"moved store's bucket: {sorted(leaked_into_old)}"
        )
        leaked_into_new = listed_new & (pre_old | post_old)
        assert not leaked_into_new, (
            "Objects written through the moved store leaked into the other "
            f"store's bucket: {sorted(leaked_into_new)}"
        )

    @tier2
    @polarion_id("OCS-8285")
    @DFBUGS_10975_SKIP
    def test_idempotent_no_op(self, mcg_obj, endpoint_conf, store_factory):
        """
        Running the update with ``--new-endpoint`` equal to ``--old-endpoint``
        is a safe no-op.

        Flow:
            1. Create a BackingStore on the OLD endpoint.
            2. Run the connection update with new-endpoint == old-endpoint.
            3. Assert the store is matched and pre-validated, its endpoint is
               unchanged, and no pause-reconcile annotation is left behind.

        Teardown: ``store_factory`` deletes the store.
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
    @polarion_id("OCS-8286")
    @DFBUGS_10975_SKIP
    def test_pause_annotation_honored_and_cleaned_up(
        self, mcg_obj, endpoint_conf, store_factory
    ):
        """
        The noobaa.io/pause-reconcile annotation suspends reconciliation while it
        is set, can be removed by hand, and is not left behind by a successful
        update.

        The annotation is what makes the endpoint switch safe: the CLI sets it on
        every matched store so the operator cannot reconcile a store out from
        under a half-applied change. A pause that is not honored would let the
        operator race the update; a pause that is not cleaned up would leave the
        store permanently unmanaged, which is the failure mode DFBUGS-10743
        produces on NamespaceStores.

        SCOPE: this is the BackingStore annotation lifecycle only - a manual
        pause/resume plus the cleanup on a SUCCESSFUL update. It is NOT a
        regression test for DFBUGS-10743, whose trigger is the webhook rejecting
        the rollback write-back. Reproducing that needs a store whose spec patch
        is denied part-way through a batch, which the happy path never hits.

        Flow:
            1. Create a BackingStore and let it settle.
            2. Set the annotation by hand and confirm the operator starts
               skipping the store, while the store itself stays Ready rather
               than degrading.
            3. Remove the annotation by hand and confirm the skipping stops.
            4. Run a successful update and confirm no annotation is left behind.

        NOTE on what is asserted in steps 2 and 3. The only externally visible
        signal that the operator is honoring the pause is its own log line - it
        skips BEFORE writing anything, so there is no status or spec change to
        observe, and a controller that finds nothing to do is indistinguishable
        from one that is paused. The match is therefore kept deliberately loose
        (the store name and the word "paused"), and step 3 asserts the skipping
        STOPS rather than trying to recognise a normal reconcile. On resume the
        operator does run a full reconcile (phase cycles Verifying -> Connecting
        -> Creating -> Ready), but those phases are transient and the end state
        is Ready either way, so catching them would be a race. Step 4 is what
        independently proves the operator is working again, since a successful
        update needs it.

        Teardown: ``store_factory`` deletes the store, and it clears the
        pause-reconcile annotation first - so a failure between steps 2 and 3
        cannot leave a paused store behind.
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
        store_ocp = OCP(
            kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name
        )
        wait_for_stores_quiesced([(constants.BACKINGSTORE, bs.name)], ns)
        operator_pod = get_noobaa_operator_pod(namespace=ns).name

        # --- honored while set ---
        store_ocp.annotate(annotation=f"{PAUSE_ANNOTATION}=true", resource_name=bs.name)
        assert (
            get_store_pause_annotation(constants.BACKINGSTORE, bs.name, ns) == "true"
        ), "The pause annotation did not stick"
        skipped = False
        for skipped in TimeoutSampler(
            PAUSE_LOG_TIMEOUT,
            PAUSE_LOG_INTERVAL,
            operator_logged_pause_skip,
            operator_pod,
            ns,
            bs.name,
            f"{PAUSE_LOG_TIMEOUT}s",
        ):
            if skipped:
                break
        assert skipped, (
            f"The operator never logged a paused/skipped reconcile for {bs.name} "
            f"within {PAUSE_LOG_TIMEOUT}s, so the annotation is not being honored"
        )
        # A paused store is skipped, not neglected - it must not degrade.
        assert (
            store_ocp.get()["status"]["phase"] == constants.STATUS_READY
        ), "The store left Ready while it was paused"

        # --- manual removal resumes reconciliation ---
        store_ocp.annotate(annotation=f"{PAUSE_ANNOTATION}-", resource_name=bs.name)
        assert not get_store_pause_annotation(
            constants.BACKINGSTORE, bs.name, ns
        ), "The pause annotation survived manual removal"
        time.sleep(PAUSE_RESUME_SETTLE)
        assert not operator_logged_pause_skip(
            operator_pod, ns, bs.name, f"{PAUSE_RESUME_SETTLE - 5}s"
        ), (
            f"The operator is still skipping {bs.name} after the annotation was "
            "removed, so reconciliation did not resume"
        )

        # --- not left behind by a successful update ---
        wait_for_stores_quiesced([(constants.BACKINGSTORE, bs.name)], ns)
        result = run_connection_update(mcg_obj, old, new)
        assert not result["aborted"], result["raw"]
        assert result["stores_updated"] == 1, result["raw"]
        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == new
        assert not get_store_pause_annotation(
            constants.BACKINGSTORE, bs.name, ns
        ), "The update left its pause annotation behind on the store"
        assert mcg_obj.check_backingstore_state(
            bs.name, constants.BS_OPTIMAL, timeout=ENDPOINT_PROPAGATION_TIMEOUT
        ), f"BackingStore {bs.name} did not return to OPTIMAL after the switch"

    @tier2
    @polarion_id("OCS-8287")
    @DFBUGS_10975_SKIP
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

        Teardown: the writer thread is stopped and joined in a ``finally``, so it
        does not outlive the test even if the update raises. A write already in
        flight cannot be cancelled - it is an exec into the awscli pod - so if
        the join times out the overlap is logged as a warning rather than
        silently ignored. The store, BucketClass,
        OBC and scratch directory belong to ``store_factory``,
        ``bucketclass_over_store``, ``bucket_factory`` and
        ``test_directory_setup`` respectively, each of which cleans up its own.
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
            _write_dir(test_directory_setup.origin_dir, "pre"),
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
                    # A directory per round, so each one uploads its own single
                    # object instead of re-syncing everything written so far.
                    write_random_test_objects_to_bucket(
                        awscli_pod_session,
                        bucket.name,
                        _write_dir(
                            test_directory_setup.origin_dir, f"during-{round_nr}"
                        ),
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
            writer.join(timeout=WRITER_JOIN_TIMEOUT)
            if writer.is_alive():
                # The loop checks stop_io only between writes, and a write
                # already in flight cannot be cancelled. Nothing here can force
                # it to end, so say so loudly: a teardown failure just below
                # this line is then attributable rather than mysterious.
                logger.warning(
                    "The I/O thread was still running "
                    f"{WRITER_JOIN_TIMEOUT}s after being asked to stop - a write "
                    "may overlap this test's teardown"
                )
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
            _write_dir(test_directory_setup.origin_dir, "post"),
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
    @polarion_id("OCS-8288")
    @DFBUGS_10975_SKIP
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
            3. Assert the update did not abort, the default store is matched,
               its endpoint is unchanged, and it stays Ready.

        Teardown: none needed. The test creates nothing and, by running the update
        with new-endpoint == old-endpoint, changes nothing on the live default
        store either.
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
        # A same-endpoint update changes nothing by design, so "endpoint
        # unchanged" cannot by itself tell success apart from an abort that made
        # no changes. The abort has to be ruled out explicitly or this test
        # passes on a CLI that did nothing at all.
        assert not result[
            "aborted"
        ], f"Default backingstore update aborted:\n{result['raw']}"
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
    """
    Failure paths: pre-validation aborts, argument handling, direct-edit.

    Pre-validation aborts the whole batch (unreachable endpoint, missing target
    bucket, a bucket that exists but is not a NooBaa location, bad credentials,
    one bad store in a batch), CLI argument handling, the direct-CR-edit path,
    and the update's behaviour when the matched stores are being written to
    concurrently.
    """

    @tier2
    @pytest.mark.parametrize(
        "failure",
        [
            # DFBUGS-10975 aborts on the credentials error before the intended
            # failure is reached, so the asserted reason never appears. The
            # abort itself still happens - only the reason is unassertable.
            pytest.param("unreachable", marks=polarion_id("OCS-8265")),
            pytest.param("missing_target_bucket", marks=polarion_id("OCS-8266")),
            pytest.param("not_noobaa_location", marks=polarion_id("OCS-8267")),
            # Credentials ARE read for this one - the bad-creds secret uses the
            # AWS key names, and fake credentials match no existing secret so
            # the operator never repoints the secretRef. The rejection is real
            # and merely misreported, which makes it DFBUGS-10938, not 10975.
            pytest.param(
                "wrong_creds",
                marks=[
                    polarion_id("OCS-8268"),
                    jira("DFBUGS-10938"),
                    DFBUGS_10938_SKIP,
                ],
            ),
            pytest.param("one_bad_in_batch", marks=polarion_id("OCS-8269")),
        ],
    )
    def test_prevalidation_failure_aborts_batch(
        self, mcg_obj, endpoint_conf, store_factory, bad_creds_secret, failure
    ):
        """
        Any pre-validation failure aborts the WHOLE batch before any spec change
        - nothing is patched and no pause-reconcile annotation is left behind
        (all-or-nothing).

        Failure variants:
            * unreachable           - the NEW endpoint does not resolve/connect.
            * missing_target_bucket - the store's target bucket does not exist
                                      on the NEW endpoint at all.
            * not_noobaa_location   - the store's target bucket DOES exist on
                                      the NEW endpoint but holds no
                                      ``noobaa_blocks/`` prefix, so it is not a
                                      valid NooBaa location.
            * wrong_creds           - the store's secret has invalid credentials.
            * one_bad_in_batch      - two stores are matched and one of them
                                      fails pre-validation.

        Each variant asserts the command aborts, that the reported failure is the
        expected one, and that every matched store stays on OLD with no pause
        annotation.

        Both the status code and the error text are asserted. The text matters
        because core's classification is coarse: a missing bucket, a DNS failure
        and an unreachable host all come back as UNKNOWN_FAILURE, with the real
        reason only in the message (DFBUGS-10938). The two bucket variants are
        kept apart precisely because they are DIFFERENT conditions that core
        happens to report differently - a bucket that is absent falls through to
        UNKNOWN_FAILURE, while a bucket that exists but is not a NooBaa location
        is reported as INVALID_ENDPOINT.

        Teardown: every store comes from ``store_factory`` and the bad-creds
        secret from ``bad_creds_secret``, both of which clean up after
        themselves. The variants that retarget a store only patch its spec, so
        they leave nothing extra behind.
        """
        if failure in _DFBUGS_10975_BLOCKED_PREVALIDATION:
            skip_if_dfbugs_10975()
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
            expected_statuses = UNREACHABLE_STATUSES
            expected_error = "ENOTFOUND"
            stores = [bs]
        elif failure == "missing_target_bucket":
            bs = store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_bucket"],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            # Retarget the store at a bucket that exists on NEITHER endpoint.
            absent_bucket = create_unique_resource_name("absent", "bucket")
            OCP(kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name).patch(
                params=(
                    f'{{"spec":{{"s3Compatible":{{"targetBucket":'
                    f'"{absent_bucket}"}}}}}}'
                ),
                format_type="merge",
            )
            result = run_connection_update(mcg_obj, old, new)
            expected_statuses = UNREACHABLE_STATUSES
            expected_error = "bucket does not exist"
            stores = [bs]
        elif failure == "not_noobaa_location":
            empty_bucket = endpoint_conf.get("empty_bucket")
            if not empty_bucket:
                pytest.skip(
                    "No empty_bucket in the endpoint pair: this case needs a "
                    "bucket that EXISTS on both endpoints but carries no "
                    "noobaa_blocks/ prefix. Add 'empty_bucket' to "
                    "ENV_DATA['mcg_endpoint_pair'] to run it against a "
                    "pre-provisioned lab."
                )
            bs = store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_bucket"],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            # Retarget the store at a bucket that is real but holds no NooBaa
            # blocks, so core reaches it, lists it, and rejects its CONTENT.
            OCP(kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name).patch(
                params=(
                    f'{{"spec":{{"s3Compatible":{{"targetBucket":'
                    f'"{empty_bucket}"}}}}}}'
                ),
                format_type="merge",
            )
            result = run_connection_update(mcg_obj, old, new)
            # Core throws a synthetic UnknownEndpoint here, which its error map
            # turns into INVALID_ENDPOINT even though the endpoint is fine -
            # the overload is tracked in DFBUGS-10938.
            expected_statuses = ("INVALID_ENDPOINT",)
            expected_error = "valid location used by noobaa"
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
            expected_statuses = ("INVALID_CREDENTIALS",)
            expected_error = "access key"
            stores = [bs]
        else:  # one_bad_in_batch
            good = store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_bucket"],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            # The bad one targets a bucket that exists on neither endpoint.
            bad = store_factory(
                constants.BACKINGSTORE,
                old,
                create_unique_resource_name("absent", "bucket"),
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
                wait=False,
            )
            result = run_connection_update(mcg_obj, old, new)
            expected_statuses = UNREACHABLE_STATUSES
            expected_error = "bucket does not exist"
            stores = [good, bad]

        assert result["aborted"], (
            f"Expected the batch to abort ('No changes have been made'):\n"
            f"{result['raw']}"
        )
        assert result["prevalidation_failed"]
        assert any(status in result["status_codes"] for status in expected_statuses), (
            f"Expected one of {list(expected_statuses)}, "
            f"got {result['status_codes']}"
        )
        # The status code alone is coarse - core buckets several distinct
        # problems under UNKNOWN_FAILURE - so pin the reason down by its message.
        assert (
            expected_error in result["raw"]
        ), f"Expected {expected_error!r} in the failure reason:\n{result['raw']}"
        assert not result["stores_updated"]
        # All-or-nothing: every store stays on OLD with no pause annotation.
        for bs in stores:
            assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old
            assert not get_store_pause_annotation(constants.BACKINGSTORE, bs.name, ns)

    @tier3
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param("missing_flag", marks=polarion_id("OCS-8270")),
            # Asserts the padded endpoint PASSES pre-validation, which
            # DFBUGS-10975 prevents.
            pytest.param("whitespace_trimmed", marks=polarion_id("OCS-8271")),
            pytest.param("malformed_url", marks=polarion_id("OCS-8272")),
            pytest.param("long_url", marks=polarion_id("OCS-8273")),
        ],
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
            * long_url           - an absurdly long but syntactically plausible
                                   host is rejected the same way: a clean abort,
                                   no crash, no partial state.

        The two rejection cases assert the abort and the untouched store rather
        than a status code. Core reports both as UNKNOWN_FAILURE today and the
        real reason is only in the message text - that coarseness is
        DFBUGS-10938, and pinning a code here would just re-assert the bug.

        Teardown: ``store_factory`` deletes the store. The whitespace variant is
        the one case that really does move it to NEW; that is the point of the
        case and makes no difference to cleanup.
        """
        if case == "whitespace_trimmed":
            skip_if_dfbugs_10975()
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
            # Raw CompletedProcess here, not the parsed dict - .returncode is a
            # real attribute. Read it directly rather than via getattr(), whose
            # default would quietly satisfy this assertion if it ever went away.
            assert result.returncode != 0, (
                "Omitting --new-endpoint was accepted instead of producing a "
                f"usage error:\n{result.stdout}\n{result.stderr}"
            )
            assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old
        elif case == "whitespace_trimmed":
            # Pad both flags with surrounding spaces; the CLI must trim them.
            # Matching the store on a padded --old-endpoint is what proves the
            # trimming happened - an untrimmed value would match nothing - so
            # that is what is asserted, rather than the outcome of the update
            # itself, which this test is not about.
            result = run_connection_update(mcg_obj, f'"  {old}  "', f'"  {new}  "')
            assert not result["no_match"], (
                "Padded --old-endpoint matched no stores, so the surrounding "
                f"whitespace was not trimmed:\n{result['raw']}"
            )
            assert result["matched"] == 1, (
                f"Expected the padded endpoint to match 1 store, got "
                f"{result['matched']}:\n{result['raw']}"
            )
            assert result["prevalidation_passed"], (
                "Padded --new-endpoint failed pre-validation, so the "
                f"surrounding whitespace was not trimmed:\n{result['raw']}"
            )
        else:  # malformed_url / long_url
            bad_new = (
                "http://"
                if case == "malformed_url"
                else f"http://{'x' * MALFORMED_LONG_HOST_LEN}:9000"
            )
            result = run_connection_update(mcg_obj, old, bad_new)
            # run_connection_update returns the PARSED dict, so the return code
            # is a key - not an attribute. getattr() on a dict would always fall
            # through to its default and make this assertion vacuously true.
            rc = result["returncode"]
            assert result["aborted"] or (rc is not None and rc != 0), (
                f"Bad --new-endpoint ({case}) was not rejected - the command "
                f"reported no abort and exited {rc}:\n{result['raw']}"
            )
            assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old
            assert not get_store_pause_annotation(constants.BACKINGSTORE, bs.name, ns)

    @tier2
    @polarion_id("OCS-8274")
    def test_direct_cr_edit_is_rejected_by_webhook(
        self, mcg_obj, endpoint_conf, store_factory
    ):
        """
        A DIRECT ``oc patch`` of the endpoint (bypassing the CLI) is denied by
        the NooBaa admission webhook, so the CLI is the only way to move a store
        between endpoints.

        This closes the hole behind DFBUGS-10346, where the direct edit was
        allowed through: it changed the CR spec while NooBaa core carried on
        serving the old endpoint, leaving the store's declared and effective
        endpoints silently out of step. Rejecting the edit outright means that
        divergence can no longer be created.

        The store is checked afterwards to confirm the denial left nothing
        behind - the spec still holds OLD and core still serves OLD.

        Teardown: ``store_factory`` deletes the store. The rejected patch never
        landed, so there is nothing else to undo.
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

        with pytest.raises(CommandFailed) as denied:
            OCP(kind=constants.BACKINGSTORE, namespace=ns, resource_name=bs.name).patch(
                params=f'{{"spec":{{"s3Compatible":{{"endpoint":"{new}"}}}}}}',
                format_type="merge",
            )
        message = str(denied.value)
        assert "admissionwebhook.noobaa.io" in message and "denied" in message, (
            "Expected the NooBaa admission webhook to deny the direct edit, got "
            f"a different failure:\n{message}"
        )

        assert get_store_endpoint(constants.BACKINGSTORE, bs.name, ns) == old
        assert get_pool_endpoint(mcg_obj, bs.name) == old

    @tier3
    @polarion_id("OCS-8275")
    @jira("DFBUGS-10937")
    def test_switch_survives_concurrent_store_writes(
        self, mcg_obj, endpoint_conf, store_factory, request
    ):
        """
        The update must survive somebody else writing to the matched stores while
        it runs.

        The CLI lists the stores once at the start and then writes back the
        copies it is holding. Anything that touches a store in between - the
        operator updating status or conditions, a user adding a label - bumps its
        ``metadata.resourceVersion`` and makes those writes stale. A stale write
        is a routine, retryable Kubernetes condition, so the command is expected
        to re-read and carry on rather than give up.

        Flow:
            1. Create several BackingStores on the OLD endpoint.
            2. Start a background thread that keeps re-annotating them, so every
               write the CLI attempts races a foreign update. The stores are
               deliberately NOT left to settle first - unlike
               :meth:`TestBackingStoreEndpointUpdate.test_bulk_switch_and_connection_dedup`,
               contention is the point here.
            3. Run one connection update OLD -> NEW.
            4. Stop the churn and assert the command neither reported a conflict
               nor aborted, that every store reached NEW, and that none was left
               paused.

        Known failure (DFBUGS-10937): ``util.KubeUpdate`` issues a single
        ``Update`` and, on conflict, logs ``Conflict:`` and returns false without
        re-reading the object. The three call sites in the connection package
        treat that as terminal - ``setPauseAnnotation`` and ``patchEndpoints``
        abort the run, and a conflict during ``rollback`` is only logged, which
        can leave part of the batch on OLD and part on NEW and breaks the
        documented all-or-nothing contract.

        Teardown: the churn thread is stopped and joined, and the annotations it
        wrote are removed, through finalizers registered before the thread
        starts, so they run even if an assertion fails. The stores themselves
        belong to ``store_factory``.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        stores = [
            store_factory(
                constants.BACKINGSTORE,
                old,
                endpoint_conf["target_buckets"][
                    i % len(endpoint_conf["target_buckets"])
                ],
                endpoint_conf["secret"],
                endpoint_conf["signature_version"],
            )
            for i in range(BULK_STORE_COUNT)
        ]
        names = [bs.name for bs in stores]

        def _remove_churn_annotations():
            for name in names:
                try:
                    OCP(kind=constants.BACKINGSTORE, namespace=ns).annotate(
                        annotation=f"{CHURN_ANNOTATION}-", resource_name=name
                    )
                except Exception as ex:  # noqa - best-effort teardown
                    logger.warning(
                        f"Could not clear the churn annotation on {name}: {ex}"
                    )

        stop_churn = threading.Event()

        def _churn():
            counter = 0
            while not stop_churn.is_set():
                counter += 1
                for name in names:
                    try:
                        OCP(kind=constants.BACKINGSTORE, namespace=ns).annotate(
                            annotation=f"{CHURN_ANNOTATION}={counter}",
                            resource_name=name,
                        )
                    except Exception as ex:  # noqa - losing a race is the point
                        logger.debug(f"Churn write on {name} did not land: {ex}")
                stop_churn.wait(CHURN_INTERVAL)

        churn_thread = threading.Thread(target=_churn, daemon=True)

        def _stop_churn():
            stop_churn.set()
            churn_thread.join(timeout=CHURN_JOIN_TIMEOUT)

        # Registered before the thread starts, and in this order, so the churn is
        # stopped before its annotations are cleared (finalizers run LIFO).
        request.addfinalizer(_remove_churn_annotations)
        request.addfinalizer(_stop_churn)
        churn_thread.start()

        result = run_connection_update(mcg_obj, old, new)
        _stop_churn()

        assert not result["conflict"], (
            "The CLI gave up on a retryable resource-version conflict instead of "
            f"re-reading and retrying - DFBUGS-10937:\n{result['raw']}"
        )
        assert not result[
            "aborted"
        ], f"The update aborted under concurrent writes:\n{result['raw']}"
        assert result["stores_updated"] == len(stores), (
            f"Expected {len(stores)} stores updated, got "
            f"{result['stores_updated']}:\n{result['raw']}"
        )
        # A conflict during rollback is only logged, so the batch can end up
        # split across the two endpoints. Check every store, not just the count.
        for name in names:
            assert get_store_endpoint(constants.BACKINGSTORE, name, ns) == new, (
                f"{name} was left behind on the old endpoint - the batch is no "
                f"longer all-or-nothing:\n{result['raw']}"
            )
            assert not get_store_pause_annotation(
                constants.BACKINGSTORE, name, ns
            ), f"{name} was left paused after the update"


# ===========================================================================
# Module C - NamespaceStore + mixed batch
# ===========================================================================
@mcg
@red_squad
class TestNamespaceStoreEndpointUpdate:
    """
    Endpoint update for NamespaceStores, on their own and in a batch alongside
    BackingStores.

    Both tests assert the intended behaviour - a successful switch.

    These were skipped while DFBUGS-10744 was open, because the admission
    webhook denied every NamespaceStore endpoint change. It was fixed by
    noobaa-operator PR #2114, which lets the webhook through for a store
    carrying ``noobaa.io/pause-reconcile=true`` - the annotation the CLI sets
    for the duration of the update - and denies the change otherwise. Verified
    present in ODF/MCG 5.0.0-45.stable, so the skip marker is gone.

    Note the fix landed as "allow the change", not as "exclude NamespaceStores
    from matching", which is what makes a successful switch the right thing to
    assert here.
    """

    @tier2
    @polarion_id("OCS-8276")
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

        Teardown: ``store_factory`` deletes the store, clearing the
        pause-reconcile annotation first - which matters here, because a
        NamespaceStore left paused by DFBUGS-10743 will not delete cleanly
        otherwise.
        """
        ns = config.ENV_DATA["cluster_namespace"]
        old, new = endpoint_conf["old"], endpoint_conf["new"]
        nss = store_factory(
            constants.NAMESPACESTORE,
            old,
            endpoint_conf["target_bucket"],
            endpoint_conf["secret"],
            endpoint_conf["signature_version_nss"],
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
    @polarion_id("OCS-8277")
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

        Teardown: ``store_factory`` deletes both stores, clearing the
        pause-reconcile annotation first so that a store left paused by
        DFBUGS-10743 still deletes cleanly. Step 3 only retargets the
        BackingStore's bucket, which goes away with the store.
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
            endpoint_conf["signature_version_nss"],
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
