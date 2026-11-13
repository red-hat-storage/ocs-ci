import base64
import json
import logging

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ec, padding

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    tier4a,
    red_squad,
    mcg,
    runs_on_provider,
    pre_upgrade,
    post_upgrade,
    polarion_id,
)
from ocs_ci.helpers.helpers import (
    get_noobaa_db_credentials_from_secret,
    setup_pod_directories,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.bucket_utils import (
    compare_directory,
    sync_object_directory,
    verify_s3_object_integrity,
    write_random_objects_in_pod,
)
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    delete_pods,
    get_active_core_pod,
    get_noobaa_core_pods,
    get_noobaa_operator_pod,
    get_standby_core_pod,
    wait_for_pods_to_be_running,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    TimeoutSampler,
    get_primary_nb_db_pod,
    get_secondary_nb_db_pod,
)

logger = logging.getLogger(__name__)

# pytest cache keys used to carry state from the pre-upgrade test to the
# post-upgrade test. The two phases run as separate tests within the SAME pytest
# session (the upgrade test is ordered between them), and the pytest cache is the
# idiomatic way to persist JSON-serializable values across them.
CACHE_KEY_BUCKET = "noobaa_db_tls/pre_upgrade_bucket"
CACHE_KEY_OBJECTS = "noobaa_db_tls/pre_upgrade_objects"
CACHE_KEY_ORIGIN_DIR = "noobaa_db_tls/pre_upgrade_origin_dir"


def get_noobaa_container_envs():
    """
    Collect the environment variable definitions from the NooBaa core and both
    NooBaa DB (CNPG primary and secondary) containers.

    Returns:
        dict: Mapping of a human readable source name to its list of env var
            definitions (each a dict with ``name`` and either ``value`` or
            ``valueFrom``).

    """
    core_env = (
        OCP(
            kind=constants.STATEFULSET,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_CORE_STATEFULSET,
        )
        .get()["spec"]["template"]["spec"]["containers"][0]
        .get("env", [])
    )
    db_primary_env = (
        get_primary_nb_db_pod().get()["spec"]["containers"][0].get("env", [])
    )
    db_secondary_env = (
        get_secondary_nb_db_pod().get()["spec"]["containers"][0].get("env", [])
    )
    return {
        constants.NOOBAA_CORE_STATEFULSET: core_env,
        "noobaa-db-cnpg-primary": db_primary_env,
        "noobaa-db-cnpg-secondary": db_secondary_env,
    }


def get_noobaa_core_env_map():
    """
    Build a ``{name: value}`` map of the NooBaa core container's env vars.

    Returns:
        dict: Mapping of env var name to its literal ``value`` (None when the var
            is sourced via ``valueFrom`` rather than a literal value).

    """
    core_env = get_noobaa_container_envs()[constants.NOOBAA_CORE_STATEFULSET]
    return {env["name"]: env.get("value") for env in core_env}


def sync_with_retry(awscli_pod, src, dst, mcg_obj):
    """
    Run :func:`sync_object_directory` with a retry on transient CommandFailed
    (S3 operations can briefly fail while pods are restarting).
    """
    return retry(CommandFailed, tries=3, delay=10)(sync_object_directory)(
        awscli_pod, src, dst, mcg_obj
    )


def wait_for_noobaa_ready(timeout=300, sleep=15):
    """
    Wait for the NooBaa CR to report ``status.phase == Ready``.

    Args:
        timeout (int): Overall timeout in seconds.
        sleep (int): Seconds between checks.

    Returns:
        bool: True if NooBaa reached Ready within the timeout, False otherwise.

    """
    nb_cr_ocp = OCP(
        kind=constants.NOOBAA_RESOURCE_NAME,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.NOOBAA_RESOURCE_NAME,
    )
    try:
        for phase in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=lambda: nb_cr_ocp.get().get("status", {}).get("phase"),
        ):
            if phase == "Ready":
                logger.info("NooBaa CR is Ready")
                return True
    except TimeoutExpiredError:
        logger.error(f"NooBaa CR did not reach Ready within {timeout}s")
        return False


def verify_cnpg_tls_secrets():
    """
    Identify the CNPG-generated cert secrets and check they exist without any
    manual cert creation. CNPG auto-generates them, so each must have the
    expected type and key material and be owned by the CNPG Cluster CR. Note the
    CA secret is Opaque (ca.crt/ca.key) while the server and replication secrets
    are kubernetes.io/tls (tls.crt/tls.key).

    Returns:
        bool: True if all expected CNPG cert secrets exist with the correct
            type/keys and are owned by the CNPG Cluster, False otherwise.

    """
    namespace = config.ENV_DATA["cluster_namespace"]
    secret_ocp = OCP(kind=constants.SECRET, namespace=namespace)
    logger.info(
        "Validating cert secrets of the CNPG cluster "
        f"'{constants.NB_DB_CNPG_CLUSTER_NAME}' in namespace '{namespace}'"
    )
    valid = True
    for name, expected in constants.NB_DB_CNPG_TLS_SECRETS.items():
        secret = secret_ocp.get(resource_name=name, dont_raise=True)
        if secret is None:
            logger.error(
                f"Expected CNPG cert secret '{name}' does not exist; CNPG should "
                "have created it automatically (no manual cert creation)"
            )
            valid = False
            continue

        secret_type = secret.get("type")
        if secret_type != expected["type"]:
            logger.error(
                f"CNPG cert secret '{name}' has type '{secret_type}', "
                f"expected '{expected['type']}'"
            )
            valid = False

        data = secret.get("data", {})
        missing_keys = [key for key in expected["keys"] if key not in data]
        if missing_keys:
            logger.error(
                f"CNPG cert secret '{name}' is missing key material {missing_keys}; "
                f"keys present = {sorted(data)}"
            )
            valid = False

        # Confirm the secret was generated by the CNPG operator (owned by the
        # CNPG Cluster CR) rather than created manually.
        owner_kinds = [
            ref.get("kind") for ref in secret["metadata"].get("ownerReferences", [])
        ]
        if "Cluster" not in owner_kinds:
            logger.error(
                f"CNPG cert secret '{name}' is not owned by a CNPG Cluster "
                f"(ownerReferences kinds: {owner_kinds}); it may have been "
                "created manually"
            )
            valid = False
        else:
            logger.info(
                f"CNPG cert secret '{name}' OK "
                f"(type={secret_type}, keys={sorted(data)})"
            )
    if valid:
        logger.info(
            "All expected CNPG cert secrets exist with the correct type/keys and "
            "are owned by the CNPG Cluster (auto-generated, no manual certs)"
        )
    return valid


def verify_postgres_ssl_required():
    """
    Check whether the NooBaa core pod is configured to require PostgreSQL TLS.
    The noobaa-operator sets POSTGRES_SSL_REQUIRED=true (and
    POSTGRES_SSL_UNAUTHORIZED=true) when CNPG TLS is enabled.

    Returns:
        bool: True if POSTGRES_SSL_REQUIRED is set and enabled on the NooBaa
            core pod, False otherwise.

    """
    core_env_map = get_noobaa_core_env_map()
    ssl_required = core_env_map.get(constants.POSTGRES_SSL_REQUIRED_ENV)
    logger.info(
        f"NooBaa core {constants.POSTGRES_SSL_REQUIRED_ENV}={ssl_required}, "
        f"{constants.POSTGRES_SSL_UNAUTHORIZED_ENV}="
        f"{core_env_map.get(constants.POSTGRES_SSL_UNAUTHORIZED_ENV)}"
    )
    if constants.POSTGRES_SSL_REQUIRED_ENV not in core_env_map:
        logger.error(
            f"{constants.POSTGRES_SSL_REQUIRED_ENV} env var is not set on the "
            f"NooBaa core container; env vars present: {sorted(core_env_map)}"
        )
        return False
    if str(ssl_required).lower() not in ("true", "1"):
        logger.error(
            f"{constants.POSTGRES_SSL_REQUIRED_ENV} is not enabled on the NooBaa "
            f"core pod (value={ssl_required})"
        )
        return False
    return True


def run_psql_on_nb_db(query, sslmode, host=constants.CNPG_READ_WRITE_HOST):
    """
    Run a psql query against the NooBaa DB from inside the primary DB pod, over
    TCP to the given service with the requested sslmode. Connecting over TCP (not
    the local unix socket, which is never encrypted) is what exercises the TLS /
    pg_hba policy.

    Args:
        query (str): The SQL to execute (passed to ``psql -tAc``).
        sslmode (str): The libpq sslmode (e.g. ``require`` or ``disable``).
        host (str): The NooBaa DB service to connect to.

    Returns:
        str: The stripped stdout of the query.

    Raises:
        CommandFailed: If psql fails to connect or the query errors (the
            exception message carries psql's stderr).

    """
    db_user, db_password = get_noobaa_db_credentials_from_secret()
    nb_db_pod = get_primary_nb_db_pod()
    psql_cmd = (
        f"PGPASSWORD='{db_password}' psql "
        f"'host={host} dbname=nbcore user={db_user} sslmode={sslmode}' "
        f"-tAc '{query}'"
    )
    return nb_db_pod.exec_sh_cmd_on_pod(command=psql_cmd, secrets=[db_password]).strip()


def verify_encrypted_db_connection(host=constants.CNPG_READ_WRITE_HOST):
    """
    Check whether the connection to the NooBaa DB is actually encrypted. Connect
    over TCP to the given service (read-write or read-only) with sslmode=require
    (require makes the connection fail rather than silently fall back to plaintext
    if TLS is unavailable). ssl_is_used() needs the optional sslinfo extension
    (not installed), so use the built-in pg_stat_ssl view scoped to our own
    backend.

    Args:
        host (str): The NooBaa DB service to connect to (defaults to the CNPG
            read-write service). Pass constants.CNPG_READ_ONLY_HOST to validate
            the read-only replica endpoint.

    Returns:
        bool: True if the current backend connection is using TLS
            (pg_stat_ssl.ssl == 't'), False otherwise.

    """
    ssl_query = "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid();"
    # With sslmode=require, a DB that does not serve TLS makes psql fail to
    # connect (non-zero exit), which exec_sh_cmd_on_pod surfaces as a
    # CommandFailed. Treat that as "TLS not in use" and return False so the
    # helper keeps its boolean contract instead of erroring the test.
    try:
        ssl_used = run_psql_on_nb_db(ssl_query, sslmode="require", host=host).lower()
    except CommandFailed as ex:
        logger.error(f"Failed to query pg_stat_ssl over TLS on host '{host}': {ex}")
        return False
    logger.info(
        f"pg_stat_ssl.ssl for the current backend on host '{host}' "
        f"returned: {ssl_used}"
    )
    if ssl_used != "t":
        logger.error(
            f"Encrypted (TLS) connection to the NooBaa DB on host '{host}' is "
            f"not in use; pg_stat_ssl.ssl returned '{ssl_used}' instead of 't'"
        )
        return False
    return True


def verify_tls_in_use(host=constants.CNPG_READ_WRITE_HOST):
    """
    Convenience check combining the two assertions used throughout the
    disruption/rotation tests: the NooBaa core requires PostgreSQL TLS
    (POSTGRES_SSL_REQUIRED) and the DB connection is actually encrypted
    (pg_stat_ssl.ssl == 't'). Each underlying helper logs its own diagnostics.

    Args:
        host (str): The NooBaa DB service to validate the encrypted connection on.

    Returns:
        bool: True only if TLS is required AND the connection is encrypted.

    """
    return verify_postgres_ssl_required() and verify_encrypted_db_connection(host)


def verify_no_tls_key_material_in_env():
    """
    Check that no TLS key material is exposed via env vars. The certs must be
    mounted from secrets (referenced via valueFrom.secretKeyRef or as volume
    mounts), never inlined as literal PEM values in the env.

    Returns:
        bool: True if no raw TLS key/cert material is found in the NooBaa core
            or DB container env vars, False otherwise.

    """
    # PEM markers that would indicate raw TLS key/cert material in an env var
    pem_markers = (
        "BEGIN PRIVATE KEY",
        "BEGIN RSA PRIVATE KEY",
        "BEGIN EC PRIVATE KEY",
        "BEGIN ENCRYPTED PRIVATE KEY",
        "BEGIN CERTIFICATE",
    )
    no_key_material = True
    for source, env_vars in get_noobaa_container_envs().items():
        for env in env_vars:
            value = env.get("value")
            if not value:
                continue
            for marker in pem_markers:
                if marker in value:
                    logger.error(
                        f"TLS key/cert material leaked into env var "
                        f"'{env['name']}' on {source} (found '{marker}')"
                    )
                    no_key_material = False
    if no_key_material:
        logger.info(
            "No raw TLS key/cert material is exposed in the NooBaa core or DB "
            "container environment variables"
        )
    return no_key_material


def verify_both_postgres_ssl_envs_true():
    """
    For the internal CNPG DB, the noobaa-operator must enable PostgreSQL TLS by
    setting BOTH POSTGRES_SSL_REQUIRED and POSTGRES_SSL_UNAUTHORIZED to "true" on
    the NooBaa core container. POSTGRES_SSL_REQUIRED forces TLS (no plaintext
    fallback) and POSTGRES_SSL_UNAUTHORIZED accepts the CNPG self-signed server
    cert without client-side CA verification.

    Returns:
        bool: True only if both env vars are present and equal to "true" on the
            NooBaa core container, False otherwise.

    """
    core_env_map = get_noobaa_core_env_map()
    all_true = True
    for env_name in (
        constants.POSTGRES_SSL_REQUIRED_ENV,
        constants.POSTGRES_SSL_UNAUTHORIZED_ENV,
    ):
        value = core_env_map.get(env_name)
        logger.info(f"NooBaa core {env_name}={value}")
        if str(value).lower() != "true":
            logger.error(
                f"{env_name} is not set to 'true' on the NooBaa core container "
                f"(value={value}); env vars present: {sorted(core_env_map)}"
            )
            all_true = False
    return all_true


def verify_no_tls_mode_field_in_noobaa_cr():
    """
    For the internal CNPG DB the operator owns the TLS lifecycle, so the NooBaa
    CR must NOT expose any user-facing TLS/SSL mode knob in its spec (in
    particular under ``spec.dbSpec``). Its absence is what guarantees TLS cannot
    be weakened through the NooBaa CR.

    Returns:
        bool: True if no TLS/SSL mode field is present anywhere in the NooBaa CR
            spec, False otherwise.

    """
    nb_cr = OCP(
        kind=constants.NOOBAA_RESOURCE_NAME,
        namespace=config.ENV_DATA["cluster_namespace"],
        resource_name=constants.NOOBAA_RESOURCE_NAME,
    ).get()
    spec = nb_cr.get("spec", {})

    offending = []

    def _walk(node, path):
        if isinstance(node, dict):
            for key, value in node.items():
                lowered = key.lower()
                if "tls" in lowered or "ssl" in lowered:
                    offending.append(f"{path}.{key}")
                _walk(value, f"{path}.{key}")
        elif isinstance(node, list):
            for idx, value in enumerate(node):
                _walk(value, f"{path}[{idx}]")

    _walk(spec, "spec")
    if offending:
        logger.error(
            f"NooBaa CR spec unexpectedly contains TLS/SSL mode field(s): "
            f"{offending}; the internal CNPG DB must not expose a TLS mode knob"
        )
        return False
    logger.info(
        "No TLS/SSL mode field exists in the NooBaa CR spec for the internal "
        "CNPG PG (operator owns the TLS lifecycle)"
    )
    return True


# Substrings that identify a genuine "connection refused because it was not
# encrypted" error from libpq/pg_hba, as opposed to an unrelated failure (psql
# missing, DNS/pod not ready, bad credentials, etc.). We must not treat those
# unrelated failures as proof that plaintext was refused.
PLAINTEXT_REJECTION_MARKERS = ("pg_hba.conf rejects", "no encryption", "no pg_hba")


def verify_db_refuses_plaintext(host=constants.CNPG_READ_WRITE_HOST):
    """
    Prove there is no plaintext fallback: attempt a connection to the NooBaa DB
    with ``sslmode=disable`` and confirm it is refused *specifically because the
    connection is unencrypted*. CNPG is configured with ``pg_hba``
    ``hostnossl ... reject``, so an unencrypted connection must be rejected rather
    than downgraded.

    A bare connection failure is not sufficient evidence: psql being missing, the
    pod not being ready, DNS failing, or wrong credentials would all raise
    CommandFailed too. Only an error that matches a pg_hba "no encryption"
    rejection counts as the DB refusing plaintext.

    Args:
        host (str): The NooBaa DB service to connect to.

    Returns:
        bool: True only if the plaintext connection was refused by pg_hba for
            being unencrypted. False if it unexpectedly succeeded, or if it failed
            for an unrelated/indeterminate reason.

    """
    try:
        out = run_psql_on_nb_db("SELECT 1;", sslmode="disable", host=host)
    except CommandFailed as ex:
        if any(marker in str(ex).lower() for marker in PLAINTEXT_REJECTION_MARKERS):
            logger.info(
                f"Plaintext (sslmode=disable) connection to host '{host}' was "
                f"rejected by pg_hba for being unencrypted (no fallback): {ex}"
            )
            return True
        logger.error(
            f"Plaintext (sslmode=disable) connection to host '{host}' failed, but "
            f"not with a pg_hba 'no encryption' rejection; the failure reason is "
            f"indeterminate and is NOT accepted as proof of no-fallback: {ex}"
        )
        return False
    logger.error(
        f"Plaintext (sslmode=disable) connection to host '{host}' unexpectedly "
        f"succeeded (returned '{out}'); the DB allowed an unencrypted fallback"
    )
    return False


def verify_tls_enforced_no_plaintext_fallback(host=constants.CNPG_READ_WRITE_HOST):
    """
    Verify TLS is enforced end-to-end with no unencrypted fallback on the given
    DB service: the TLS (sslmode=require) connection works and the plaintext
    (sslmode=disable) connection is refused by pg_hba.

    Args:
        host (str): The NooBaa DB service to connect to.

    Returns:
        bool: True only if the encrypted connection works AND plaintext is
            refused, False otherwise.

    """
    encrypted_ok = verify_encrypted_db_connection(host)
    plaintext_refused = verify_db_refuses_plaintext(host)
    return encrypted_ok and plaintext_refused


def _load_cnpg_cert(secret_ocp, secret_name, cert_key):
    """
    Load an X.509 certificate out of a CNPG-managed secret.

    Args:
        secret_ocp (OCP): OCP handle for secrets in the cluster namespace.
        secret_name (str): Secret to read (server or CA secret).
        cert_key (str): Key under ``data`` holding the PEM cert
            (``tls.crt`` for the server secret, ``ca.crt`` for the CA secret).

    Returns:
        cryptography.x509.Certificate: The parsed certificate, or None if the
            secret/key is missing.

    """
    # dont_raise: a momentarily-absent secret yields None so pollers keep sampling.
    secret = secret_ocp.get(resource_name=secret_name, dont_raise=True)
    cert_b64 = (secret or {}).get("data", {}).get(cert_key)
    if not cert_b64:
        logger.warning(f"Cert key '{cert_key}' not available in secret '{secret_name}'")
        return None
    # A secret caught mid-rotation may hold partial data; return None so pollers retry.
    try:
        return x509.load_pem_x509_certificate(base64.b64decode(cert_b64))
    except Exception as e:
        logger.warning(f"Failed to load cert from {secret_name}/{cert_key}: {e}")
        return None


def get_cnpg_cert_serials(secret_ocp):
    """
    Collect the current server-leaf and CA certificate serial numbers from the
    CNPG-managed secrets, using the ``cryptography`` library (no shelling out to
    openssl, no temp files).

    Args:
        secret_ocp (OCP): OCP handle for secrets in the cluster namespace.

    Returns:
        dict: ``{"server": <hex serial or None>, "ca": <hex serial or None>}``.

    """
    sources = {
        "server": (constants.NB_DB_CNPG_SERVER_SECRET, "tls.crt"),
        "ca": (constants.NB_DB_CNPG_CA_SECRET, "ca.crt"),
    }
    serials = {}
    for role, (secret_name, cert_key) in sources.items():
        cert = _load_cnpg_cert(secret_ocp, secret_name, cert_key)
        serials[role] = format(cert.serial_number, "x") if cert else None
        logger.info(f"{role} cert serial ({secret_name}): {serials[role]}")
    return serials


def wait_for_cert_serials_changed(secret_ocp, baseline, roles, timeout=120, sleep=5):
    """
    Poll the CNPG cert secrets until the serials for the given roles all differ
    from the baseline, proving the operator regenerated/rotated them.

    Args:
        secret_ocp (OCP): OCP handle for secrets.
        baseline (dict): Serials captured before the trigger (from
            :func:`get_cnpg_cert_serials`).
        roles (list): Which roles must change, e.g. ``["server"]`` or
            ``["server", "ca"]``.
        timeout (int): Overall timeout in seconds.
        sleep (int): Seconds between polls.

    Returns:
        dict: The current serials once all requested roles changed, or None on
            timeout.

    """
    try:
        for current in TimeoutSampler(
            timeout, sleep, get_cnpg_cert_serials, secret_ocp
        ):
            if all(current[role] and current[role] != baseline[role] for role in roles):
                logger.info(f"Cert serial(s) {roles} changed: {baseline} -> {current}")
                return current
    except TimeoutExpiredError:
        logger.error(f"Cert serial(s) {roles} did not change within {timeout}s")
        return None


def verify_leaf_signed_by_ca(secret_ocp):
    """
    Cryptographically verify that the server-leaf certificate was signed by the
    current CA's private key. Unlike an issuer/subject DN comparison (which is
    tautological when the CA subject is stable), this validates the actual
    signature against the CA public key, covering both ECDSA and RSA CA keys.

    Args:
        secret_ocp (OCP): OCP handle for secrets.

    Returns:
        bool: True if the leaf's signature verifies against the CA, else False.

    """
    server_cert = _load_cnpg_cert(
        secret_ocp, constants.NB_DB_CNPG_SERVER_SECRET, "tls.crt"
    )
    ca_cert = _load_cnpg_cert(secret_ocp, constants.NB_DB_CNPG_CA_SECRET, "ca.crt")
    if not server_cert or not ca_cert:
        return False
    ca_pubkey = ca_cert.public_key()
    try:
        if isinstance(ca_pubkey, ec.EllipticCurvePublicKey):
            ca_pubkey.verify(
                server_cert.signature,
                server_cert.tbs_certificate_bytes,
                ec.ECDSA(server_cert.signature_hash_algorithm),
            )
        else:
            ca_pubkey.verify(
                server_cert.signature,
                server_cert.tbs_certificate_bytes,
                padding.PKCS1v15(),
                server_cert.signature_hash_algorithm,
            )
    except InvalidSignature:
        logger.error("Server leaf signature does not verify against the CA public key")
        return False
    logger.info("Server leaf signature verifies against the current CA public key")
    return True


@mcg
@red_squad
@runs_on_provider
class TestNoobaaDbTLS:
    """
    Verify that the NooBaa DB (PostgreSQL managed by the CloudNativePG operator)
    serves connections over TLS using certificates that CNPG generates
    automatically, without any manual certificate creation.
    """

    @tier2
    @polarion_id("OCS-8364")
    def test_noobaa_db_cnpg_tls(self):
        """
        Verify NooBaa DB TLS on an already-deployed cluster:

        1. Get secrets and identify the cert secrets created by CNPG.
        2. Verify the cert secrets exist without any manual cert creation.
        3. Verify the NooBaa core pod requires PostgreSQL TLS
           (POSTGRES_SSL_REQUIRED env).
        4. Verify the connection is encrypted (pg_stat_ssl / ssl_is_used == t)
           on both the read-write and read-only DB service endpoints.
        5. Verify there is no TLS key material exposed in env vars.
        """
        assert verify_cnpg_tls_secrets(), (
            "CNPG cert secrets validation failed; see logs for the offending "
            "secret(s)"
        )
        assert (
            verify_postgres_ssl_required()
        ), "NooBaa core does not require PostgreSQL TLS (POSTGRES_SSL_REQUIRED)"
        assert verify_encrypted_db_connection(
            constants.CNPG_READ_WRITE_HOST
        ), "NooBaa DB read-write connection is not encrypted (TLS not in use)"
        assert verify_encrypted_db_connection(
            constants.CNPG_READ_ONLY_HOST
        ), "NooBaa DB read-only connection is not encrypted (TLS not in use)"
        assert (
            verify_no_tls_key_material_in_env()
        ), "TLS key/cert material is exposed in NooBaa container env vars"

    @tier2
    @polarion_id("OCS-8363")
    def test_internal_pg_tls_cannot_be_disabled_no_plaintext_fallback(self, request):
        """
        Verify the internal CNPG DB does not expose a way to weaken TLS and never
        falls back to unencrypted connections:

        1. Baseline: POSTGRES_SSL_REQUIRED and POSTGRES_SSL_UNAUTHORIZED are both
           "true" on the NooBaa core, and no TLS mode field exists in the NooBaa
           CR spec for the internal PG (the operator owns the TLS lifecycle).
        2. Attempt to force internal PG to disable TLS by patching the CNPG
           Cluster CR spec.postgresql.parameters.ssl=off, and confirm CNPG
           rejects it (``ssl`` is a fixed parameter CNPG manages itself), so the
           change never lands.
        3. Confirm TLS stays enforced with no unencrypted fallback: the TLS
           (sslmode=require) connection still works and the plaintext
           (sslmode=disable) connection is rejected by pg_hba.
        """
        # Step 1: baseline expectations.
        assert verify_both_postgres_ssl_envs_true(), (
            "POSTGRES_SSL_REQUIRED and POSTGRES_SSL_UNAUTHORIZED are not both "
            "'true' on the NooBaa core pod"
        )
        assert verify_no_tls_mode_field_in_noobaa_cr(), (
            "A TLS mode field unexpectedly exists in the NooBaa CR spec for the "
            "internal PG"
        )

        # Step 2: attempting to disable TLS via the CNPG parameters must be
        # rejected. CNPG treats ``ssl`` as a fixed parameter, so the patch is
        # refused by its validating webhook and nothing changes on the cluster.
        cnpg_ocp = OCP(
            kind=constants.CNPG_CLUSTER_KIND,
            resource_name=constants.NB_DB_CNPG_CLUSTER_NAME,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        def finalizer():
            # Defensive: on a version where ``ssl`` is NOT a fixed parameter the
            # patch below could actually land and disable TLS. If so, remove it
            # and wait for NooBaa to recover so the cluster is left usable.
            landed = (
                cnpg_ocp.get()["spec"]
                .get("postgresql", {})
                .get("parameters", {})
                .get("ssl")
            )
            if landed is not None:
                logger.warning(
                    f"ssl parameter unexpectedly present (value={landed}); "
                    "removing it to restore TLS"
                )
                cnpg_ocp.patch(
                    params='[{"op": "remove", '
                    '"path": "/spec/postgresql/parameters/ssl"}]',
                    format_type="json",
                )
                assert (
                    wait_for_noobaa_ready()
                ), "NooBaa not Ready after ssl parameter cleanup"

        request.addfinalizer(finalizer)

        logger.info(
            "Attempting (and expecting rejection of) a patch to disable internal "
            f"PG TLS: CNPG Cluster CR '{constants.NB_DB_CNPG_CLUSTER_NAME}' "
            "spec.postgresql.parameters.ssl=off"
        )
        ssl_off_rejected = False
        try:
            cnpg_ocp.patch(
                params=json.dumps(
                    {"spec": {"postgresql": {"parameters": {"ssl": "off"}}}}
                ),
                format_type="merge",
            )
        except CommandFailed as ex:
            # Only a webhook rejection specifically about the fixed ssl parameter
            # counts; any other command failure (schema/path/DNS/generic "invalid"
            # webhook errors) is indeterminate and must not be accepted as proof
            # of the protection.
            ex_msg = str(ex).lower()
            if "ssl" in ex_msg and "fixed configuration parameter" in ex_msg:
                logger.info(
                    "CNPG rejected the ssl=off patch as expected (ssl is a fixed, "
                    f"operator-managed parameter): {ex}"
                )
                ssl_off_rejected = True
            else:
                raise
        assert ssl_off_rejected, (
            "CNPG did not reject the attempt to disable TLS via "
            "spec.postgresql.parameters.ssl=off; internal PG TLS must not be "
            "disableable through the CNPG parameters"
        )
        # Guard against a silent apply: make sure the override did not land.
        current_ssl = (
            cnpg_ocp.get()["spec"]
            .get("postgresql", {})
            .get("parameters", {})
            .get("ssl")
        )
        assert current_ssl is None, (
            f"The ssl parameter unexpectedly landed on the CNPG Cluster CR "
            f"(value={current_ssl}); TLS may have been weakened"
        )

        # Step 3: TLS stays enforced, with no unencrypted fallback.
        assert verify_tls_enforced_no_plaintext_fallback(
            constants.CNPG_READ_WRITE_HOST
        ), (
            "TLS is not enforced after the rejected ssl=off attempt: either the "
            "encrypted connection failed or the DB accepted a plaintext "
            "(sslmode=disable) connection (unencrypted fallback); see logs"
        )

    @tier4a
    @polarion_id("OCS-8362")
    def test_internal_pg_tls_secret_replacement_no_downgrade(self, request):
        """
        Negative: replacing values in the CNPG-managed server TLS secret must not
        weaken TLS. Overwrite tls.crt in the server secret with bogus data and
        confirm that:

        * the secret stays owned by the CNPG Cluster (it is not a user-ownable
          object an attacker can repurpose), and
        * TLS stays enforced end-to-end with no unencrypted fallback: the running
          DB does not hot-reload the tampered cert, so encrypted connections keep
          working and plaintext connections stay refused by pg_hba.

        The original cert material is always restored on teardown.

        Note: CNPG does not actively revert a hand-edited secret's *content*
        within a short window (it regenerates certs on its own rotation schedule),
        so this test deliberately does not assert an immediate operator revert.
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        secret_ocp = OCP(
            kind=constants.SECRET,
            namespace=namespace,
            resource_name=constants.NB_DB_CNPG_SERVER_SECRET,
        )
        original_tls_crt = secret_ocp.get()["data"]["tls.crt"]
        bogus_tls_crt = base64.b64encode(b"not-a-real-certificate").decode()
        assert bogus_tls_crt != original_tls_crt, "Failed to build bogus cert data"

        def finalizer():
            # Always restore the real cert material, then wait for NooBaa to
            # report Ready so this destructive test does not poison later tests.
            # Leaving the bogus cert would break TLS on the DB's next reload, so
            # confirm the restore actually took rather than trusting the patch.
            logger.info("Restoring the original server cert material")
            secret_ocp.patch(
                params=json.dumps({"data": {"tls.crt": original_tls_crt}}),
                format_type="merge",
            )
            restored_crt = secret_ocp.get()["data"]["tls.crt"]
            assert (
                restored_crt == original_tls_crt
            ), "Failed to restore the original server cert material"
            assert wait_for_noobaa_ready(), "NooBaa not Ready after cert restore"

        request.addfinalizer(finalizer)

        logger.info(
            f"Replacing tls.crt in the CNPG server secret "
            f"'{constants.NB_DB_CNPG_SERVER_SECRET}' with bogus data [negative]"
        )
        assert secret_ocp.patch(
            params=json.dumps({"data": {"tls.crt": bogus_tls_crt}}),
            format_type="merge",
        ), "Failed to patch the CNPG server secret with bogus cert data"

        # The secret must remain owned by the CNPG Cluster CR.
        owner_kinds = [
            ref.get("kind")
            for ref in secret_ocp.get()["metadata"].get("ownerReferences", [])
        ]
        assert "Cluster" in owner_kinds, (
            f"Server TLS secret is no longer owned by a CNPG Cluster "
            f"(ownerReferences kinds: {owner_kinds})"
        )

        # No unencrypted fallback: tampering the secret must not downgrade TLS.
        # The running DB keeps its loaded cert, so encrypted connections must keep
        # working and plaintext must stay refused.
        assert verify_tls_enforced_no_plaintext_fallback(
            constants.CNPG_READ_WRITE_HOST
        ), (
            "TLS was weakened after tampering with the server secret: either the "
            "encrypted connection stopped working or the DB accepted a plaintext "
            "(sslmode=disable) connection (unencrypted fallback); see logs"
        )

    @pre_upgrade
    def test_noobaa_db_tls_fill_bucket_before_upgrade(
        self, request, mcg_obj_session, awscli_pod_session, bucket_factory_session
    ):
        """
        Pre-upgrade (ODF N-1, internal NooBaa without TLS):

        Create a bucket and upload objects; the uploaded objects act as the
        checksum baseline for post-upgrade comparison.
        """
        bucket = bucket_factory_session(1)[0]
        mcg_bucket_path = f"s3://{bucket.name}"

        # Create the origin dir under the test name on the awscli pod. Call
        # setup_pod_directories directly (not the test_directory_setup fixture)
        # so the baseline objects survive the upgrade without being cleaned up.
        origin_dir = setup_pod_directories(awscli_pod_session, ["origin"])[0]
        pre_upgrade_objects = write_random_objects_in_pod(
            awscli_pod_session, origin_dir, amount=5
        )
        assert pre_upgrade_objects, "No objects generated in the pre-upgrade phase"

        logger.info(f"Uploading pre-upgrade objects to bucket '{bucket.name}'")
        retry(CommandFailed, tries=3, delay=10)(sync_object_directory)(
            awscli_pod_session, origin_dir, mcg_bucket_path, mcg_obj_session
        )

        # Persist the pre-upgrade state for the post-upgrade test via the pytest
        # cache (values are JSON-serializable).
        request.config.cache.set(CACHE_KEY_BUCKET, bucket.name)
        request.config.cache.set(CACHE_KEY_OBJECTS, pre_upgrade_objects)
        request.config.cache.set(CACHE_KEY_ORIGIN_DIR, origin_dir)
        logger.info(
            f"Recorded {len(pre_upgrade_objects)} pre-upgrade objects in "
            f"bucket '{bucket.name}' as the checksum baseline"
        )

    @post_upgrade
    @polarion_id("OCS-8365")
    def test_noobaa_db_tls_after_upgrade(
        self, request, mcg_obj_session, awscli_pod_session
    ):
        """
        Post-upgrade (ODF 5.0, TLS auto-enabled by the operator):

        1. Verify the CNPG TLS cert secrets were created.
        2. Verify POSTGRES_SSL_REQUIRED=true and the DB connection is encrypted
           (SELECT ssl ... / ssl_is_used == t), with no TLS key material in env.
           TLS being required now, with no action taken between the two phases,
           shows the operator handled the Prefer -> Require transition itself.
        3. Re-download all pre-upgrade objects and compare checksums against the
           pre-upgrade baseline.
        """
        pre_upgrade_bucket = request.config.cache.get(CACHE_KEY_BUCKET, None)
        pre_upgrade_objects = request.config.cache.get(CACHE_KEY_OBJECTS, None)
        origin_dir = request.config.cache.get(CACHE_KEY_ORIGIN_DIR, None)
        assert (
            pre_upgrade_bucket and pre_upgrade_objects and origin_dir
        ), "No pre-upgrade state recorded; the pre-upgrade phase did not run"

        # Steps 1-2: full TLS validation now that the cluster is upgraded. The
        # operator enabled Require mode on its own (no manual action between the
        # two phases), which verify_postgres_ssl_required confirms.
        assert verify_cnpg_tls_secrets(), (
            "CNPG cert secrets validation failed after upgrade; see logs for the "
            "offending secret(s)"
        )
        assert verify_postgres_ssl_required(), (
            "NooBaa core does not require PostgreSQL TLS after upgrade "
            "(operator did not enable Require mode)"
        )
        assert verify_encrypted_db_connection(constants.CNPG_READ_WRITE_HOST), (
            "NooBaa DB read-write connection is not encrypted after upgrade "
            "(TLS not in use)"
        )
        assert verify_encrypted_db_connection(constants.CNPG_READ_ONLY_HOST), (
            "NooBaa DB read-only connection is not encrypted after upgrade "
            "(TLS not in use)"
        )
        assert (
            verify_no_tls_key_material_in_env()
        ), "TLS key/cert material is exposed in NooBaa container env vars after upgrade"

        # Step 3: re-download the pre-upgrade objects and compare checksums
        mcg_bucket_path = f"s3://{pre_upgrade_bucket}"
        result_dir = setup_pod_directories(awscli_pod_session, ["result"])[0]
        logger.info(
            f"Re-downloading pre-upgrade objects from bucket '{pre_upgrade_bucket}'"
        )
        retry(CommandFailed, tries=3, delay=10)(sync_object_directory)(
            awscli_pod_session, mcg_bucket_path, result_dir, mcg_obj_session
        )
        for obj in pre_upgrade_objects:
            assert verify_s3_object_integrity(
                original_object_path=f"{origin_dir}/{obj}",
                result_object_path=f"{result_dir}/{obj}",
                awscli_pod=awscli_pod_session,
            ), (
                f"Checksum mismatch for object '{obj}' between the pre-upgrade "
                "baseline and the post-upgrade download"
            )
        logger.info(
            f"All {len(pre_upgrade_objects)} pre-upgrade objects match their "
            "checksum baseline after the upgrade"
        )

    @tier2
    @polarion_id("OCS-8397")
    def test_noobaa_core_pod_restart_preserves_postgres_ssl_config(self):
        """
        Verify that POSTGRES_SSL_REQUIRED env var is preserved across NooBaa core
        pod restarts and operator pod restart:

        1. Confirm POSTGRES_SSL_REQUIRED=true and encrypted connection (TLS in use).
        2. Delete both NooBaa core pods sequentially, verify each restarts
           automatically, and re-check POSTGRES_SSL_REQUIRED and encrypted
           connection after each restart.
        3. Delete the NooBaa operator pod; verify it restarts automatically.
        4. After operator restart, confirm POSTGRES_SSL_REQUIRED is
           re-applied and connection remains encrypted.
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # Step 1: baseline verification
        logger.info("Step 1: Verifying baseline TLS (SSL required + encrypted)")
        assert verify_tls_in_use(), "Baseline check failed: TLS not in use"

        # Identify the active (leader) core pod and its passive peer. Deleting
        # the active pod first forces a leader failover onto the passive peer.
        core_pods = get_noobaa_core_pods(namespace=namespace)
        logger.info(f"Found core pod(s): {[p.name for p in core_pods]}")
        active_core_pod = get_active_core_pod(core_pods=core_pods, namespace=namespace)
        passive_core_pod = get_standby_core_pod(
            core_pods=core_pods, active_pod=active_core_pod
        )
        logger.info(
            f"Active (leader): {active_core_pod.name}, "
            f"passive: {passive_core_pod.name}"
        )

        # Step 2: delete each leader in turn; wait for the standby peer to be
        # promoted to 2/2, then re-check TLS (the deleted pod returns as 1/2).
        deletion_order = [
            (active_core_pod, passive_core_pod),
            (passive_core_pod, active_core_pod),
        ]
        for idx, (pod_to_delete, peer_to_wait) in enumerate(deletion_order, start=1):
            logger.info(f"Step 2.{idx}: Deleting core pod {pod_to_delete.name}")
            pod_to_delete.delete()
            assert peer_to_wait.ocp.wait(
                resource_name=peer_to_wait.name, condition=constants.STATUS_READY
            ), f"Core pod {peer_to_wait.name} was not promoted to fully ready (2/2)"
            assert (
                verify_tls_in_use()
            ), f"After deleting core pod {pod_to_delete.name}: TLS not in use"

        # Step 3: delete the NooBaa operator pod and wait for it to recover.
        logger.info("Step 3: Deleting NooBaa operator pod")
        delete_pods([get_noobaa_operator_pod()])
        assert wait_for_pods_to_be_running(
            namespace=namespace, timeout=300
        ), "Operator pod did not restart within timeout"
        assert wait_for_noobaa_ready(), "NooBaa not Ready after operator restart"

        # Step 4: the operator must re-apply the TLS config on its own.
        logger.info("Step 4: Verifying TLS is re-applied after operator restart")
        assert verify_tls_in_use(), "After operator restart: TLS not in use"

    @tier4a
    @polarion_id("OCS-8394")
    def test_noobaa_tls_with_s3_workload_and_pod_disruptions(
        self, mcg_obj_session, awscli_pod_session, bucket_factory_session
    ):
        """
        Comprehensive test: Verify TLS enforcement under operator and core pod
        disruptions with simultaneous S3 write workload:

        1. Create S3 bucket and start background write workload.
        2. Verify baseline: POSTGRES_SSL_REQUIRED=true, encrypted connection works.
        3. Delete NooBaa operator pod to trigger re-reconciliation.
        4. Monitor for S3 operation errors during operator restart.
        5. After operator restart, verify SSL config is re-applied and connection works.
        6. Compare checksums of objects written before and during reconciliation.
        7. Delete NooBaa core pod while S3 I/O is ongoing.
        8. Verify S3 operations resume after pod recovery.
        9. Final verification: TLS still enforced (ssl_is_used == t).
        """
        namespace = config.ENV_DATA["cluster_namespace"]

        # Distinct object-name patterns per batch so the three batches do not
        # overwrite each other in the shared origin dir / bucket.
        baseline_pattern = "baseline-obj-"
        reconcile_pattern = "reconcile-obj-"
        post_switch_pattern = "post-switch-obj-"

        # Create both pod dirs up front so the finally block can always clean up.
        origin_dir, result_dir = setup_pod_directories(
            awscli_pod_session, ["origin", "result"]
        )
        try:
            # Step 1: Create bucket and upload the baseline objects over S3.
            logger.info("Step 1: Creating S3 bucket and uploading baseline objects")
            bucket = bucket_factory_session(1)[0]
            mcg_bucket_path = f"s3://{bucket.name}"
            baseline_objects = write_random_objects_in_pod(
                awscli_pod_session, origin_dir, amount=10, pattern=baseline_pattern
            )
            assert baseline_objects, "Failed to create baseline objects"
            sync_with_retry(
                awscli_pod_session, origin_dir, mcg_bucket_path, mcg_obj_session
            )
            logger.info(f"Uploaded {len(baseline_objects)} baseline objects")

            # Step 2: baseline TLS verification.
            logger.info("Step 2: Verifying baseline TLS (SSL required + encrypted)")
            assert verify_tls_in_use(), "Baseline: TLS not in use"

            # Step 3: delete the operator pod to trigger re-reconciliation.
            logger.info("Step 3: Deleting NooBaa operator pod")
            delete_pods([get_noobaa_operator_pod()])

            # Step 4: write more objects while the operator is restarting; S3 may
            # briefly error during reconciliation, which sync_with_retry absorbs.
            logger.info("Step 4: Writing objects during operator reconciliation")
            reconciliation_objects = write_random_objects_in_pod(
                awscli_pod_session, origin_dir, amount=10, pattern=reconcile_pattern
            )
            sync_with_retry(
                awscli_pod_session, origin_dir, mcg_bucket_path, mcg_obj_session
            )

            # Step 5: operator must recover and re-apply the TLS config on its own.
            logger.info("Step 5: Verifying TLS config after operator restart")
            assert wait_for_pods_to_be_running(
                namespace=namespace, timeout=300
            ), "Operator pod did not restart within timeout"
            assert wait_for_noobaa_ready(), "NooBaa not Ready after operator restart"
            assert verify_tls_in_use(), "After operator restart: TLS not in use"

            # Step 6: re-download everything and compare checksums against source.
            logger.info(
                "Step 6: Verifying checksums (baseline + reconciliation objects)"
            )
            sync_with_retry(
                awscli_pod_session, mcg_bucket_path, result_dir, mcg_obj_session
            )
            assert compare_directory(
                awscli_pod_session,
                origin_dir,
                result_dir,
                amount=len(baseline_objects),
                pattern=baseline_pattern,
            ), "Checksum mismatch for baseline objects"
            assert compare_directory(
                awscli_pod_session,
                origin_dir,
                result_dir,
                amount=len(reconciliation_objects),
                pattern=reconcile_pattern,
            ), "Checksum mismatch for reconciliation objects"

            # Step 7: delete the active core pod while I/O is ongoing; the passive
            # peer must take over and become fully ready (promoted to 2/2).
            logger.info("Step 7: Deleting active core pod during S3 I/O")
            core_pods = get_noobaa_core_pods(namespace=namespace)
            active_core_pod = get_active_core_pod(
                core_pods=core_pods, namespace=namespace
            )
            passive_core_pod = get_standby_core_pod(
                core_pods=core_pods, active_pod=active_core_pod
            )
            active_core_pod.delete()
            assert passive_core_pod.ocp.wait(
                resource_name=passive_core_pod.name, condition=constants.STATUS_READY
            ), f"Core pod {passive_core_pod.name} was not promoted to fully ready (2/2)"

            # Step 8: S3 operations must resume after the pod switch.
            logger.info("Step 8: Verifying S3 operations resume after core pod switch")
            post_disruption_objects = write_random_objects_in_pod(
                awscli_pod_session, origin_dir, amount=5, pattern=post_switch_pattern
            )
            sync_with_retry(
                awscli_pod_session, origin_dir, mcg_bucket_path, mcg_obj_session
            )

            # Step 9: TLS must still be enforced end to end.
            logger.info("Step 9: Final verification - TLS still enforced")
            assert verify_tls_in_use(), "After core pod switch: TLS not in use"

            logger.info(
                f"Test completed. baseline={len(baseline_objects)}, "
                f"reconciliation={len(reconciliation_objects)}, "
                f"post_disruption={len(post_disruption_objects)}"
            )
        finally:
            awscli_pod_session.exec_cmd(f"rm -rf {origin_dir} {result_dir}")

    @tier4a
    @polarion_id("OCS-8395")
    def test_cnpg_tls_cert_rotation_automatic_regeneration(self, request):
        """
        Verify automatic TLS certificate regeneration and CA rotation by CNPG
        without manual cert creation:

        1. Record baseline serials and verify the leaf is genuinely signed by
           the CA (real signature check, not a DN comparison).
        2. Delete the server TLS secret to trigger CNPG regeneration.
        3. Verify automatic regeneration (new serial, CNPG-owned) and that the
           regenerated leaf is still signed by the CA.
        4. Delete the CA secret and verify the CA is rotated to a new serial.
           CNPG does not reissue the already-running leaf in place, so assert
           the leaf serial is unchanged (documents the actual guarantee).
        5. Verify the DB still enforces TLS afterwards (no downtime).
        """
        namespace = config.ENV_DATA["cluster_namespace"]
        secret_ocp = OCP(kind=constants.SECRET, namespace=namespace)

        def finalizer():
            # This test deletes both CNPG-managed secrets. Wait for CNPG to
            # regenerate them and for NooBaa to report Ready so a mid-test
            # failure does not leave a half-rotated CA for later tier4a tests.
            for secret_name in (
                constants.NB_DB_CNPG_SERVER_SECRET,
                constants.NB_DB_CNPG_CA_SECRET,
            ):
                assert secret_ocp.get(
                    resource_name=secret_name, dont_raise=True
                ), f"CNPG did not regenerate secret '{secret_name}'"
            assert wait_for_noobaa_ready(), "NooBaa not Ready after cert rotation test"

        request.addfinalizer(finalizer)

        # Step 1: baseline serials + real signature check (leaf signed by CA).
        logger.info("Step 1: Recording baseline serials and verifying leaf signature")
        baseline = get_cnpg_cert_serials(secret_ocp)
        assert (
            baseline["server"] is not None and baseline["ca"] is not None
        ), "Failed to read baseline server/CA cert serials"
        assert verify_leaf_signed_by_ca(
            secret_ocp
        ), "Baseline: server leaf is not signed by the CA"

        # Step 2: delete the server secret to trigger regeneration.
        logger.info(
            f"Step 2: Deleting server TLS secret '{constants.NB_DB_CNPG_SERVER_SECRET}'"
        )
        secret_ocp.delete(resource_name=constants.NB_DB_CNPG_SERVER_SECRET)

        # Step 3: CNPG must regenerate the server cert (new serial), keep it
        # CNPG-owned, and the new leaf must still verify against the CA.
        logger.info("Step 3: Verifying automatic server cert regeneration")
        regenerated = wait_for_cert_serials_changed(
            secret_ocp, baseline, roles=["server"]
        )
        assert regenerated, "Server TLS secret was not regenerated automatically"
        owner_kinds = [
            ref.get("kind")
            for ref in secret_ocp.get(resource_name=constants.NB_DB_CNPG_SERVER_SECRET)[
                "metadata"
            ].get("ownerReferences", [])
        ]
        assert "Cluster" in owner_kinds, (
            "Regenerated server secret is not owned by CNPG Cluster "
            "(manual cert creation detected)"
        )
        assert verify_leaf_signed_by_ca(
            secret_ocp
        ), "Regenerated server leaf is not signed by the CA"

        # Step 4: trigger a real CA rotation. CNPG regenerates the CA with a new
        # serial but does not reissue the already-running leaf in place, so the
        # leaf serial must stay unchanged (the actual CNPG guarantee).
        logger.info(f"Step 4: Deleting CA secret '{constants.NB_DB_CNPG_CA_SECRET}'")
        secret_ocp.delete(resource_name=constants.NB_DB_CNPG_CA_SECRET)

        rotated = wait_for_cert_serials_changed(secret_ocp, regenerated, roles=["ca"])
        assert rotated, "CA cert was not rotated (new serial) within timeout"
        assert rotated["server"] == regenerated["server"], (
            "CNPG unexpectedly reissued the running server leaf during CA "
            "rotation (leaf serial changed)"
        )

        # Step 5: TLS must still be enforced with the rotated CA (no downtime).
        logger.info("Step 5: Verifying TLS still enforced after rotation")
        assert verify_tls_in_use(), "After cert rotation: TLS not in use"
        logger.info(
            f"CA rotation verified: CA {regenerated['ca']} -> {rotated['ca']}, "
            f"server leaf unchanged at {rotated['server']}"
        )

    @pre_upgrade
    def test_noobaa_backup_before_tls_upgrade(
        self, request, mcg_obj_session, awscli_pod_session, bucket_factory_session
    ):
        """
        Pre-upgrade (ODF N-1, internal NooBaa without TLS):

        1. Create a bucket and upload objects (the backup payload).
        2. Persist the bucket/objects/source dir so the post-upgrade test can
           restore and compare checksums across the TLS activation.
        """
        logger.info("Creating bucket and uploading backup objects (pre-upgrade)")
        bucket = bucket_factory_session(1)[0]
        origin_dir = setup_pod_directories(awscli_pod_session, ["backup_origin"])[0]
        backup_objects = write_random_objects_in_pod(
            awscli_pod_session, origin_dir, amount=10
        )
        assert backup_objects, "Failed to create backup baseline objects"
        sync_with_retry(
            awscli_pod_session, origin_dir, f"s3://{bucket.name}", mcg_obj_session
        )

        # Persist the baseline for the post-upgrade test via the pytest cache.
        request.config.cache.set("noobaa_db_tls/backup_bucket", bucket.name)
        request.config.cache.set("noobaa_db_tls/backup_objects", backup_objects)
        request.config.cache.set("noobaa_db_tls/backup_origin_dir", origin_dir)
        logger.info(
            f"Recorded backup baseline: bucket={bucket.name}, "
            f"objects={len(backup_objects)}"
        )

    @post_upgrade
    @polarion_id("OCS-8396")
    def test_noobaa_backup_restore_after_tls_upgrade(
        self, request, mcg_obj_session, awscli_pod_session
    ):
        """
        Post-upgrade (ODF 5.0, TLS auto-enabled by operator):

        1. Verify TLS is enabled post-upgrade.
        2. Restore (re-download) the objects backed up pre-upgrade over TLS.
        3. Compare checksums against the pre-upgrade baseline.
        """
        backup_bucket = request.config.cache.get("noobaa_db_tls/backup_bucket", None)
        backup_objects = request.config.cache.get("noobaa_db_tls/backup_objects", None)
        backup_origin_dir = request.config.cache.get(
            "noobaa_db_tls/backup_origin_dir", None
        )
        assert (
            backup_bucket and backup_objects and backup_origin_dir
        ), "No backup baseline recorded; the pre-upgrade phase did not run"

        # Step 1: TLS must be active after the upgrade.
        logger.info("Step 1: Verifying TLS is enabled post-upgrade")
        assert verify_cnpg_tls_secrets(), "CNPG TLS secrets not found after upgrade"
        assert verify_tls_in_use(), "TLS not in use after upgrade"

        # Steps 2-3: restore the backed-up objects over TLS and compare checksums.
        logger.info(
            f"Steps 2-3: Restoring {len(backup_objects)} objects from "
            f"'{backup_bucket}' over TLS and verifying checksums"
        )
        result_dir = setup_pod_directories(awscli_pod_session, ["backup_result"])[0]
        sync_with_retry(
            awscli_pod_session, f"s3://{backup_bucket}", result_dir, mcg_obj_session
        )
        assert compare_directory(
            awscli_pod_session,
            backup_origin_dir,
            result_dir,
            amount=len(backup_objects),
        ), "Checksum mismatch between restored objects and the pre-upgrade baseline"
