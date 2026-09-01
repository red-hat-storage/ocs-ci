"""
OpenShift Lightspeed (OLS) operator deployment on the ACM hub cluster.

Handles the full lifecycle in an idempotent way — every step checks whether
the resource already exists before creating it, so the deployer is safe to
re-run against a partially- or fully-installed environment.

Sequence
--------
1. Create the ``openshift-lightspeed`` namespace (skip if present)
2. Create the OperatorGroup (skip if present)
3. Create the Subscription (skip if present)
4. Wait for the CSV to reach ``Succeeded``
5. Create the LLM credentials Secret (skip if present)
6. Apply the OLSConfig CR (create or patch-in-place)
7. Wait for the ``lightspeed-app-server`` pods to be running
8. Grant the ``lightspeed-operator-query-access`` ClusterRole (the OLS query
   access role; older installs may have it as ``ols-user``) to the requested
   principals (skip if binding already exists)
"""

import logging
import tempfile
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.csv import CSV, get_csvs_start_with_prefix
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility import templating
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler
from ocs_ci.utility.version import get_semantic_ocs_version_from_config

logger = logging.getLogger(__name__)


def get_ols_rag_images(odf_version=None):
    """
    Build the RAG image list for OLSConfig dynamically from the ODF version.

    The ODF Lightspeed RAG content image tag and its ``indexID`` are derived
    from the running ODF major.minor version so that a re-run on a different
    cluster or after an upgrade always uses the correct image.

    Args:
        odf_version (str): Optional explicit version string in ``"X.Y"``
            format (e.g. ``"4.22"``).  When ``None`` (default) the version is
            read from ``config.ENV_DATA["ocs_version"]`` via
            :func:`~ocs_ci.utility.version.get_semantic_ocs_version_from_config`.

    Returns:
        list[dict]: Two-element list ready to assign to
            ``OLSConfig.spec.ols.rag``:

            * ODF content RAG image (version-tagged)
            * DR recipe BYOK RAG image (version-independent)

    Example::

        # On a 4.22 cluster:
        get_ols_rag_images()
        # [
        #   {"image": "quay.io/rhceph-dev/...rhel9:v4.22",
        #    "indexID": "odf-4.22", "indexPath": "/rag/vector_db"},
        #   {"image": "quay.io/anarasag/recipe-byok-image:latest",
        #    "indexID": "vector_db_index", "indexPath": "/rag/vector_db"},
        # ]
    """
    if odf_version is None:
        ver = get_semantic_ocs_version_from_config()
        odf_version = f"{ver.major}.{ver.minor}"

    image_tag = f"v{odf_version}"
    index_id = f"odf-{odf_version}"
    logger.info(f"Building OLS RAG image list for ODF version {odf_version}")

    return [
        {
            "image": f"{constants.OLS_RAG_CONTENT_IMAGE_BASE}:{image_tag}",
            "indexID": index_id,
            "indexPath": "/rag/vector_db",
        },
        {
            "image": constants.OLS_RAG_RECIPE_BYOK_IMAGE,
            "indexID": "vector_db_index",
            "indexPath": "/rag/vector_db",
        },
    ]


class OLSInstaller:
    """
    Idempotent installer for the OpenShift Lightspeed operator and service.

    All public ``deploy_*`` / ``configure_*`` methods are safe to re-run.
    Each step inspects existing cluster state before applying changes, so
    repeated runs converge without errors.

    Args:
        channel (str): OLM channel for the Subscription.
            Defaults to ``constants.OLS_OPERATOR_CHANNEL``.
        switch_ctx (int): Cluster index to switch to before running any
            ``oc`` commands.  Pass ``None`` (default) to stay on the current
            active context; pass the ACM index to target the hub cluster.
    """

    def __init__(self, channel=None, switch_ctx=None):
        self.namespace = constants.OLS_NAMESPACE
        self.channel = channel or constants.OLS_OPERATOR_CHANNEL
        self.switch_ctx = switch_ctx

    # ------------------------------------------------------------------
    # Context helper
    # ------------------------------------------------------------------

    def _switch(self):
        """Switch cluster context when an index was provided."""
        if self.switch_ctx is not None:
            config.switch_ctx(self.switch_ctx)
        else:
            config.switch_acm_ctx()

    # ------------------------------------------------------------------
    # Existence helpers
    # ------------------------------------------------------------------

    def _namespace_exists(self):
        return OCP(kind=constants.NAMESPACE).is_exist(resource_name=self.namespace)

    def _operatorgroup_exists(self):
        return OCP(
            kind=constants.OPERATOR_GROUP,
            namespace=self.namespace,
            resource_name=constants.OLS_OPERATORGROUP_NAME,
        ).is_exist(resource_name=constants.OLS_OPERATORGROUP_NAME)

    def _subscription_exists(self):
        return OCP(
            kind=constants.SUBSCRIPTION_COREOS,
            namespace=self.namespace,
            resource_name=constants.OLS_SUBSCRIPTION_NAME,
        ).is_exist(resource_name=constants.OLS_SUBSCRIPTION_NAME)

    def _olsconfig_exists(self):
        return OCP(
            kind=constants.OLS_CONFIG_KIND,
            namespace=self.namespace,
            resource_name=constants.OLS_CONFIG_NAME,
        ).is_exist(resource_name=constants.OLS_CONFIG_NAME)

    def _secret_exists(self, secret_name):
        return OCP(
            kind=constants.SECRET,
            namespace=self.namespace,
            resource_name=secret_name,
        ).is_exist(resource_name=secret_name)

    def _clusterrolebinding_exists(self, crb_name):
        return OCP(
            kind=constants.CLUSTERROLEBINDING,
            resource_name=crb_name,
        ).is_exist(resource_name=crb_name)

    def _ols_csv_succeeded(self):
        """Return True only when a matching OLS CSV has phase ``Succeeded``."""
        csvs = get_csvs_start_with_prefix(
            csv_prefix=constants.OLS_CSV_PREFIX,
            namespace=self.namespace,
        )
        return any(csv.get("status", {}).get("phase") == "Succeeded" for csv in csvs)

    # ------------------------------------------------------------------
    # Step 1 — Namespace
    # ------------------------------------------------------------------

    def create_namespace(self):
        """
        Create the ``openshift-lightspeed`` namespace.

        Skips silently if the namespace already exists.
        """
        self._switch()
        if self._namespace_exists():
            logger.info(f"Namespace '{self.namespace}' already exists — skipping")
            return
        logger.info(f"Creating namespace '{self.namespace}'")
        ns_data = templating.load_yaml(constants.OLS_NAMESPACE_YAML)
        ns_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ols_namespace_", delete=False
        )
        templating.dump_data_to_temp_yaml(ns_data, ns_manifest.name)
        exec_cmd(f"oc apply -f {ns_manifest.name}")

    # ------------------------------------------------------------------
    # Step 2 — OperatorGroup
    # ------------------------------------------------------------------

    def create_operatorgroup(self):
        """
        Create the OLS OperatorGroup.

        Skips silently if it already exists.
        """
        self._switch()
        if self._operatorgroup_exists():
            logger.info(
                f"OperatorGroup '{constants.OLS_OPERATORGROUP_NAME}' already exists — skipping"
            )
            return
        logger.info("Creating OperatorGroup for OLS")
        exec_cmd(f"oc apply -f {constants.OLS_OPERATORGROUP_YAML} -n {self.namespace}")

    # ------------------------------------------------------------------
    # Step 3 — Subscription
    # ------------------------------------------------------------------

    def create_subscription(self):
        """
        Create the OLS OLM Subscription.

        Skips silently if it already exists.  Sets the channel from
        ``self.channel``.
        """
        self._switch()
        if self._subscription_exists():
            logger.info(
                f"Subscription '{constants.OLS_SUBSCRIPTION_NAME}' already exists — skipping"
            )
            return
        logger.info(f"Creating OLS subscription on channel '{self.channel}'")
        sub_data = templating.load_yaml(constants.OLS_SUBSCRIPTION_YAML)
        sub_data["spec"]["channel"] = self.channel
        sub_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ols_subscription_", delete=False
        )
        templating.dump_data_to_temp_yaml(sub_data, sub_manifest.name)
        exec_cmd(f"oc apply -f {sub_manifest.name}")

    # ------------------------------------------------------------------
    # Step 4 — Wait for CSV
    # ------------------------------------------------------------------

    def wait_for_csv(self, timeout=600):
        """
        Wait until the OLS CSV reaches the ``Succeeded`` phase.

        Args:
            timeout (int): Seconds to wait before raising an assertion error.
        """
        self._switch()
        if self._ols_csv_succeeded():
            logger.info("OLS CSV already in Succeeded phase — skipping wait")
            return

        logger.info("Waiting for OLS CSV to reach Succeeded phase")
        # Give OLM a moment to create the CSV after subscription
        time.sleep(30)
        for csvs in TimeoutSampler(
            timeout=timeout,
            sleep=15,
            func=get_csvs_start_with_prefix,
            csv_prefix=constants.OLS_CSV_PREFIX,
            namespace=self.namespace,
        ):
            if csvs:
                csv_name = csvs[0]["metadata"]["name"]
                logger.info(f"Found OLS CSV: {csv_name}")
                csv_obj = CSV(resource_name=csv_name, namespace=self.namespace)
                csv_obj.wait_for_phase("Succeeded", timeout=timeout)
                logger.info("OLS CSV reached Succeeded phase")
                return

    # ------------------------------------------------------------------
    # Step 5 — LLM credentials Secret
    # ------------------------------------------------------------------

    def create_llm_secret(self, secret_name, api_token=None):
        """
        Create the LLM provider credentials Secret.

        Skips silently if the Secret already exists so that a re-run does not
        need the token at all — the existing secret is left untouched.

        Args:
            secret_name (str): Name for the Secret (referenced in OLSConfig).
            api_token (str): API token / key for the LLM provider.  Required
                only when the Secret does not already exist on the cluster.
                Pass ``None`` (default) when the Secret is pre-created.

        Raises:
            ValueError: If the Secret does not exist and ``api_token`` is
                ``None``.
        """
        self._switch()
        if self._secret_exists(secret_name):
            logger.info(
                f"LLM credentials Secret '{secret_name}' already exists — skipping"
            )
            return
        if not api_token:
            raise ValueError(
                f"LLM credentials Secret '{secret_name}' does not exist and "
                f"no api_token was supplied. Either pre-create the Secret or "
                f"provide AUTH.ols.api_token in your conf file."
            )
        logger.info(f"Creating LLM credentials Secret '{secret_name}'")
        exec_cmd(
            f"oc create secret generic {secret_name}"
            f" --from-literal=apitoken={api_token}"
            f" -n {self.namespace}",
            secrets=[api_token],
        )

    # ------------------------------------------------------------------
    # Step 6 — OLSConfig CR
    # ------------------------------------------------------------------

    def create_or_update_olsconfig(
        self,
        provider_name,
        provider_type,
        provider_url,
        model_name,
        secret_name,
        rag_images=None,
    ):
        """
        Apply the OLSConfig CR.

        If the CR already exists it is patched in-place (``oc apply``), so
        re-running with new RAG images or model settings is safe.

        Args:
            provider_name (str): Name of the LLM provider (e.g. ``rhoai-qwen``).
            provider_type (str): OLS provider type (e.g. ``rhoai_vllm``,
                ``openai``, ``watsonx``).
            provider_url (str): Base URL of the LLM inference endpoint.
            model_name (str): Model identifier.
            secret_name (str): Name of the Secret holding the LLM API token.
            rag_images (list[dict]): Optional list of RAG image entries.  Each
                dict must have ``image``, ``indexID``, and ``indexPath`` keys.
                When ``None`` (default), :func:`get_ols_rag_images` is called
                to build the list from the current ODF version automatically.

        Example ``rag_images`` entry::

            {
                "image": "quay.io/rhceph-dev/odf4-odf-lightspeed-rag-content-rhel9:v4.22",
                "indexID": "odf-4.22",
                "indexPath": "/rag/vector_db",
            }
        """
        self._switch()
        rag_images = rag_images if rag_images is not None else get_ols_rag_images()

        logger.info(
            f"Applying OLSConfig (provider={provider_name}, model={model_name})"
        )
        olsconfig_data = templating.load_yaml(constants.OLS_CONFIG_YAML)

        # LLM provider
        provider_entry = olsconfig_data["spec"]["llm"]["providers"][0]
        provider_entry["credentialsSecretRef"]["name"] = secret_name
        provider_entry["models"][0]["name"] = model_name
        provider_entry["name"] = provider_name
        provider_entry["type"] = provider_type
        provider_entry["url"] = provider_url

        # OLS defaults
        olsconfig_data["spec"]["ols"]["defaultModel"] = model_name
        olsconfig_data["spec"]["ols"]["defaultProvider"] = provider_name

        # RAG images
        olsconfig_data["spec"]["ols"]["rag"] = rag_images

        olsconfig_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="ols_olsconfig_", delete=False
        )
        templating.dump_data_to_temp_yaml(olsconfig_data, olsconfig_manifest.name)
        # `oc apply` is inherently idempotent — creates or updates in-place
        exec_cmd(f"oc apply -f {olsconfig_manifest.name}")

        if self._olsconfig_exists():
            logger.info("OLSConfig applied successfully")

    # ------------------------------------------------------------------
    # Step 7 — Wait for pods
    # ------------------------------------------------------------------

    def wait_for_ols_pods(self, timeout=300):
        """
        Wait until the ``lightspeed-app-server`` pods are running.

        Args:
            timeout (int): Seconds to wait.

        Raises:
            TimeoutExpiredError: When pods do not reach Running within
                ``timeout`` seconds.
        """
        from ocs_ci.ocs.resources.pod import get_all_pods

        self._switch()
        logger.info("Waiting for OLS pods to be running")
        # Resolve the current pod names for lightspeed-app-server so we can
        # pass them explicitly — wait_for_pods_to_be_running does not accept
        # a prefix filter, only a pod_names list.
        pods = get_all_pods(
            namespace=self.namespace,
            selector=["lightspeed-app-server"],
            selector_label="app.kubernetes.io/name",
        )
        pod_names = [p.name for p in pods] if pods else None
        result = wait_for_pods_to_be_running(
            namespace=self.namespace,
            pod_names=pod_names,
            timeout=timeout,
        )
        if not result:
            raise TimeoutExpiredError(
                f"OLS pods in namespace '{self.namespace}' did not reach "
                f"Running state within {timeout} seconds."
            )
        logger.info("OLS pods are running")

    # ------------------------------------------------------------------
    # Step 7b — Expose Route
    # ------------------------------------------------------------------

    def expose_route(self):
        """
        Expose the ``lightspeed-app-server`` service as a passthrough Route.

        Skips silently if the Route already exists.
        The Route is required for external API access — without it the
        :class:`~ocs_ci.ocs.openshift_lightspeed.OpenShiftLightspeed` client
        cannot resolve the base URL.
        """
        self._switch()
        route_ocp = OCP(
            kind=constants.ROUTE,
            namespace=self.namespace,
            resource_name=constants.OLS_ROUTE_NAME,
        )
        if route_ocp.check_resource_existence(
            timeout=6,
            should_exist=True,
            resource_name=constants.OLS_ROUTE_NAME,
        ):
            logger.info(f"Route '{constants.OLS_ROUTE_NAME}' already exists — skipping")
            return
        logger.info(f"Exposing Route '{constants.OLS_ROUTE_NAME}'")
        exec_cmd(
            f"oc create route passthrough {constants.OLS_ROUTE_NAME}"
            f" --service={constants.OLS_ROUTE_NAME}"
            f" --port=8443"
            f" -n {self.namespace}"
        )

    # ------------------------------------------------------------------
    # Step 8 — RBAC
    # ------------------------------------------------------------------

    def _resolve_ols_query_role(self):
        """
        Return the name of the OLS query-access ClusterRole present on the
        cluster.

        OLS 1.1+ uses ``lightspeed-operator-query-access``; older installs may
        still have ``ols-user``.  Returns ``None`` when neither exists.

        Returns:
            str | None: ClusterRole name, or ``None`` if not found.
        """
        for role in (constants.OLS_QUERY_ACCESS_ROLE, "ols-user"):
            if OCP(
                kind=constants.CLUSTER_ROLE,
                resource_name=role,
            ).is_exist(resource_name=role):
                return role
        return None

    def grant_ols_user_role(
        self, username=None, service_account=None, sa_namespace=None
    ):
        """
        Grant the OLS query-access ClusterRole to a user or service account.

        The role name is resolved at runtime — ``lightspeed-operator-query-access``
        for OLS 1.1+ or ``ols-user`` for older installs.

        Handles all cases safely:

        * Skips if the ClusterRoleBinding already exists (idempotent re-run).
        * Skips if neither OLS ClusterRole exists on the cluster (e.g. operator
          not yet installed).

        Either ``username`` **or** both ``service_account`` + ``sa_namespace``
        must be provided.

        Args:
            username (str): OpenShift user to grant the role to.
            service_account (str): Service account name.
            sa_namespace (str): Namespace of the service account.
        """
        self._switch()
        if username:
            subject = username
            crb_name = f"ols-user-{username}"
        elif service_account and sa_namespace:
            subject = f"system:serviceaccount:{sa_namespace}:{service_account}"
            crb_name = f"ols-user-sa-{sa_namespace}-{service_account}"
        else:
            raise ValueError(
                "Provide either 'username' or both 'service_account' and 'sa_namespace'"
            )

        if self._clusterrolebinding_exists(crb_name):
            logger.info(f"ClusterRoleBinding '{crb_name}' already exists — skipping")
            return

        role_name = self._resolve_ols_query_role()
        if not role_name:
            logger.warning(
                "No OLS query-access ClusterRole found on the cluster — "
                "skipping RBAC grant. The operator may not have reconciled yet."
            )
            return

        logger.info(f"Granting ClusterRole '{role_name}' to '{subject}'")
        exec_cmd(f"oc adm policy add-cluster-role-to-user {role_name} {subject}")

    # ------------------------------------------------------------------
    # High-level entry point
    # ------------------------------------------------------------------

    def deploy(
        self,
        provider_name,
        provider_type,
        provider_url,
        model_name,
        secret_name,
        api_token=None,
        rag_images=None,
        username=None,
        service_account=None,
        sa_namespace=None,
        csv_timeout=600,
        pods_timeout=300,
    ):
        """
        Full idempotent deployment of OLS on the hub cluster.

        Steps performed (each skipped automatically if already done):

        1. Create namespace
        2. Create OperatorGroup
        3. Create Subscription
        4. Wait for CSV ``Succeeded``
        5. Create LLM credentials Secret
        6. Apply OLSConfig CR
        7. Wait for pods running
        8. Grant ``ols-user`` RBAC (when ``username`` or ``service_account``
           is provided)

        Args:
            provider_name (str): LLM provider name.
            provider_type (str): OLS provider type string.
            provider_url (str): LLM inference endpoint URL.
            model_name (str): Model identifier.
            secret_name (str): Name for the credentials Secret.
            api_token (str): API token value for the credentials Secret.
            rag_images (list[dict]): RAG content images.  When ``None``
                (default) :func:`get_ols_rag_images` builds them dynamically
                from the current ODF version.
            username (str): User to grant ``ols-user`` role to.
            service_account (str): SA to grant ``ols-user`` role to.
            sa_namespace (str): Namespace of the SA.
            csv_timeout (int): Seconds to wait for CSV (default 600).
            pods_timeout (int): Seconds to wait for pods (default 300).
        """
        logger.info("Starting OLS deployment (idempotent)")

        self.create_namespace()
        self.create_operatorgroup()
        self.create_subscription()
        self.wait_for_csv(timeout=csv_timeout)
        self.create_llm_secret(secret_name=secret_name, api_token=api_token)
        self.create_or_update_olsconfig(
            provider_name=provider_name,
            provider_type=provider_type,
            provider_url=provider_url,
            model_name=model_name,
            secret_name=secret_name,
            rag_images=rag_images,
        )
        self.wait_for_ols_pods(timeout=pods_timeout)
        self.expose_route()

        if username or (service_account and sa_namespace):
            self.grant_ols_user_role(
                username=username,
                service_account=service_account,
                sa_namespace=sa_namespace,
            )

        logger.info("OLS deployment complete")
