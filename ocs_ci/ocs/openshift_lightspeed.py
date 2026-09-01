"""
OpenShift Lightspeed (OLS) API client

Provides a Python interface for interacting with the OpenShift Lightspeed
REST API without using the web console UI.

For operator installation and OLSConfig management see
:mod:`ocs_ci.deployment.openshift_lightspeed`.

Reference:
    https://docs.redhat.com/en/documentation/red_hat_openshift_lightspeed/1.0/html-single/operate/index
"""

import base64
import logging
import time

import requests
import urllib3

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

# Disable TLS warnings when verify=False is used in test environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def is_ols_available(namespace=None, route_name=None):
    """
    Return ``True`` when the OLS ``lightspeed-app-server`` Route exists on
    the current cluster context, ``False`` otherwise.

    This is intentionally lightweight — it only checks for the Route object,
    not for pod readiness or API reachability.  Use it as a fast pre-check
    before constructing an :class:`OpenShiftLightspeed` client.

    Args:
        namespace (str): OLS namespace.  Defaults to ``constants.OLS_NAMESPACE``.
        route_name (str): OLS route name.  Defaults to ``constants.OLS_ROUTE_NAME``.

    Returns:
        bool: ``True`` if the OLS route exists, ``False`` if not found.
    """
    ns = namespace or constants.OLS_NAMESPACE
    rname = route_name or constants.OLS_ROUTE_NAME
    route_ocp = OCP(kind=constants.ROUTE, namespace=ns)
    return route_ocp.is_exist(resource_name=rname)


class OpenShiftLightspeed:
    """
    Client for the OpenShift Lightspeed (OLS) REST API.

    Authenticates using the current OpenShift user bearer token and
    communicates with the ``lightspeed-app-server`` route in the
    ``openshift-lightspeed`` namespace.

    Example::

        ols = OpenShiftLightspeed()
        response = ols.query("How do I create a DR Recipe for a busybox app?")
        print(response["response"])
    """

    # (connect_timeout, read_timeout) in seconds for every OLS HTTP call.
    # LLM responses can be slow (60–180 s) depending on model and prompt size;
    # read timeout is generous but bounded so a hung route cannot block the
    # test runner indefinitely.
    _REQUEST_TIMEOUT = (10, 300)

    def __init__(self, namespace=None, route_name=None, verify_tls=False):
        """
        Initialize the OLS client.

        On construction, :meth:`ensure_working_model` is called automatically:
        it reads the live OLSConfig, probes each model in the provider's
        ``models`` list via the LiteMaaS ``/v1/models`` endpoint, and patches
        ``spec.ols.defaultModel`` to the first healthy model if the current
        default is unavailable.  This means tests never need to manually
        switch models after a LiteMaaS outage.

        Args:
            namespace (str): Namespace where OLS is installed.
                Defaults to ``constants.OLS_NAMESPACE``.
            route_name (str): Name of the OLS route.
                Defaults to ``constants.OLS_ROUTE_NAME``.
            verify_tls (bool): Whether to verify TLS certificates.
                Set to False for clusters with self-signed certs (default).
        """
        self.namespace = namespace or constants.OLS_NAMESPACE
        self.route_name = route_name or constants.OLS_ROUTE_NAME
        self.verify_tls = verify_tls
        self._token = None
        self._base_url = None
        self.ensure_working_model()

    def ensure_working_model(self):
        """
        Auto-failover: read the live OLSConfig and switch ``defaultModel`` to
        the first healthy model if the current default is unavailable.

        Steps:
        1. Read ``spec.llm.providers[0]`` from the OLSConfig CR.
        2. Probe each model by sending a minimal ``/v1/chat/completions``
           request to the provider URL using the token from the OLS Secret.
        3. If the current ``defaultModel`` is unhealthy (non-2xx response) and
           a healthy alternative is found in the ``models`` list, patch the
           OLSConfig CR to set the new ``defaultModel``.

        Failures are non-fatal: if the OLSConfig cannot be read or all models
        fail the probe, a warning is logged and the method returns silently so
        that the normal ``is_authorized()`` / ``query()`` call flow can surface
        the real error to the test.
        """
        try:
            olsconfig_ocp = OCP(
                kind="OLSConfig",
                namespace=self.namespace,
                resource_name="cluster",
            )
            olsconfig = olsconfig_ocp.get()
        except Exception as exc:
            logger.warning(f"ensure_working_model: could not read OLSConfig: {exc}")
            return

        try:
            provider = olsconfig["spec"]["llm"]["providers"][0]
            provider_url = provider.get("url", "")
            secret_name = provider["credentialsSecretRef"]["name"]
            models = [m["name"] for m in provider.get("models", [])]
            current_default = olsconfig["spec"]["ols"].get("defaultModel", "")
            provider_name = olsconfig["spec"]["ols"].get("defaultProvider", "")
        except (KeyError, IndexError) as exc:
            logger.warning(f"ensure_working_model: unexpected OLSConfig shape: {exc}")
            return

        if not models or not provider_url:
            logger.warning(
                "ensure_working_model: no models or provider URL in OLSConfig"
            )
            return

        # Read the API token from the OLS Secret
        try:
            secret_ocp = OCP(kind="Secret", namespace=self.namespace)
            secret = secret_ocp.get(resource_name=secret_name)
            api_token = base64.b64decode(secret["data"].get("apitoken", "")).decode()
        except Exception as exc:
            logger.warning(
                f"ensure_working_model: could not read Secret '{secret_name}': {exc}"
            )
            return

        # Probe each model — send a tiny chat request directly to the provider
        def _probe(model_name):
            try:
                resp = requests.post(
                    f"{provider_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model_name,
                        "messages": [{"role": "user", "content": "ping"}],
                        "max_tokens": 1,
                    },
                    verify=self.verify_tls,
                    timeout=(10, 30),
                )
                return resp.status_code < 500
            except Exception:
                return False

        # Check if the current default is healthy
        if _probe(current_default):
            logger.info(
                f"ensure_working_model: default model '{current_default}' is healthy"
            )
            return

        logger.warning(
            f"ensure_working_model: default model '{current_default}' is unhealthy — "
            "probing fallback models"
        )

        # Find the first healthy alternative
        for model in models:
            if model == current_default:
                continue
            if _probe(model):
                logger.warning(
                    f"ensure_working_model: switching defaultModel "
                    f"'{current_default}' → '{model}'"
                )
                try:
                    olsconfig_ocp.patch(
                        resource_name="cluster",
                        params={"spec": {"ols": {"defaultModel": model}}},
                        format_type="merge",
                    )
                    logger.info(
                        f"ensure_working_model: OLSConfig patched — "
                        f"defaultModel is now '{model}' (provider: {provider_name})"
                    )
                except Exception as exc:
                    logger.warning(
                        f"ensure_working_model: failed to patch OLSConfig: {exc}"
                    )
                return

        logger.warning(
            "ensure_working_model: all models are unhealthy — "
            "proceeding anyway, queries will likely fail"
        )

    def _ensure_test_sa(self):
        """
        Ensure a dedicated ``ols-test-user`` ServiceAccount exists in the OLS
        namespace and has the ``lightspeed-operator-query-access`` ClusterRole
        bound to it.

        This is used as the token source on cert-based kubeconfigs
        (``system:admin``) where ``oc whoami --show-token`` is unavailable.
        The SA is created idempotently; if it or the ClusterRoleBinding already
        exists the step is skipped.

        Returns:
            str: Name of the service account (``constants.OLS_TEST_SA_NAME``).
        """
        sa_name = constants.OLS_TEST_SA_NAME
        ocp = OCP(namespace=self.namespace)

        # Create the SA if it doesn't exist
        existing = ocp.exec_oc_cmd(
            f"get serviceaccount {sa_name} -n {self.namespace} --ignore-not-found"
            " -o jsonpath='{.metadata.name}'",
            out_yaml_format=False,
        ).strip()
        if not existing:
            logger.info(
                f"Creating ServiceAccount '{sa_name}' in namespace '{self.namespace}'"
            )
            ocp.exec_oc_cmd(
                f"create serviceaccount {sa_name} -n {self.namespace}",
                out_yaml_format=False,
            )
        else:
            logger.info(
                f"ServiceAccount '{sa_name}' already exists — skipping creation"
            )

        # Bind query-access ClusterRole if the binding doesn't exist yet
        crb_name = "ols-test-user-query-access"
        existing_crb = ocp.exec_oc_cmd(
            f"get clusterrolebinding {crb_name} --ignore-not-found"
            " -o jsonpath='{.metadata.name}'",
            out_yaml_format=False,
        ).strip()
        if not existing_crb:
            logger.info(
                f"Granting '{constants.OLS_QUERY_ACCESS_ROLE}' to SA '{sa_name}'"
            )
            ocp.exec_oc_cmd(
                f"create clusterrolebinding {crb_name}"
                f" --clusterrole={constants.OLS_QUERY_ACCESS_ROLE}"
                f" --serviceaccount={self.namespace}:{sa_name}",
                out_yaml_format=False,
            )
        else:
            logger.info(f"ClusterRoleBinding '{crb_name}' already exists — skipping")

        return sa_name

    @property
    def token(self):
        """
        Bearer token for the current OpenShift user or a short-lived SA token.

        Two kubeconfig styles are handled:

        1. **Session-based** (``oc login``): ``oc whoami --show-token`` returns
           the bearer token directly.
        2. **Certificate-based** (installer ``auth/kubeconfig``, ``system:admin``):
           ``oc whoami --show-token`` fails with *"no token is currently in use"*.
           Falls back to creating a dedicated ``ols-test-user`` ServiceAccount,
           granting it the ``lightspeed-operator-query-access`` ClusterRole, and
           minting a short-lived token via ``oc create token``.

        Returns:
            str: Bearer token string.
        """
        if not self._token:
            from ocs_ci.ocs.exceptions import CommandFailed

            ocp = OCP(namespace=self.namespace)
            try:
                self._token = ocp.get_user_token()
                logger.info("Retrieved OLS bearer token via oc whoami --show-token")
            except CommandFailed as exc:
                if "no token is currently in use" not in str(exc):
                    raise
                logger.info(
                    "oc whoami --show-token failed (cert-based kubeconfig) — "
                    "creating ols-test-user SA and minting a token"
                )
                sa_name = self._ensure_test_sa()
                self._token = ocp.exec_oc_cmd(
                    f"create token {sa_name} -n {self.namespace}",
                    out_yaml_format=False,
                ).strip()
                logger.info(f"Retrieved OLS bearer token via oc create token {sa_name}")
        return self._token

    @property
    def base_url(self):
        """
        Base HTTPS URL of the OLS route.

        Returns:
            str: URL in the form ``https://<route-host>``.
        """
        if not self._base_url:
            route_ocp = OCP(
                kind=constants.ROUTE,
                namespace=self.namespace,
                resource_name=self.route_name,
            )
            host = route_ocp.get()["spec"]["host"]
            self._base_url = f"https://{host}"
            logger.info(f"OLS base URL resolved to: {self._base_url}")
        return self._base_url

    def _headers(self):
        """Build the common request headers."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def is_authorized(self):
        """
        Verify that the current user is authorized to use OLS.

        Returns:
            bool: True if the ``/authorized`` endpoint returns HTTP 200.

        Raises:
            requests.HTTPError: If the server returns an unexpected error.
        """
        url = f"{self.base_url}/authorized"
        logger.info(f"Checking OLS authorization at {url}")
        response = requests.post(
            url,
            headers=self._headers(),
            verify=self.verify_tls,
            timeout=self._REQUEST_TIMEOUT,
        )
        if response.status_code == 200:
            logger.info("OLS authorization check passed")
            return True
        logger.warning(
            f"OLS authorization check failed: {response.status_code} {response.text}"
        )
        response.raise_for_status()

    def query(
        self,
        query_text,
        conversation_id=None,
        provider=None,
        model=None,
        attachments=None,
        max_retries=3,
        retry_backoff=15,
    ):
        """
        Send a question to the OLS service (``POST /v1/query``).

        Transient 5xx errors and read timeouts from the LLM backend are
        retried automatically up to ``max_retries`` times with a
        ``retry_backoff``-second pause between attempts.  4xx errors are
        never retried (they indicate a bad request).

        Args:
            query_text (str): The question or prompt to send.
            conversation_id (str): Optional conversation ID for multi-turn
                conversations.  If provided OLS uses the prior history as
                context.
            provider (str): Optional LLM provider name to override the
                OLSConfig default.
            model (str): Optional model name to override the OLSConfig
                default.
            attachments (list): Optional list of attachment dicts, each
                containing ``attachment_type``, ``content_type`` and
                ``content`` keys.
            max_retries (int): Maximum number of retry attempts on 5xx /
                timeout (default 3).
            retry_backoff (int): Seconds to wait between retries (default 15).

        Returns:
            dict: Parsed JSON response from OLS, typically containing a
                ``response`` key with the LLM answer and a
                ``conversation_id`` key.

        Raises:
            requests.HTTPError: On HTTP 4xx responses or when all retries
                are exhausted on 5xx responses.
            requests.ReadTimeout: When all retries are exhausted on timeouts.
        """
        url = f"{self.base_url}/v1/query"
        payload = {"query": query_text}
        if conversation_id:
            payload["conversation_id"] = conversation_id
        if provider:
            payload["provider"] = provider
        if model:
            payload["model"] = model
        if attachments:
            payload["attachments"] = attachments

        last_exc = None
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"Sending OLS query (attempt {attempt}/{max_retries}): "
                    f"{query_text[:120]!r}..."
                )
                response = requests.post(
                    url,
                    json=payload,
                    headers=self._headers(),
                    verify=self.verify_tls,
                    timeout=self._REQUEST_TIMEOUT,
                )
            except requests.Timeout as exc:
                last_exc = exc
                logger.warning(
                    f"OLS query timed out (attempt {attempt}/{max_retries}): {exc}"
                )
                if attempt < max_retries:
                    time.sleep(retry_backoff)
                continue

            if not response.ok:
                logger.error(
                    f"OLS query failed — HTTP {response.status_code} "
                    f"(attempt {attempt}/{max_retries}): {response.text[:2000]}"
                )
                # 4xx = bad request, no point retrying
                if response.status_code < 500:
                    response.raise_for_status()
                last_exc = None
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_backoff}s...")
                    time.sleep(retry_backoff)
                    # Re-mint token in case it expired during the backoff
                    self._token = None
                    continue
                response.raise_for_status()

            result = response.json()
            logger.info(
                f"OLS response received "
                f"(conversation_id={result.get('conversation_id')})"
            )
            return result

        # All retries exhausted — re-raise the last timeout exception if any
        if last_exc:
            raise last_exc
        response.raise_for_status()

    def list_conversations(self):
        """
        List all conversations for the current user (``GET /v1/conversations``).

        Returns:
            list: List of conversation summary dicts with ``conversation_id``,
                ``summary``, and ``message_count`` fields.
        """
        url = f"{self.base_url}/v1/conversations"
        logger.info("Listing OLS conversations")
        response = requests.get(
            url,
            headers=self._headers(),
            verify=self.verify_tls,
            timeout=self._REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    def get_conversation(self, conversation_id):
        """
        Retrieve the full message history for a conversation
        (``GET /v1/conversations/{id}``).

        Args:
            conversation_id (str): The conversation ID to retrieve.

        Returns:
            dict: Conversation history object.
        """
        url = f"{self.base_url}/v1/conversations/{conversation_id}"
        logger.info(f"Getting OLS conversation: {conversation_id}")
        response = requests.get(
            url,
            headers=self._headers(),
            verify=self.verify_tls,
            timeout=self._REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    def delete_conversation(self, conversation_id):
        """
        Delete a conversation's history
        (``DELETE /v1/conversations/{id}``).

        Args:
            conversation_id (str): The conversation ID to delete.

        Returns:
            bool: True on successful deletion (HTTP 204).
        """
        url = f"{self.base_url}/v1/conversations/{conversation_id}"
        logger.info(f"Deleting OLS conversation: {conversation_id}")
        response = requests.delete(
            url,
            headers=self._headers(),
            verify=self.verify_tls,
            timeout=self._REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        logger.info(f"Conversation {conversation_id} deleted")
        return True

    def submit_feedback(self, conversation_id, sentiment, feedback_text=None):
        """
        Submit sentiment feedback for a conversation
        (``POST /v1/feedback``).

        Args:
            conversation_id (str): The conversation ID to rate.
            sentiment (int): ``1`` for positive, ``-1`` for negative.
            feedback_text (str): Optional free-text feedback.

        Returns:
            dict: Response from the feedback endpoint.
        """
        url = f"{self.base_url}/v1/feedback"
        payload = {"conversation_id": conversation_id, "sentiment": sentiment}
        if feedback_text:
            payload["feedback_text"] = feedback_text
        logger.info(
            f"Submitting OLS feedback sentiment={sentiment} for conversation {conversation_id}"
        )
        response = requests.post(
            url,
            json=payload,
            headers=self._headers(),
            verify=self.verify_tls,
            timeout=self._REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
