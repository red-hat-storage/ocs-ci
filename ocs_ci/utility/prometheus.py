import base64
import contextlib
import logging
import os
import requests
import tempfile
import time
import yaml
from threading import Timer
from datetime import datetime

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import AlertingError, AuthError, NoThreadingLockUsedError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.ssl_certs import get_root_ca_cert
from ocs_ci.utility.utils import TimeoutIterator

logger = logging.getLogger(__name__)


# TODO(fbalak): if ignore_more_occurences is set to False then tests are flaky.
# The root cause should be inspected.
def check_alert_list(
    label,
    msg,
    alerts,
    states,
    severity="warning",
    ignore_more_occurences=True,
    description=None,
    runbook=None,
):
    """
    Check list of alerts that there are alerts with requested label and
    message for each provided state. If some alert is missing then this check
    fails.

    Args:
        label (str): Alert label
        msg (str): Alert message
        alerts (list): List of alerts to check
        states (list): List of states to check, order is important
        ignore_more_occurences (bool): If true then there is checkced only
            occurence of alert with requested label, message and state but
            it is not checked if there is more of occurences than one.
        description (str): Alert description
        runbook (str): Alert's runbook URL

    """
    target_alerts = [
        alert for alert in alerts if alert.get("labels").get("alertname") == label
    ]
    logger.info(f"Checking properties of found {label} alerts")

    for key, state in enumerate(states):
        found_alerts = [
            alert
            for alert in target_alerts
            if alert["annotations"]["message"] == msg
            and alert["annotations"]["severity_level"] == severity
            and alert["state"] == state
        ]
        assert_msg = (
            f"There was not found alert {label} with message: {msg}, "
            f"severity: {severity} in state: {state}"
            f"Alerts matched with alert name are {target_alerts}"
            f"Alerts matched with given message, severity and state are {found_alerts}"
        )
        assert found_alerts, assert_msg

        if not ignore_more_occurences:
            assert_msg = (
                f"There are multiple instances of alert {label} with "
                f"message: {msg}, severity: {severity} in state: {state}"
            )
            assert len(found_alerts) == 1, assert_msg

        if description:
            assert_msg = f"Alert description for alert {label} is not correct"
            assert (
                found_alerts[key]["annotations"]["description"] == description
            ), assert_msg

        if runbook:
            assert_msg = f"Alert runbook url for alert {label} is not correct"
            assert (
                found_alerts[key]["annotations"]["runbook_url"] == runbook
            ), assert_msg

    logger.info("Alerts were triggered correctly during utilization")


def validate_alert(
    threading_lock,
    alert_constant,
    message,
    description,
    runbook,
    severity="warning",
    state="pending",
    timeout=1200,
    sleep=None,
):
    """
    Wait for an alert and validate its properties.

    Args:
        threading_lock: Threading lock object for thread-safe Prometheus API operations
        alert_constant (str): Alert name constant
        message (str): Expected alert message
        description (str): Expected alert description
        runbook (str): Expected runbook URL
        severity (str): Expected severity
        state (str): Alert state to wait for
        timeout (int): Timeout in seconds to wait for the alert
        sleep (int): Optional polling interval for alert wait

    Returns:
        bool: True if alert is validated successfully, False otherwise
    """
    api = PrometheusAPI(threading_lock=threading_lock)
    wait_kwargs = {"name": alert_constant, "state": state, "timeout": timeout}
    if sleep is not None:
        wait_kwargs["sleep"] = sleep
    alerts = api.wait_for_alert(**wait_kwargs)

    try:
        check_alert_list(
            label=alert_constant,
            msg=message,
            description=description,
            runbook=runbook,
            states=[state],
            severity=severity,
            alerts=alerts,
        )
        logger.info("Alert verified successfully")
        return True
    except AssertionError:
        return False


def check_query_range_result_viafunction(
    result,
    is_value_good,
    is_value_bad=lambda val: False,
    exp_metric_num=None,
    exp_delay=None,
    exp_good_time=None,
    is_float=False,
):
    """
    Check that result of range query matches expectations expressed via
    ``is_value_good`` (and optionally ``is_value_bad``) functions, which takes
    a value and returns True if the value is good (or bad).

    Args:
        result (list): Data from ``query_range()`` method.
        is_value_good (function): returns True for a good value
        is_value_bad (function): returns True for a bad balue, indicating a
            problem (optional, use if you need to distinguish bad and invalid
            values)
        exp_metric_num (int): expected number of data series in the result,
            optional (eg. for ``ceph_health_status`` this would be 1, but
            for something like ``ceph_osd_up`` this will be a number of
            OSDs in the cluster)
        exp_delay (int): Number of seconds from the start of the query
            time range for which we should tolerate bad values. This is
            useful if you change cluster state and processing of this
            change is expected to take some time.
        exp_good_time (int): Number of seconds during which we should see
            good values in the metrics data. When this time passess values
            can go bad (but can't be invalid). If not specified, good values
            should be presend during the whole time.
        is_float (bool): assume that the value is float, otherwise assume int

    Returns:
        bool: True if result matches given expectations, False otherwise
    """
    logger.info("Validating a result of a range query")
    # result of the validation
    is_result_ok = True
    # timestamps of values in bad_values list
    bad_value_timestamps = []
    # timestamps of values outside of both bad and good values list
    invalid_value_timestamps = []

    # check that result contains expected number of metric data series
    if exp_metric_num is not None and len(result) != exp_metric_num:
        msg = (
            f"result doesn't contain {exp_metric_num} of series only, "
            f"actual number data series is {len(result)}"
        )
        logger.error(msg)
        is_result_ok = False

    for metric in result:
        name = metric["metric"]["__name__"]
        logger.info(f"checking metric {metric['metric']}")
        # get start of the query range for which we are processing data
        start_ts = metric["values"][0][0]
        start_dt = datetime.utcfromtimestamp(start_ts)
        logger.info(f"metrics for {name} starts at {start_dt}")
        for ts, value in metric["values"]:
            if is_float:
                value = float(value)
            else:
                value = int(value)
            dt = datetime.utcfromtimestamp(ts)
            if is_value_good(value):
                logger.debug(f"{name} has good value {value} at {dt}")
            elif is_value_bad(value):
                msg = f"{name} has bad value {value} at {dt}"
                # delta is time since start of the query range
                delta = dt - start_dt
                if exp_delay is not None and delta.seconds < exp_delay:
                    logger.info(msg + f" but within expected {exp_delay}s delay")
                elif exp_good_time is not None and delta.seconds >= exp_good_time:
                    logger.info(msg + f" but after {exp_good_time}s already passed")
                else:
                    logger.error(msg)
                    bad_value_timestamps.append(dt)
            else:
                msg = f"{name} invalid (not good or bad): {value} at {dt}"
                logger.error(msg)
                invalid_value_timestamps.append(dt)

    if bad_value_timestamps != []:
        is_result_ok = False
    else:
        logger.info("No bad values detected")
    if invalid_value_timestamps != []:
        is_result_ok = False
    else:
        logger.info("No invalid values detected")

    return is_result_ok


def check_query_range_result_enum(
    result,
    good_values,
    bad_values=(),
    exp_metric_num=None,
    exp_delay=None,
    exp_good_time=None,
):
    """
    Check that result of range query matches given expectations. Useful
    for metrics which convey status (eg. ceph health, ceph_osd_up), so that
    you can assume that during a given period, it's value should match
    given single (or tuple of) value(s).

    Args:
        result (list): Data from ``query_range()`` method.
        good_values (tuple): Tuple of values considered good
        bad_values (tuple): Tuple of values considered bad, indicating a
            problem (optional, use if you need to distinguish bad and
            invalid values)
        exp_metric_num (int): expected number of data series in the result,
            optional (eg. for ``ceph_health_status`` this would be 1, but
            for something like ``ceph_osd_up`` this will be a number of
            OSDs in the cluster)
        exp_delay (int): Number of seconds from the start of the query
            time range for which we should tolerate bad values. This is
            useful if you change cluster state and processing of this
            change is expected to take some time.
        exp_good_time (int): Number of seconds during which we should see
            good values in the metrics data. When this time passess values
            can go bad (but can't be invalid). If not specified, good values
            should be presend during the whole time.

    Returns:
        bool: True if result matches given expectations, False otherwise
    """
    is_value_good = lambda val: val in good_values  # noqa: E731
    is_value_bad = lambda val: val in bad_values  # noqa: E731
    is_result_ok = check_query_range_result_viafunction(
        result,
        is_value_good,
        is_value_bad,
        exp_metric_num,
        exp_delay,
        exp_good_time,
        is_float=False,
    )
    return is_result_ok


def check_query_range_result_limits(
    result,
    good_min,
    good_max,
    exp_metric_num=None,
    exp_delay=None,
    exp_good_time=None,
):
    """
    Check that result of range query matches given expectations. Useful
    for metrics which convey continuous value, eg. storage or cpu utilization.

    Args:
        result (list): Data from ``query_range()`` method.
        good_min (float): Min. value which is considered good.
        good_max (float): Max. value which is still considered as good.
        exp_metric_num (int): expected number of data series in the result,
            optional (eg. for ``ceph_health_status`` this would be 1, but
            for something like ``ceph_osd_up`` this will be a number of
            OSDs in the cluster)
        exp_delay (int): Number of seconds from the start of the query
            time range for which we should tolerate bad values. This is
            useful if you change cluster state and processing of this
            change is expected to take some time.
        exp_good_time (int): Number of seconds during which we should see
            good values in the metrics data. When this time passess values
            can go bad (but can't be invalid). If not specified, good values
            should be presend during the whole time.

    Returns:
        bool: True if result matches given expectations, False otherwise
    """
    is_value_good = lambda val: good_min <= val <= good_max  # noqa: E731
    is_value_bad = lambda val: False  # noqa: E731
    is_result_ok = check_query_range_result_viafunction(
        result,
        is_value_good,
        is_value_bad,
        exp_metric_num,
        exp_delay,
        exp_good_time,
        is_float=True,
    )
    return is_result_ok


def log_parsing_error(query, resp_content, ex):
    """
    Log an error raised during parsing of a prometheus query.

    Args:
        query (dict): Full specification of a prometheus query.
        resp_content (bytes): Response from prometheus
        ex (Exception): Exception raised during parsing of prometheus reply.

    """
    logger.error(
        "For query '%s' Prometheus returned a response which " "failed to be parsed.",
        query,
    )
    logger.debug(ex)
    logger.debug("prometheus reply which failed to load:\n%s\n", resp_content)


def validate_status(content):
    """
    Validate content data from Prometheus. If this fails, Prometheus instance
    or a query is so broken that test can't be performed. We assume that
    Prometheus reports "success" even for queries which returns nothing.

    Args:
        content (dict): data from Prometheus

    Raises:
        TypeError: when content is not a dict
        ValueError: when status of the content is not success
    """
    logger.debug("content value: %s", content)
    if not isinstance(content, dict):
        logger.error("content is not a dict, but %s", type(content))
        raise TypeError("content is not a dict")
    status = content.get("status")
    if status != "success":
        logger.error("content status is not success, but %s", status)
        raise ValueError("content status is not success")


def _validate_alert_instance(
    alert, alert_name, instance_index, expected_severity, expected_message_substr
):
    """
    Assert that a single alert instance matches the expected criteria.

    Args:
        alert (dict): A single alert record from the Prometheus API.
        alert_name (str): Alert name (used in assertion messages).
        instance_index (int): Instance index (used in assertion messages).
        expected_severity (str | None): If given, assert that
            ``alert["labels"]["severity"]`` equals this value.
        expected_message_substr (str | None): If given, assert that this
            substring appears (case-insensitive) in
            ``alert["annotations"]["message"]``.

    Raises:
        AssertionError: If any validation check fails.
    """
    if expected_severity is not None:
        assert alert["labels"]["severity"] == expected_severity, (
            f"Alert '{alert_name}' instance {instance_index}: expected severity "
            f"'{expected_severity}', "
            f"got '{alert['labels']['severity']}'"
        )
    if expected_message_substr is not None:
        assert expected_message_substr.lower() in (
            alert["annotations"]["message"].lower()
        ), (
            f"Alert '{alert_name}' instance {instance_index}: expected "
            f"'{expected_message_substr}' in message: "
            f"{alert['annotations']['message']}"
        )
    assert alert["annotations"].get("runbook_url") or alert["annotations"].get(
        "description"
    ), (
        f"Alert '{alert_name}' instance {instance_index} is missing both "
        f"runbook_url and description"
    )


def wait_for_alert_firing(
    api,
    alert_name,
    timeout=600,
    expected_severity=None,
    expected_message_substr=None,
    min_count=1,
):
    """
    Wait for a Prometheus alert to reach the ``firing`` state and validate it.

    Args:
        api (PrometheusAPI): Prometheus API instance.
        alert_name (str): Alert name to wait for.
        timeout (int): Seconds to wait for the alert to fire.
        expected_severity (str | None): If given, assert that
            ``alert["labels"]["severity"]`` equals this value.
        expected_message_substr (str | None): If given, assert that this
            substring appears (case-insensitive) in
            ``alert["annotations"]["message"]``.
        min_count (int): Minimum number of firing alert instances to
            wait for. Defaults to 1.

    Returns:
        list[dict]: All fired alert records (at least ``min_count``).

    Raises:
        AssertionError: If fewer than ``min_count`` alerts fire, or any
            validation fails.
    """
    alerts = api.wait_for_alert(
        name=alert_name,
        state="firing",
        timeout=timeout,
        sleep=10,
        min_count=min_count,
    )
    assert len(alerts) >= min_count, (
        f"Expected at least {min_count} '{alert_name}' alert(s) to fire "
        f"within {timeout}s, got {len(alerts)}"
    )
    for instance_index, alert in enumerate(alerts):
        _validate_alert_instance(
            alert,
            alert_name,
            instance_index,
            expected_severity,
            expected_message_substr,
        )
    logger.info(
        "Alert '%s' fired: %d instance(s), labels=%s message=%s",
        alert_name,
        len(alerts),
        alerts[0]["labels"],
        alerts[0]["annotations"].get("message"),
    )
    return alerts


def wait_for_alert_cleared(api, alert_name, timeout=600):
    """
    Wait for a Prometheus alert to disappear (no longer firing or pending).

    Args:
        api (PrometheusAPI): Prometheus API instance.
        alert_name (str): Alert name to wait for clearing.
        timeout (int): Seconds to wait for the alert to clear.

    Raises:
        AssertionError: If the alert is still present after ``timeout``.
    """
    alerts = api.wait_for_alert(name=alert_name, timeout=timeout, sleep=10)
    assert not alerts, f"Alert '{alert_name}' did not clear within {timeout}s"
    logger.info("Alert '%s' cleared successfully", alert_name)


class PrometheusAPI(object):
    """
    This is wrapper class for Prometheus API.
    """

    _token = None
    _user = None
    _password = None
    _endpoint = None
    _cacert = False
    _threading_lock = None
    _cluster_context = None

    def __init__(
        self,
        user=None,
        password=None,
        threading_lock=None,
        cluster_context=config.RunWithProviderConfigContextIfAvailable,
    ):
        """
        Constructor for PrometheusAPI class.

        Args:
            user (str): OpenShift username used to connect to API
            password (str): Password for the OpenShift username used to connect to API
            threading_lock (threading.RLock): Lock used for synchronization of the
                threads in Prometheus calls
            cluster_context (object): context object in which the bucket will be created.
                Default is provider context.

        """
        if threading_lock is None:
            raise NoThreadingLockUsedError(
                "using threading.Lock object is mandatory for PrometheusAPI class"
            )
        self._cluster_context = cluster_context
        with self._cluster_context():
            if (
                config.ENV_DATA["platform"].lower() == "ibm_cloud"
                and config.ENV_DATA["deployment_type"] == "managed"
            ):
                self._user = user or "apikey"
                self._password = password or config.AUTH["ibmcloud"]["api_key"]
            else:
                self._user = user or config.RUN["username"]
                if not password:
                    filename = os.path.join(
                        config.ENV_DATA["cluster_path"], config.RUN["password_location"]
                    )
                    with open(filename) as f:
                        password = f.read().rstrip("\n")
                self._password = password
            self._threading_lock = threading_lock
            self.refresh_connection()
            if (
                not config.ENV_DATA["platform"].lower() == "ibm_cloud"
                and not config.ENV_DATA["platform"].lower()
                == constants.ROSA_HCP_PLATFORM
                and config.ENV_DATA["deployment_type"] == "managed"
            ):
                self.generate_cert()

    def refresh_connection(self):
        """
        Login into OCP, refresh endpoint and token.
        """
        with self._cluster_context():
            kubeconfig = config.RUN["kubeconfig"]
            ocp = OCP(
                kind=constants.ROUTE,
                namespace=defaults.OCS_MONITORING_NAMESPACE,
                threading_lock=self._threading_lock,
                cluster_kubeconfig=kubeconfig,
                skip_tls_verify=config.ENV_DATA.get("skip_tls_verify", False),
            )
            kube_data = ""
            with open(kubeconfig, "r") as kube_file:
                kube_data = kube_file.readlines()
            login_ok = ocp.login(self._user, self._password)
            if not login_ok:
                raise AuthError("Login to OCP failed")
            self._token = ocp.get_user_token()
            with open(kubeconfig, "w") as kube_file:
                kube_file.writelines(kube_data)
            route_obj = ocp.get(resource_name=defaults.PROMETHEUS_ROUTE)
            self._endpoint = "https://" + route_obj["spec"]["host"]

    def generate_cert(self):
        """
        Generate CA certificate from kubeconfig for API.

        TODO: find proper way how to generate/load cert files.
        """
        with self._cluster_context():
            if config.DEPLOYMENT.get("use_custom_ingress_ssl_cert"):
                cert_provider = config.DEPLOYMENT.get("custom_ssl_cert_provider")
                if cert_provider == constants.SSL_CERT_PROVIDER_OCS_QE_CA:
                    self._cacert = get_root_ca_cert()
                    return
                elif cert_provider == constants.SSL_CERT_PROVIDER_LETS_ENCRYPT:
                    self._cacert = True
                    return
            kubeconfig_path = os.path.join(
                config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
            )
            with open(kubeconfig_path, "r") as f:
                kubeconfig = yaml.load(f, yaml.Loader)
            cert_file = tempfile.NamedTemporaryFile(delete=False)
            cert_file.write(
                base64.b64decode(
                    kubeconfig["clusters"][0]["cluster"]["certificate-authority-data"]
                )
            )
            cert_file.close()
            self._cacert = cert_file.name
            logger.info(f"Generated CA certification file: {self._cacert}")

    def get(self, resource, payload=None, timeout=300):
        """
        Get alerts from Prometheus API.

        Args:
            resource (str): Represents part of uri that specifies given
                resource
            payload (dict): Provide parameters to GET API call.
                e.g. for `alerts` resource this can be
                {'silenced': False, 'inhibited': False}
            timeout (int): Number of seconds to wait for Prometheus endpoint to
                get available if it is not available

        Returns:
            requests.models.Response: Response from Prometheus alerts api
        """
        pattern = f"/api/v1/{resource}"
        headers = {"Authorization": f"Bearer {self._token}"}

        logger.debug(f"GET {self._endpoint + pattern}")
        logger.debug(f"headers={headers}")
        logger.debug(f"verify={self._cacert}")
        logger.debug(f"params={payload}")

        if timeout:
            with self._cluster_context():
                for sample_response in TimeoutIterator(
                    timeout=timeout,
                    sleep=15,
                    func=requests.get,
                    func_kwargs={
                        "url": self._endpoint + pattern,
                        "headers": headers,
                        "verify": self._cacert,
                        "params": payload,
                        "timeout": 60,
                    },
                ):
                    response = sample_response
                    if not response.ok:
                        logger.warning(
                            f"There was an error in response: {response.text}"
                        )
                        logger.warning("Refreshing connection")
                        self.refresh_connection()
                        if (
                            not config.ENV_DATA["platform"].lower() == "ibm_cloud"
                            and config.ENV_DATA["deployment_type"] == "managed"
                        ):
                            logger.warning("Generating new certificate")
                            self.generate_cert()
                        logger.warning("Connection refreshed")
                    else:
                        break
            return response
        else:
            with self._cluster_context():
                response = requests.get(
                    self._endpoint + pattern,
                    headers=headers,
                    verify=self._cacert,
                    params=payload,
                    timeout=60,
                )
            return response

    def query(
        self,
        query,
        timestamp=None,
        timeout=None,
        validate=True,
        mute_logs=False,
        log_debug=False,
    ):
        """
        Perform Prometheus `instant query`_. This is a simple wrapper over
        ``get()`` method with plumbing code for instant queries, additional
        validation and logging.

        Args:
            query (str): Prometheus expression query string.
            timestamp (str): Evaluation timestamp (rfc3339 or unix timestamp).
                Optional.
            timeout (str): Evaluation timeout in duration format. Optional.
            validate (bool): Perform basic validation on the response.
                Optional, ``True`` is the default. Use ``False`` when you
                expect query to fail eg. during negative testing.
            mute_logs (bool): True for muting the logs, False otherwise
            log_debug (bool): True for logging in debug, False otherwise

        Returns:
            list: Result of the query (value(s) for a single timestamp)

        .. _`instant query`: https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
        """
        with self._cluster_context():
            query_payload = {"query": query}
            log_msg = f"Performing prometheus instant query '{query}'"
            if timestamp is not None:
                query_payload["time"] = timestamp
                log_msg += f" for timestamp {timestamp}"
            if timeout is not None:
                query_payload["timeout"] = timeout
            # Log human readable summary of the query
            if not mute_logs:
                if log_debug:
                    logger.debug(log_msg)
                else:
                    logger.info(log_msg)
            resp = self.get("query", payload=query_payload)
            try:
                content = yaml.safe_load(resp.content)
            except Exception as ex:
                log_parsing_error(query_payload, resp.content, ex)
                raise
            if validate:
                validate_status(content)
        # return actual result of the query
        return content["data"]["result"]

    def query_range(self, query, start, end, step, timeout=None, validate=True):
        """
        Perform Prometheus `range query`_. This is a simple wrapper over
        ``get()`` method with plumbing code for range queries, additional
        validation and logging.

        Args:
            query (str): Prometheus expression query string.
            start (str): start timestamp (rfc3339 or unix timestamp)
            end (str): end timestamp (rfc3339 or unix timestamp)
            step (float): Query resolution step width as float number of
                seconds.
            timeout (str): Evaluation timeout in duration format. Optional.
            validate (bool): Perform basic validation on the response.
                Optional, ``True`` is the default. Use ``False`` when you
                expect query to fail eg. during negative testing.

        Returns:
            list: result of the query

        .. _`range query`: https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
        """
        with self._cluster_context():
            query_payload = {"query": query, "start": start, "end": end, "step": step}
            if timeout is not None:
                query_payload["timeout"] = timeout
            # Human readable summary of the query (details are logged by get
            # method itself with debug level).
            logger.info(
                (
                    f"Performing prometheus range query '{query}' "
                    f"over a time range ({start}, {end})"
                )
            )
            resp = self.get("query_range", payload=query_payload)
            try:
                content = yaml.safe_load(resp.content)
            except Exception as ex:
                log_parsing_error(query_payload, resp.content, ex)
                raise
            if validate:
                # If this fails, Prometheus instance is so broken that test can't
                # be performed.
                validate_status(content)
                # For a range query, we should always get a matrix result type, as
                # noted in Prometheus documentation, see:
                # https://prometheus.io/docs/prometheus/latest/querying/api/#range-vectors
                result_type = content["data"].get("resultType")
                if result_type != "matrix":
                    logger.error("unexpected resultType: %s", result_type)
                    raise ValueError("resultType is not matrix but %s", result_type)
                # All metric sample series has the same size.
                sizes = []
                for metric in content["data"]["result"]:
                    sizes.append(len(metric["values"]))
                if not all(size == sizes[0] for size in sizes):
                    msg = "Metric sample series doesn't have the same size."
                    logger.error(msg)
                    raise ValueError(msg)
                # Check if the query result is empty (which is a valid answer from
                # validation standpoint).
                if len(sizes) == 0:
                    logger.warning("prometheus query result is empty")
                else:
                    # Check that we don't have holes in the response. If this
                    # fails, our Prometheus instance is missing some part of the
                    # data we are asking it about. For positive test cases, this is
                    # most likely a test blocker product bug.
                    start_dt = datetime.utcfromtimestamp(start)
                    end_dt = datetime.utcfromtimestamp(end)
                    duration = end_dt - start_dt
                    exp_samples = duration.seconds / step
                    if exp_samples - 1 <= sizes[0] <= exp_samples + 1:
                        logger.debug("there are no holes in the data")
                    else:
                        msg = "there are holes in prometheus data"
                        logger.error(
                            msg
                            + ": result size is %d while expected sample size is %d +-1",
                            sizes[0],
                            exp_samples,
                        )
                        raise ValueError(msg)
        # return actual result of the query
        return content["data"]["result"]

    def wait_for_alert(self, name, state=None, timeout=1200, sleep=5, min_count=1):
        """
        Search for alerts that have requested name and state.

        Args:
            name (str): Alert name
            state (str): Alert state, if provided then there are searched
                alerts with provided state. If not provided then alerts are
                searched for absence of the alert. Loop that looks for alerts
                is broken when there are no alerts returned from API. This
                is done because API is not returning any alerts that are not
                in pending or firing state
            timeout (int): Number of seconds for how long the alert should
                be searched
            sleep (int): Number of seconds to sleep in between alert search
            min_count (int): Minimum number of matching alerts required
                before breaking out of the polling loop (only applies when
                ``state`` is provided). Defaults to 1.

        Returns:
            list: List of alert records
        """
        with self._cluster_context():
            while timeout > 0:
                alerts_response = self.get(
                    "alerts",
                    payload={
                        "silenced": False,
                        "inhibited": False,
                    },
                )
                msg = f"Request {alerts_response.request.url} failed"
                if not alerts_response.ok:
                    logger.error(msg)
                    raise AlertingError(msg)
                if state:
                    alerts = [
                        alert
                        for alert in alerts_response.json().get("data").get("alerts")
                        if alert.get("labels").get("alertname") == name
                        and alert.get("state") == state
                    ]
                    logger.info(
                        f"Checking for {name} alerts with state {state}... "
                        f"{len(alerts)} found (need {min_count})"
                    )
                    if len(alerts) >= min_count:
                        break
                else:
                    # search for missing alerts, search is completed when
                    # there are no alerts with given name
                    alerts = [
                        alert
                        for alert in alerts_response.json().get("data").get("alerts")
                        if alert.get("labels").get("alertname") == name
                    ]
                    logger.info(
                        f"Checking for {name} alerts. There should be no alerts ... "
                        f"{len(alerts)} found"
                    )
                    if len(alerts) == 0:
                        break
                time.sleep(sleep)
                timeout -= sleep
        return alerts

    def get_alerts_by_labels(self, alert_name, labels_dict):
        """
        Get alerts (pending or firing) matching a specific alert name and
        label values.

        Args:
            alert_name (str): Alert name to match.
            labels_dict (dict): Label key-value pairs to filter by
                (e.g. {"source_bucket": "my-bucket"}).

        Returns:
            list: Matching alert records, empty if none found.
        """
        with self._cluster_context():
            response = self.get(
                "alerts",
                payload={"silenced": False, "inhibited": False},
            )
            if not response.ok:
                raise AlertingError(f"Request {response.request.url} failed")
            return [
                alert
                for alert in response.json()["data"]["alerts"]
                if alert["labels"].get("alertname") == alert_name
                and all(alert["labels"].get(k) == v for k, v in labels_dict.items())
            ]

    def check_alert_cleared(self, label, measure_end_time, time_min=120):
        """
        Check that all alerts with provided label are cleared.

        Args:
            label (str): Alerts label
            measure_end_time (int): Timestamp of measurement end
            time_min (int): Number of seconds to wait for alert to be cleared
                since measurement end
        """
        with self._cluster_context():
            time_actual = time.time()
            time_wait = int((measure_end_time + time_min) - time_actual)
            if time_wait > 0:
                logger.info(
                    f"Waiting for approximately {time_wait} seconds for alerts "
                    f"to be cleared ({time_min} seconds since measurement end)"
                )
            else:
                time_wait = 1
            cleared_alerts = self.wait_for_alert(
                name=label, state=None, timeout=time_wait
            )
            logger.info(f"Cleared alerts: {cleared_alerts}")
            if len(cleared_alerts) == 0:
                logger.info(f"{label} alerts were cleared")
            else:
                error_msg = f"{label} alerts were not cleared"
                logger.error(error_msg)
                raise AlertingError(error_msg)

    def prometheus_log(self, prometheus_alert_list, log_level=logging.INFO):
        """
        Log all alerts from Prometheus API to list

        Args:
            prometheus_alert_list (list): List to be populated with alerts
            log_level (int): Level used for logging of newly collected alerts

        Returns:
            bool: True if alerts were successfully collected from Prometheus,
                False if the Prometheus request failed

        """

        with self._cluster_context():
            alerts_response = self.get(
                "alerts", payload={"silenced": False, "inhibited": False}
            )
            msg = f"Request {alerts_response.request.url} failed"
            if alerts_response.ok:
                for alert in alerts_response.json().get("data").get("alerts"):
                    if alert not in prometheus_alert_list:
                        logger.log(log_level, f"Adding {alert} to alert list")
                        prometheus_alert_list.append(alert)
                return True
            else:
                # no need raise Assertion error or Exception here:
                # 1. It will not lead to a test failure, fixture is in parallel Thread, in SetUp
                # 2. One bad response should not fail the test
                # 3. If Prometheus stopped responding, or we missed alert the test will fail anyway
                #    on checking alert list
                logger.error(msg)
                return False

    def verify_alerts_via_prometheus(self, expected_alerts, threading_lock):
        """
        Verify Alerts on prometheus

        Args:
            expected_alerts (list): list of alert names
            threading_lock (threading.Rlock): Lock object to prevent simultaneous calls to 'oc'

        Returns:
            bool: True if expected_alerts exist, False otherwise

        """
        logger.info("Logging of all prometheus alerts started")
        with self._cluster_context():
            alerts_response = self.get(
                "alerts", payload={"silenced": False, "inhibited": False}
            )
        actual_alerts = list()
        for alert in alerts_response.json().get("data").get("alerts"):
            actual_alerts.append(alert.get("labels").get("alertname"))
            logger.info(f"Actual Alerts: {actual_alerts}")
        for expected_alert in expected_alerts:
            if expected_alert not in actual_alerts:
                logger.error(
                    f"{expected_alert} alert does not exist in alerts list."
                    f"The actual alerts : {actual_alerts}"
                )
                return False
        return True


class PrometheusAlertSubscriber(Timer):

    def __init__(
        self, threading_lock, interval: float, alert_list=None, log_level=logging.INFO
    ):
        """
        Args:
            threading_lock (threading.RLock): Lock used for synchronization of
                the threads in Prometheus calls
            interval (float): Number of seconds between Prometheus polls
            alert_list (list): List to be populated with collected alerts. When
                not provided, a new list is created. Providing own list is
                useful when the caller needs to access collected alerts also
                before the subscriber is unsubscribed.
            log_level (int): Level used for logging of newly collected alerts

        """
        self.prometheus_alert_list = alert_list if alert_list is not None else []
        self.prometheus_api = PrometheusAPI(threading_lock=threading_lock)
        # Numbers of Prometheus polls that succeeded and failed. They allow the
        # consumer of collected alerts to distinguish an empty alert list
        # caused by unavailable Prometheus from a successful collection where
        # no alert was fired.
        self.successful_polls = 0
        self.failed_polls = 0
        super().__init__(
            interval,
            lambda: self._poll(log_level),
        )

    def _poll(self, log_level):
        """
        Collect alerts from Prometheus and record the result of the poll.

        Args:
            log_level (int): Level used for logging of newly collected alerts

        """
        if self.prometheus_api.prometheus_log(
            self.prometheus_alert_list, log_level=log_level
        ):
            self.successful_polls += 1
        else:
            self.failed_polls += 1

    def run(self):
        """
        Run logging of all prometheus alerts.

        ! This method is called by Timer class, do not call it directly !
        """
        while not self.finished.wait(self.interval):
            try:
                self.function(*self.args, **self.kwargs)
            except Exception:
                # Prometheus might be temporarily unavailable, for example when
                # the monitoring stack itself is being upgraded. One failed
                # poll must not terminate the whole collection.
                self.failed_polls += 1
                logger.exception("Collection of prometheus alerts failed")

    def get_alerts(self):
        """
        Get list of all alerts
        """
        return self.prometheus_alert_list

    def clear_alerts(self):
        """
        Clear alert list
        """
        self.prometheus_alert_list.clear()

    def subscribe(self):
        """
        Start logging of all prometheus alerts
        """
        logger.info("Logging of all prometheus alerts started")
        self.daemon = True
        self.start()

    def unsubscribe(self):
        """
        Stop logging of all prometheus alerts
        """
        self.cancel()
        logger.info("Logging of all prometheus alerts stopped")


@contextlib.contextmanager
def alert_collection(
    threading_lock,
    alert_list=None,
    interval=10,
    log_level=logging.DEBUG,
    status=None,
    stop_timeout=360,
):
    """
    Context manager that collects all Prometheus alerts fired during the
    execution of the wrapped block of code.

    Problems with Prometheus (e.g. unreachable endpoint during an upgrade of
    the monitoring stack) are logged but they do not interrupt the wrapped
    operation because alert collection is only a supplementary activity.

    Args:
        threading_lock (threading.RLock): Lock used for synchronization of the
            threads in Prometheus calls
        alert_list (list): List to be populated with collected alerts. When not
            provided, a new list is created.
        interval (float): Number of seconds between Prometheus polls
        log_level (int): Level used for logging of each newly collected alert.
            Alerts are logged with DEBUG level by default because the summary
            of all collected alerts is logged when the context manager is left.
        status (dict): Dictionary to be populated with the status of the
            collection so that the consumer of collected alerts is able to
            distinguish a failed or unavailable collection from a successful
            collection where no alert was fired. Following keys are provided:
            "started" (bool) - collection thread was started,
            "successful_polls" (int) and "failed_polls" (int) - numbers of
            successful and failed Prometheus polls, "complete" (bool) -
            collection thread finished before the context manager was left and
            the collected alerts are complete.
        stop_timeout (float): Number of seconds to wait for the collection
            thread to terminate when the context manager is left. When the
            thread is still running after this timeout, the collection is
            recorded as incomplete.

    Yields:
        list: Alerts collected so far. The list is complete once the context
            manager is left.

    """
    alerts = alert_list if alert_list is not None else []
    collection_status = status if status is not None else {}
    collection_status.update(
        {"started": False, "successful_polls": 0, "failed_polls": 0, "complete": False}
    )
    try:
        subscriber = PrometheusAlertSubscriber(
            threading_lock=threading_lock,
            interval=interval,
            alert_list=alerts,
            log_level=log_level,
        )
        subscriber.subscribe()
        collection_status["started"] = True
    except Exception:
        logger.exception(
            "Prometheus alert collection could not be started. Alerts fired "
            "during the following operation will not be collected."
        )
        subscriber = None

    try:
        yield alerts
    finally:
        if subscriber is not None:
            subscriber.unsubscribe()
            # Wait for the collection thread to finish so that the alert list
            # is not modified while it is being processed by the caller.
            subscriber.join(timeout=stop_timeout)
            if subscriber.is_alive():
                logger.warning(
                    "Prometheus alert collection did not stop within "
                    f"{stop_timeout} seconds, collected alerts might be "
                    "incomplete"
                )
            else:
                collection_status["complete"] = True
            collection_status["successful_polls"] = subscriber.successful_polls
            collection_status["failed_polls"] = subscriber.failed_polls
            if not subscriber.successful_polls:
                logger.error(
                    "No Prometheus poll succeeded during the alert collection "
                    f"({subscriber.failed_polls} failed polls). Collected "
                    "alerts cannot be considered as complete."
                )
        logger.info(f"Collected {len(alerts)} alerts: {get_alert_names(alerts)}")


def get_alert_names(alerts):
    """
    Get sorted names of provided alerts.

    Args:
        alerts (list): List of alerts as returned by Prometheus API

    Returns:
        list: Sorted unique alert names

    """
    return sorted({alert.get("labels", {}).get("alertname") or "" for alert in alerts})


def get_unexpected_alerts(alerts, expected_alerts=None, ignored_severities=None):
    """
    Filter out expected alerts from the provided list of alerts.

    Args:
        alerts (list): List of alerts as returned by Prometheus API
        expected_alerts (list): Names of alerts that are not considered
            unexpected
        ignored_severities (list): Alerts with a severity label from this list
            are not considered unexpected

    Returns:
        list: Alerts that are neither expected nor ignored

    """
    expected_alerts = expected_alerts or []
    ignored_severities = ignored_severities or []
    unexpected_alerts = []
    for alert in alerts:
        labels = alert.get("labels", {})
        if labels.get("alertname") in expected_alerts:
            continue
        if labels.get("severity") in ignored_severities:
            continue
        unexpected_alerts.append(alert)
    return unexpected_alerts
