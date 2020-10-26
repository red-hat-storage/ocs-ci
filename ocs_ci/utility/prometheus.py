import base64
import json
import logging
import os
import re
import requests
import tempfile
import time
import yaml
from datetime import datetime

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(name=__file__)


# TODO(fbalak): if ignore_more_occurences is set to False then tests are flaky.
# The root cause should be inspected.
def check_alert_list(
    label,
    msg,
    alerts,
    states,
    severity="warning",
    ignore_more_occurences=True
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
    """

    target_alerts = [
        alert
        for alert
        in alerts
        if alert.get('labels').get('alertname') == label
    ]

    logger.info(f"Checking properties of found {label} alerts")
    if ignore_more_occurences:
        for state in states:
            delete = False
            for key, alert in reversed(list(enumerate(target_alerts))):
                if alert.get('state') == state:
                    if delete:
                        d_msg = f"Ignoring {alert} as alert already appeared."
                        logger.debug(d_msg)
                        target_alerts.pop(key)
                    else:
                        delete = True
    assert_msg = (
        f"Incorrect number of {label} alerts ({len(target_alerts)} "
        f"instead of {len(states)} with states: {states})."
        f"\nAlerts: {target_alerts}"
    )
    assert len(target_alerts) == len(states), assert_msg

    for key, state in enumerate(states):

        assert_msg = 'Alert message for alert {label} is not correct'
        assert target_alerts[key]['annotations']['message'] == msg, assert_msg

        assert_msg = f"Alert {label} doesn't have {severity} severity"
        assert target_alerts[key]['annotations']['severity_level'] == severity, assert_msg

        assert_msg = f"Alert {label} is not in {state} state"
        assert target_alerts[key]['state'] == state, assert_msg

    logger.info("Alerts were triggered correctly during utilization")


def check_query_range_result_viafunction(
    result,
    is_value_good,
    is_value_bad=lambda val: False,
    exp_metric_num=None,
    exp_delay=None,
    exp_good_time=None,
    is_float=False
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
            f"actual number data series is {len(result)}")
        logger.error(msg)
        is_result_ok = False

    for metric in result:
        name = metric['metric']['__name__']
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
                    logger.info(
                        msg + f" but within expected {exp_delay}s delay")
                elif (exp_good_time is not None
                        and delta.seconds >= exp_good_time):
                    logger.info(
                        msg + f" but after {exp_good_time}s already passed")
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
    is_value_bad = lambda val: val in bad_values    # noqa: E731
    is_result_ok = check_query_range_result_viafunction(
        result,
        is_value_good,
        is_value_bad,
        exp_metric_num,
        exp_delay,
        exp_good_time,
        is_float=False
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
    is_value_bad = lambda val: False                         # noqa: E731
    is_result_ok = check_query_range_result_viafunction(
        result,
        is_value_good,
        is_value_bad,
        exp_metric_num,
        exp_delay,
        exp_good_time,
        is_float=True
    )
    return is_result_ok


def get_api_token():
    """
    Get API token used for communication with Prometheus API.

    Returns:
        str: API token

    """
    if config.ENV_DATA['platform'].lower() == 'ibm_cloud':
        logger.info('Get API access token from IBM cloud')
        apikey = config.AUTH.get('ibm_apikey')
        cluster_name = config.ENV_DATA['cluster_name']
        cluster_id = config.ENV_DATA['ibm_id']
        exec_cmd([
            'ibmcloud', 'login', '--apikey', apikey,
            '-c', cluster_id, '-r'
        ])
        logger.info('Get IBM token endpoint')
        ibm_cluster_info_cmd = ['ibmcloud', 'oc', 'cluster', 'get', '-c', cluster_name, '--json']
        ibm_cluster_info = exec_cmd(ibm_cluster_info_cmd).stdout.decode()
        ibm_cluster_info = json.loads(ibm_cluster_info)
        authorization_endpoint = requests.get(
            f"{ibm_cluster_info['masterURL']}/.well-known/oauth-authorization-server"
        ).json()['authorization_endpoint']
        logger.info(f"IBM Auth endpoint: {authorization_endpoint}")
        # How to get access token:
        # https://tools.ietf.org/html/rfc6749#section-4.2.2
        token_source = requests.get(
            f"{authorization_endpoint}?client_id=openshift-challenging-client&response_type=token",
            auth=('apikey', apikey),
            headers={"X-CSRF-Token": "a"},
            allow_redirects=False
        )
        token_source = token_source.headers['Location']
        # regular expression to get access token
        token = re.findall('(?<=access_token=)([^&]*)(?=&)?', token_source)[0]

    else:
        logger.info('Login as kubeadmin and get API access token')
        password_file = os.path.join(
            config.ENV_DATA['cluster_path'],
            config.RUN['password_location']
        )
        with open(password_file) as f:
            password = f.read().rstrip('\n')
        kubeconfig = os.getenv('KUBECONFIG')
        kube_data = ""
        with open(kubeconfig, 'r') as kube_file:
            kube_data = kube_file.readlines()
        ocp = OCP(
            kind=constants.ROUTE,
            namespace=defaults.OCS_MONITORING_NAMESPACE
        )
        assert ocp.login('kubeadmin', password), 'Login to OCP failed'
        token = ocp.get_user_token()
        with open(kubeconfig, 'w') as kube_file:
            kube_file.writelines(kube_data)
    return token


class PrometheusAPI(object):
    """
    This is wrapper class for Prometheus API. It requires access token for
    API communication.

    """

    _token = None
    _endpoint = None
    _cacert = False

    def __init__(self, token):
        """
        Constructor for PrometheusAPI class.

        Args:
            token (str): API token used for Prometheus communication

        """
        self._token = token
        # TODO: generate certificate for IBM cloud platform
        if not config.ENV_DATA['platform'].lower() == 'ibm_cloud':
            self.generate_cert()

        ocp = OCP(
            kind=constants.ROUTE,
            namespace=defaults.OCS_MONITORING_NAMESPACE
        )
        route_obj = ocp.get(
            resource_name=defaults.PROMETHEUS_ROUTE
        )
        self._endpoint = f"https://{route_obj['spec']['host']}"

    def generate_cert(self):
        """
        Generate CA certificate from kubeconfig for API.

        TODO: find proper way how to generate/load cert files.

        """
        kubeconfig_path = os.path.join(
            config.ENV_DATA['cluster_path'],
            config.RUN['kubeconfig_location']
        )
        with open(kubeconfig_path, "r") as f:
            kubeconfig = yaml.load(f, yaml.Loader)
        cert_file = tempfile.NamedTemporaryFile(delete=False)
        cert_file.write(
            base64.b64decode(
                kubeconfig['clusters'][0]['cluster']['certificate-authority-data']
            )
        )
        cert_file.close()
        self._cacert = cert_file.name
        logger.info(f"Generated CA certification file: {self._cacert}")

    def get(self, resource, payload=None):
        """
        Get alerts from Prometheus API.

        Args:
            resource (str): Represents part of uri that specifies given
                resource
            payload (dict): Provide parameters to GET API call.
                e.g. for `alerts` resource this can be
                {'silenced': False, 'inhibited': False}

        Returns:
            dict: Response from Prometheus alerts api

        """
        pattern = f"/api/v1/{resource}"
        headers = {'Authorization': f"Bearer {self._token}"}

        logger.debug(f"GET {self._endpoint + pattern}")
        logger.debug(f"headers={headers}")
        logger.debug(f"verify={self._cacert}")
        logger.debug(f"params={payload}")

        response = requests.get(
            self._endpoint + pattern,
            headers=headers,
            verify=self._cacert,
            params=payload
        )
        return response

    def query(self, query, timestamp=None, timeout=None, validate=True, mute_logs=False):
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

        Returns:
            list: Result of the query (value(s) for a single timestamp)

        .. _`instant query`: https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries

        """
        query_payload = {'query': query}
        log_msg = f"Performing prometheus instant query '{query}'"
        if timestamp is not None:
            query_payload['time'] = timestamp
            log_msg += f" for timestamp {timestamp}"
        if timeout is not None:
            query_payload['timeout'] = timeout
        # Log human readable summary of the query
        if not mute_logs:
            logger.info(log_msg)
        resp = self.get('query', payload=query_payload)
        content = yaml.safe_load(resp.content)
        if validate:
            # If this fails, Prometheus instance or a query is so broken that
            # test can't be performed. Note that prometheus reports "success"
            # even for queryies which returns nothing.
            assert content["status"] == "success"
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
        query_payload = {
            'query': query,
            'start': start,
            'end': end,
            'step': step}
        if timeout is not None:
            query_payload['timeout'] = timeout
        # Human readable summary of the query (details are logged by get
        # method itself with debug level).
        logger.info((
            f"Performing prometheus range query '{query}' "
            f"over a time range ({start}, {end})"))
        resp = self.get('query_range', payload=query_payload)
        content = yaml.safe_load(resp.content)
        if validate:
            # If this fails, Prometheus instance is so broken that test can't
            # be performed.
            assert content["status"] == "success"
            # For a range query, we should always get a matrix result type, as
            # noted in Prometheus documentation, see:
            # https://prometheus.io/docs/prometheus/latest/querying/api/#range-vectors
            assert content["data"]["resultType"] == "matrix"
            # All metric sample series has the same size.
            sizes = []
            for metric in content["data"]["result"]:
                sizes.append(len(metric["values"]))
            msg = "Metric sample series doesn't have the same size."
            assert all(size == sizes[0] for size in sizes), msg
            # Check that we don't have holes in the response. If this fails,
            # our Prometheus instance is missing some part of the data we are
            # asking it about. For positive test cases, this is most likely a
            # test blocker product bug.
            start_dt = datetime.utcfromtimestamp(start)
            end_dt = datetime.utcfromtimestamp(end)
            duration = end_dt - start_dt
            exp_samples = duration.seconds / step
            assert exp_samples - 1 <= sizes[0] <= exp_samples + 1
        # return actual result of the query
        return content["data"]["result"]

    def wait_for_alert(self, name, state=None, timeout=1200, sleep=5):
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

        Returns:
            list: List of alert records

        """
        while timeout > 0:
            alerts_response = self.get(
                'alerts',
                payload={
                    'silenced': False,
                    'inhibited': False,
                }
            )
            msg = f"Request {alerts_response.request.url} failed"
            assert alerts_response.ok, msg
            if state:
                alerts = [
                    alert
                    for alert
                    in alerts_response.json().get('data').get('alerts')
                    if alert.get('labels').get('alertname') == name
                    and alert.get('state') == state
                ]
                logger.info(f"Checking for {name} alerts with state {state}... "
                            f"{len(alerts)} found")
                if len(alerts) > 0:
                    break
            else:
                # search for missing alerts, search is completed when
                # there are no alerts with given name
                alerts = [
                    alert
                    for alert
                    in alerts_response.json().get('data').get('alerts')
                    if alert.get('labels').get('alertname') == name
                ]
                logger.info(f"Checking for {name} alerts. There should be no alerts ... "
                            f"{len(alerts)} found")
                if len(alerts) == 0:
                    break
            time.sleep(sleep)
            timeout -= sleep
        return alerts

    def check_alert_cleared(self, label, measure_end_time, time_min=120):
        """
        Check that all alerts with provided label are cleared.

        Args:
            label (str): Alerts label
            measure_end_time (int): Timestamp of measurement end
            time_min (int): Number of seconds to wait for alert to be cleared
                since measurement end

        """
        time_actual = time.time()
        time_wait = int(
            (measure_end_time + time_min) - time_actual
        )
        if time_wait > 0:
            logger.info(f"Waiting for approximately {time_wait} seconds for alerts "
                        f"to be cleared ({time_min} seconds since measurement end)")
        else:
            time_wait = 1
        cleared_alerts = self.wait_for_alert(
            name=label,
            state=None,
            timeout=time_wait
        )
        logger.info(f"Cleared alerts: {cleared_alerts}")
        assert len(cleared_alerts) == 0, f"{label} alerts were not cleared"
        logger.info(f"{label} alerts were cleared")
