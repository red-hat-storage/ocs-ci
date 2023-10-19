# -*- coding: utf8 -*-

"""
This module contains functions implementing functionality of workload
fixtures in ocs-ci.

.. moduleauthor:: Filip Balák
"""


import json
import logging
import os
import time

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.pagerduty import PagerDutyAPI
from ocs_ci.utility.prometheus import PrometheusAlertSubscriber

logger = logging.getLogger(__name__)


def is_measurement_done(result_file):
    """
    Has the measurement been already performed and stored in a result file?

    Returns:
      bool: True if the measurement has been already performed.
    """
    if os.path.isfile(result_file) and os.access(result_file, os.R_OK):
        logger.info("Measurements file %s is already created.", result_file)
        return True
    return False


def measure_operation(
    operation,
    result_file,
    minimal_time=None,
    metadata=None,
    measure_after=False,
    pagerduty_service_ids=None,
    threading_lock=None,
):
    """
    Get dictionary with keys 'start', 'stop', 'metadata' and 'result' that
    contain information about start and stop time of given function and its
    result.

    Args:
        operation (function): Function to be performed
        result_file (str): File name that should contain measurement results
            including logs in json format. If this file exists then it is
            used for test.
        minimal_time (int): Minimal number of seconds to monitor a system.
            If provided then monitoring of system continues even when
            operation is finshed. If not specified then measurement is finished
            when operation is complete
        metadata (dict): This can contain dictionary object with information
            relevant to test (e.g. volume name, operating host, ...)
        measure_after (bool): Determine if time measurement is done before or
            after the operation returns its state. This can be useful e.g.
            for capacity utilization testing where operation fills capacity
            and utilized data are measured after the utilization is completed
        pagerduty_service_ids (list): Service IDs from PagerDuty system used
            incidents query
        threading_lock (threading.RLock): Lock used for synchronization of the threads in Prometheus calls

    Returns:
        dict: contains information about `start` and `stop` time of given
            function and its `result` and provided `metadata`
            Example::

                {
                    'start': 1569827653.1903834,
                    'stop': 1569828313.6469617,
                    'result': 'rook-ceph-osd-2',
                    'metadata': {'status': 'success'},
                    'prometheus_alerts': [{'labels': ...}, {...}, ...]
                }

    """

    # check if file with results for this operation already exists
    # if it exists then use it
    if is_measurement_done(result_file):
        with open(result_file) as open_file:
            results = json.load(open_file)
            # indicate that we are not going to execute the workload, but
            # just reuse measurement from earlier run
            results["first_run"] = False
        logger.info("Measurement file %s loaded.", result_file)
        logger.debug("Content of measurement file:\n%s", results)

    # if there is no file with results from previous run
    # then perform operation measurement
    else:
        if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
            logger.info("Starting PagerDuty periodical update of pagerduty secret")
            config.RUN["thread_pagerduty_secret_update"] = "required"
        logger.info(f"File {result_file} not created yet. Starting measurement...")
        if not measure_after:
            start_time = time.time()

        # init logging thread that checks for Prometheus alerts
        alert_subscriber = PrometheusAlertSubscriber(
            threading_lock=threading_lock, interval=3
        )
        alert_subscriber.subscribe()

        try:
            result = operation()
        except Exception as ex:
            # When the operation (which is being measured) fails, we need to
            # make sure that alert harvesting thread ends and (at least)
            # alerting data are saved into measurement dump file.
            result = None
            logger.error("exception raised during measured operation: %s", ex)
            # Additional waiting for the measurement purposes is no longer
            # necessary, and would only confuse anyone observing the failure.
            minimal_time = 0
            # And make sure the exception is properly processed by pytest (it
            # would make the fixture fail).
            raise (ex)
        finally:
            if measure_after:
                start_time = time.time()
            passed_time = time.time() - start_time
            if minimal_time:
                additional_time = minimal_time - passed_time
                if additional_time > 0:
                    logger.info(
                        f"Starting {additional_time}s sleep for the purposes of measurement."
                    )
                    time.sleep(additional_time)
            # Dumping measurement results into result file.
            stop_time = time.time()

            alert_subscriber.unsubscribe()
            prometheus_alert_list = alert_subscriber.get_alerts()

            results = {
                "start": start_time,
                "stop": stop_time,
                "result": result,
                "metadata": metadata,
                "prometheus_alerts": prometheus_alert_list,
                "first_run": True,
            }
            if (
                config.ENV_DATA["platform"].lower()
                in constants.MANAGED_SERVICE_PLATFORMS
            ):
                # During testing of ODF Managed Service are also collected alerts
                # in PagerDuty, Sendgrid and Dead Man's Snith systems
                pagerduty = PagerDutyAPI()
                logger.info("Logging all PagerDuty incidents")
                incidents_response = pagerduty.get(
                    "incidents",
                    payload={
                        "service_ids[]": pagerduty_service_ids,
                        "since": time.strftime(
                            "%Y-%m-%d %H:%M:%S", time.gmtime(start_time)
                        ),
                        "time_zone": "UTC",
                    },
                )
                incidents_response.raise_for_status()
                pagerduty_incidents = incidents_response.json().get("incidents")
                results["pagerduty_incidents"] = pagerduty_incidents
                logger.info("Stopping PagerDuty periodical update of pagerduty secret")
                config.RUN["thread_pagerduty_secret_update"] = "required"
            logger.info(f"Results of measurement: {results}")
            with open(result_file, "w") as outfile:
                logger.info(f"Dumping results of measurement into {result_file}")
                json.dump(results, outfile)
    return results
