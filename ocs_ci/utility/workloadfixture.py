# -*- coding: utf8 -*-

"""
This module contains functions implementing functionality of workload
fixtures in ocs-ci.

.. moduleauthor:: Filip Balák
"""


import json
import logging
import os
import threading
import time

from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


def measure_operation(
    operation,
    result_file,
    minimal_time=None,
    metadata=None,
    measure_after=False
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
    def prometheus_log(info, alert_list):
        """
        Log all alerts from Prometheus API every 3 seconds.

        Args:
            info (dict): Contains run key attribute that controls thread.
                If `info['run'] == False` then thread will stop
            alert_list (list): List to be populated with alerts

        """
        prometheus = PrometheusAPI()
        logger.info('Logging of all prometheus alerts started')
        while info.get('run'):
            alerts_response = prometheus.get(
                'alerts',
                payload={
                    'silenced': False,
                    'inhibited': False
                }
            )
            msg = f"Request {alerts_response.request.url} failed"
            assert alerts_response.ok, msg
            for alert in alerts_response.json().get('data').get('alerts'):
                if alert not in alert_list:
                    logger.info(f"Adding {alert} to alert list")
                    alert_list.append(alert)
            time.sleep(3)
        logger.info('Logging of all prometheus alerts stopped')

    # check if file with results for this operation already exists
    # if it exists then use it
    if os.path.isfile(result_file) and os.access(result_file, os.R_OK):
        logger.info(
            f"File {result_file} already created."
            f" Trying to use it for tests..."
        )
        with open(result_file) as open_file:
            results = json.load(open_file)
            # indicate that we are not going to execute the workload, but
            # just reuse measurement from earlier run
            results['first_run'] = False
        logger.info(
            f"File {result_file} loaded. Content of file:\n{results}"
        )

    # if there is no file with results from previous run
    # then perform operation measurement
    else:
        logger.info(
            f"File {result_file} not created yet. Starting measurement..."
        )
        if not measure_after:
            start_time = time.time()

        # init logging thread that checks for Prometheus alerts
        # while workload is running
        # based on https://docs.python.org/3/howto/logging-cookbook.html#logging-from-multiple-threads
        info = {'run': True}
        alert_list = []

        logging_thread = threading.Thread(
            target=prometheus_log,
            args=(info, alert_list)
        )
        logging_thread.start()

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
            raise(ex)
        finally:
            if measure_after:
                start_time = time.time()
            passed_time = time.time() - start_time
            if minimal_time:
                additional_time = minimal_time - passed_time
                if additional_time > 0:
                    logger.info(f"Starting {additional_time}s sleep for the purposes of measurement.")
                    time.sleep(additional_time)
            # Dumping measurement results into result file.
            stop_time = time.time()
            info['run'] = False
            logging_thread.join()
            results = {
                'start': start_time,
                'stop': stop_time,
                'result': result,
                'metadata': metadata,
                'prometheus_alerts': alert_list,
                'first_run': True,
            }
            logger.info(f"Results of measurement: {results}")
            with open(result_file, 'w') as outfile:
                logger.info(f"Dumping results of measurement into {result_file}")
                json.dump(results, outfile)
    return results
