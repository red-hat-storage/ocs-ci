"""
A module for cluster load related functionalities

"""
import logging
import time
from datetime import datetime
from uuid import uuid4
import math

from range_key_dict import RangeKeyDict

from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import get_trim_mean
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.cluster import (
    get_osd_pods_memory_sum, get_percent_used_capacity
)
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


def wrap_msg(msg):
    """
    Wrap a log message with '=' marks.
    Necessary for making cluster load background logging distinguishable

    Args:
        msg (str): The log message to wrap

    Returns:
        str: The wrapped log message

    """
    marks = "=" * len(msg) if len(msg) < 150 else "=" * 150
    return f"\n{marks}\n{msg}\n{marks}"


class ClusterLoad:
    """
    A class for cluster load functionalities

    """

    def __init__(
        self, project_factory=None, pvc_factory=None, sa_factory=None,
        pod_factory=None, target_percentage=None
    ):
        """
        Initializer for ClusterLoad

        Args:
            project_factory (function): A call to project_factory function
            pvc_factory (function): A call to pvc_factory function
            sa_factory (function): A call to service_account_factory function
            pod_factory (function): A call to pod_factory function
            target_percentage (float): The percentage of cluster load that is
                required. The value should be greater than 0.1 and smaller than 0.95

        """
        self.prometheus_api = PrometheusAPI()
        self.pvc_factory = pvc_factory
        self.sa_factory = sa_factory
        self.pod_factory = pod_factory
        self.target_percentage = target_percentage
        self.cluster_limit = None
        self.dc_objs = list()
        self.pvc_objs = list()
        self.previous_iops = None
        self.current_iops = None
        self.rate = None
        self.pvc_size = None
        if not config.DEPLOYMENT['external_mode']:
            self.pvc_size = int(get_osd_pods_memory_sum() * 0.5)
        else:
            self.pvc_size = 10
        self.sleep_time = 45
        self.target_pods_number = None
        if project_factory:
            project_name = f"{defaults.BG_LOAD_NAMESPACE}-{uuid4().hex[:5]}"
            self.project = project_factory(project_name=project_name)

    def increase_load(self, rate, wait=True):
        """
        Create a PVC, a service account and a DeploymentConfig of FIO pod

        Args:
            rate (str): FIO 'rate' value (e.g. '20M')
            wait (bool): True for waiting for IO to kick in on the
                newly created pod, False otherwise

        """
        pvc_obj = self.pvc_factory(
            interface=constants.CEPHBLOCKPOOL, project=self.project,
            size=self.pvc_size, volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        self.pvc_objs.append(pvc_obj)
        service_account = self.sa_factory(pvc_obj.project)

        # Set new arguments with the updated file size to be used for
        # DeploymentConfig of FIO pod creation
        fio_dc_data = templating.load_yaml(constants.FIO_DC_YAML)
        args = fio_dc_data.get('spec').get('template').get(
            'spec'
        ).get('containers')[0].get('args')
        new_args = [
            x for x in args if not x.startswith('--filesize=') and not x.startswith('--rate=')
        ]
        io_file_size = f"{self.pvc_size * 1000 - 200}M"
        new_args.append(f"--filesize={io_file_size}")
        new_args.append(f"--rate={rate}")
        dc_obj = self.pod_factory(
            pvc=pvc_obj, pod_dict_path=constants.FIO_DC_YAML,
            raw_block_pv=True, deployment_config=True,
            service_account=service_account, command_args=new_args
        )
        self.dc_objs.append(dc_obj)
        if wait:
            logger.info(
                f"Waiting {self.sleep_time} seconds for IO to kick-in on the newly "
                f"created FIO pod {dc_obj.name}"
            )
            time.sleep(self.sleep_time)

    def decrease_load(self, wait=True):
        """
        Delete DeploymentConfig with its pods and the PVC. Then, wait for the
        IO to be stopped

        Args:
            wait (bool): True for waiting for IO to drop after the deletion
                of the FIO pod, False otherwise

        """
        dc_name = self.dc_objs[-1].name
        self.dc_objs[-1].delete()
        self.dc_objs[-1].ocp.wait_for_delete(dc_name)
        self.dc_objs.remove(self.dc_objs[-1])
        self.pvc_objs[-1].delete()
        self.pvc_objs[-1].ocp.wait_for_delete(self.pvc_objs[-1].name)
        self.pvc_objs.remove(self.pvc_objs[-1])
        if wait:
            logger.info(
                f"Waiting {self.sleep_time} seconds for IO to drop after "
                f"the deletion of {dc_name}"
            )
            time.sleep(self.sleep_time)

    def increase_load_and_print_data(self, rate, wait=True):
        """
        Increase load and print data

        Args:
            rate (str): FIO 'rate' value (e.g. '20M')
            wait (bool): True for waiting for IO to kick in on the
                newly created pod, False otherwise

        """
        self.increase_load(rate=rate, wait=wait)
        self.previous_iops = self.current_iops
        self.current_iops = self.calc_trim_metric_mean(metric=constants.IOPS_QUERY)
        msg = f"Current: {self.current_iops:.2f} || Previous: {self.previous_iops:.2f}"
        logger.info(f"IOPS:{wrap_msg(msg)}")
        self.print_metrics()

    def reach_cluster_load_percentage(self):
        """
        Reach the cluster limit and then drop to the given target percentage.
        The number of pods needed for the desired target percentage is determined by
        creating pods one by one, while examining the cluster latency. Once the latency
        is greater than 250 ms and it is growing exponentially, it means that
        the cluster limit has been reached.
        Then, dropping to the target percentage by deleting all pods and re-creating
        ones with smaller value of FIO 'rate' param.
        This leaves the number of pods needed running IO for cluster load to
        be around the desired percentage.

        """
        if not self.target_percentage:
            logger.warning("The target percentage was not provided. Breaking")
            return
        if not 0.1 < self.target_percentage < 0.95:
            logger.warning(
                f"The target percentage is {self.target_percentage * 100}% which is "
                "not within the accepted range. Therefore, IO will not be started"
            )
            return
        low_diff_counter = 0
        cluster_limit = None
        latency_vals = list()
        time_to_wait = 60 * 30
        time_before = time.time()

        self.current_iops = self.get_query(query=constants.IOPS_QUERY)

        # Creating FIO DeploymentConfig pods one by one, with a large value of FIO
        # 'rate' arg. This in order to determine the cluster limit faster.
        # Once determined, these pods will be deleted. Then, new FIO DC pods will be
        # created, with a smaller value of 'rate' param. This in order to be more
        # accurate with reaching the target percentage
        while True:
            wait = False if len(self.dc_objs) <= 1 else True
            self.increase_load_and_print_data(rate='250M', wait=wait)
            if self.current_iops > self.previous_iops:
                cluster_limit = self.current_iops

            latency = self.calc_trim_metric_mean(metric=constants.LATENCY_QUERY) * 1000
            latency_vals.append(latency)
            logger.info(f"Latency values: {latency_vals}")

            iops_diff = (self.current_iops / self.previous_iops * 100) - 100
            low_diff_counter += 1 if -15 < iops_diff < 10 else 0

            cluster_used_space = get_percent_used_capacity()

            if len(latency_vals) > 1 and latency > 250:
                # Checking for an exponential growth. In case the latest latency sample
                # value is more than 128 times the first latency value sample, we can conclude
                # that the cluster limit in terms of IOPS, has been reached.
                # See https://blog.docbert.org/vdbench-curve/ for more details.
                # In other cases, when the first latency sample value is greater than 3 ms,
                # the multiplication factor we check according to, is lower, in order to
                # determine the cluster load faster.
                if latency > latency_vals[0] * 2 ** 7 or (
                    3 < latency_vals[0] < 50 and len(latency_vals) > 5
                ):
                    logger.info(
                        wrap_msg("The cluster limit was determined by latency growth")
                    )
                    break

            # In case the latency is greater than 2 seconds,
            # most chances the limit has been reached
            elif latency > 2000:
                logger.info(
                    wrap_msg(f"The limit was determined by the high latency - {latency} ms")
                )
                break

            # For clusters that their nodes do not meet the minimum
            # resource requirements, the cluster limit is being reached
            # while the latency remains low. For that, the cluster limit
            # needs to be determined by the following condition of IOPS
            # diff between FIO pod creation iterations
            elif low_diff_counter > 3:
                logger.warning(
                    wrap_msg(
                        "Limit was determined by low IOPS diff between "
                        f"iterations - {iops_diff:.2f}%"
                    )
                )
                break

            elif time.time() > time_before + time_to_wait:
                logger.warning(
                    wrap_msg(
                        "Could not determine the cluster IOPS limit within"
                        f"the given {time_to_wait} seconds timeout. Breaking"
                    )
                )
                break

            elif cluster_used_space > 60:
                logger.warning(
                    wrap_msg(
                        f"Cluster used space is {cluster_used_space}%. Could "
                        "not reach the cluster IOPS limit before the "
                        "used spaced reached 60%. Breaking"
                    )
                )
                break

        self.cluster_limit = cluster_limit
        logger.info(wrap_msg(f"The cluster IOPS limit is {self.cluster_limit:.2f}"))
        logger.info("Deleting all DC FIO pods that have large FIO rate")
        while self.dc_objs:
            self.decrease_load(wait=False)

        target_iops = self.cluster_limit * self.target_percentage

        range_map = RangeKeyDict(
            {
                (0, 500): (6, 0.82, 0.4),
                (500, 1000): (8, 0.84, 0.45),
                (1000, 1500): (10, 0.86, 0.5),
                (1500, 2000): (12, 0.88, 0.55),
                (2000, 2500): (14, 0.90, 0.6),
                (2500, 3000): (16, 0.92, 0.65),
                (3000, 3500): (18, 0.94, 0.7),
                (3500, math.inf): (20, 0.96, 0.75),
            }
        )
        self.rate = f'{range_map[target_iops][0]}M'
        # Creating the first pod of small FIO 'rate' param, to speed up the process.
        # In the meantime, the load will drop, following the deletion of the
        # FIO pods with large FIO 'rate' param
        logger.info("Creating FIO pods, one by one, until the target percentage is reached")
        self.increase_load_and_print_data(rate=self.rate)
        msg = (
            f"The target load, in IOPS, is: {target_iops}, which is "
            f"{self.target_percentage*100}% of the {self.cluster_limit} cluster limit"
        )
        logger.info(wrap_msg(msg))

        while self.current_iops < target_iops * range_map[target_iops][1]:
            wait = False if self.current_iops < target_iops * range_map[target_iops][2] else True
            self.increase_load_and_print_data(rate=self.rate, wait=wait)

        msg = f"The target load, of {self.target_percentage * 100}%, has been reached"
        logger.info(wrap_msg(msg))
        self.target_pods_number = len(self.dc_objs)

    def get_query(self, query, mute_logs=False):
        """
        Get query from Prometheus and parse it

        Args:
            query (str): Query to be done
            mute_logs (bool): True for muting the logs, False otherwise

        Returns:
            float: the query result

        """
        now = datetime.now
        timestamp = datetime.timestamp
        return float(
            self.prometheus_api.query(
                query, str(timestamp(now())), mute_logs=mute_logs
            )[0]['value'][1]
        )

    def calc_trim_metric_mean(self, metric, samples=5, mute_logs=False):
        """
        Get the trimmed mean of a given metric

        Args:
            metric (str): The metric to calculate the average result for
            samples (int): The number of samples to take
            mute_logs (bool): True for muting the logs, False otherwise

        Returns:
            float: The average result for the metric

        """
        vals = list()
        for i in range(samples):
            vals.append(round(self.get_query(metric, mute_logs), 5))
            if i == samples - 1:
                break
            time.sleep(5)
        return round(get_trim_mean(vals), 5)

    def print_metrics(self, mute_logs=False):
        """
        Print metrics

        Args:
            mute_logs (bool): True for muting the Prometheus logs, False otherwise

        """
        high_latency = 200
        metrics = {
            "throughput": self.get_query(constants.THROUGHPUT_QUERY, mute_logs=mute_logs) * (
                constants.TP_CONVERSION.get(' B/s')
            ),
            "latency": self.get_query(constants.LATENCY_QUERY, mute_logs=mute_logs) * 1000,
            "iops": self.get_query(constants.IOPS_QUERY, mute_logs=mute_logs),
            "used_space": self.get_query(constants.USED_SPACE_QUERY, mute_logs=mute_logs) / 1e+9
        }
        limit_msg = (
            f" ({metrics.get('iops') / self.cluster_limit * 100:.2f}% of the "
            f"{self.cluster_limit:.2f} limit)"
        ) if self.cluster_limit else ""
        pods_msg = f" || Number of FIO pods: {len(self.dc_objs)}" if self.dc_objs else ""
        msg = (
            f"Throughput: {metrics.get('throughput'):.2f} MB/s || "
            f"Latency: {metrics.get('latency'):.2f} ms || "
            f"IOPS: {metrics.get('iops'):.2f}{limit_msg} || "
            f"Used Space: {metrics.get('used_space'):.2f} GB{pods_msg}"
        )
        logger.info(f"Cluster utilization:{wrap_msg(msg)}")
        if metrics.get('latency') > high_latency:
            logger.warning(f"Cluster latency is higher than {high_latency} ms!")

    def adjust_load_if_needed(self):
        """
        Dynamically adjust the IO load based on the cluster latency.
        In case the latency goes beyond 250 ms, start deleting FIO pods.
        Once latency drops back below 100 ms, re-create the FIO pods
        to make sure that cluster load is around the target percentage

        """
        latency = self.calc_trim_metric_mean(
            constants.LATENCY_QUERY, mute_logs=True
        )
        if latency > 0.25 and len(self.dc_objs) > 0:
            msg = (
                f"Latency is too high - {latency * 1000:.2f} ms."
                " Dropping the background load. Once the latency drops back to "
                "normal, the background load will be increased back"
            )
            logger.warning(wrap_msg(msg))
            self.decrease_load(wait=False)
        if latency < 0.1 and self.target_pods_number > len(self.dc_objs):
            msg = (
                f"Latency is back to normal - {latency * 1000:.2f} ms. "
                f"Increasing back the load"
            )
            logger.info(wrap_msg(msg))
            self.increase_load(rate=self.rate, wait=False)

    def reduce_load(self, pause=True):
        """
        Pause the cluster load

        """
        pods_to_keep = 0 if pause else int(len(self.dc_objs) / 2)
        logger.info(wrap_msg(f"{'Pausing' if pods_to_keep == 0 else 'Reducing'} the cluster load"))
        while len(self.dc_objs) > pods_to_keep:
            self.decrease_load(wait=False)

    def resume_load(self):
        """
        Resume the cluster load

        """
        logger.info(wrap_msg("Resuming the cluster load"))
        while len(self.dc_objs) < self.target_pods_number:
            self.increase_load(rate=self.rate, wait=False)
