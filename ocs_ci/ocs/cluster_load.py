"""
A module for cluster load related functionalities

"""
import logging
import time
from datetime import datetime
from uuid import uuid4

from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.utils import get_trim_mean
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.cluster import (
    get_osd_pods_memory_sum, get_percent_used_capacity
)


logger = logging.getLogger(__name__)


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
            pvc_factory (function): A call to pvc_factory function
            sa_factory (function): A call to service_account_factory function
            pod_factory (function): A call to pod_factory function
            target_percentage (float): The percentage of cluster load that is
                required. The value should be greater than 0 and smaller than 1

        """
        self.prometheus_api = PrometheusAPI()
        self.pvc_factory = pvc_factory
        self.sa_factory = sa_factory
        self.pod_factory = pod_factory
        self.target_percentage = target_percentage
        self.cluster_limit = None
        self.dc_objs = list()
        self.pvc_objs = list()
        self.pvc_size = int(get_osd_pods_memory_sum() * 0.5)
        self.io_file_size = f"{self.pvc_size * 1000 - 200}M"
        self.sleep_time = 35
        if project_factory:
            project_name = f"{defaults.BG_LOAD_NAMESPACE}-{uuid4().hex[:5]}"
            self.project = project_factory(project_name=project_name)

    def increase_load(self, rate=None, wait=True):
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
        new_args.append(f"--filesize={self.io_file_size}")
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
                f"Waiting {self.sleep_time} seconds for IO to drop after the deletion of {dc_name}"
            )
            time.sleep(self.sleep_time)

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
            logger.warning(f"The target percentage was not provided. Breaking")
            return
        if not 0.1 < self.target_percentage < 0.95:
            logger.warning(
                f"The target percentage is {self.target_percentage * 100}% which is "
                f"not within the accepted range. Therefore, IO will not be started"
            )
            return
        low_diff_counter = 0
        limit_reached = False
        cluster_limit = None
        latency_vals = list()
        time_to_wait = 60 * 30
        time_before = time.time()

        current_iops = self.get_query(query=constants.IOPS_QUERY)

        msg = (
            "\n======================\nCurrent IOPS: {:.2f}"
            "\nPrevious IOPS: {:.2f}\n======================"
        )

        # Creating FIO DeploymentConfig pods one by one, with a large value of FIO
        # 'rate' arg. This in order to determine the cluster limit faster.
        # Once determined, these pods will be deleted. Then, new FIO DC pods will be
        # created, with a smaller value of 'rate' param. This in order to be more
        # accurate with reaching the target percentage
        rate = '250M'
        while not limit_reached:
            self.increase_load(rate=rate)
            previous_iops = current_iops
            current_iops = self.get_query(query=constants.IOPS_QUERY)
            if current_iops > previous_iops:
                cluster_limit = current_iops

            logger.info(msg.format(current_iops, previous_iops, len(self.dc_objs)))
            self.print_metrics()

            latency = self.calc_trim_metric_mean(metric=constants.LATENCY_QUERY) * 1000
            latency_vals.append(latency)
            logger.info(f"Latency values: {latency_vals}")

            if len(latency_vals) > 1 and latency > 250:
                # Checking for an exponential growth
                if latency > latency_vals[0] * 2 ** 7:
                    logger.info("Latency exponential growth was detected")
                    limit_reached = True

            # In case the latency is greater than 3 seconds,
            # most chances the limit has been reached
            if latency > 3000:
                logger.info(
                    f"Limit was determined by latency, which is "
                    f"higher than 3 seconds - {latency} ms"
                )
                limit_reached = True

            # For clusters that their nodes do not meet the minimum
            # resource requirements, the cluster limit is being reached
            # while the latency remains low. For that, the cluster limit
            # needs to be determined by the following condition of IOPS
            # diff between FIO pod creation iterations
            iops_diff = (current_iops / previous_iops * 100) - 100
            low_diff_counter += 1 if -15 < iops_diff < 10 else 0
            if low_diff_counter > 3:
                logger.warning(
                    f"Limit was determined by low IOPS diff between "
                    f"iterations - {iops_diff:.2f}%"
                )
                limit_reached = True

            if time.time() > time_before + time_to_wait:
                logger.warning(
                    f"Could not determine the cluster IOPS limit within"
                    f"\nthe given {time_to_wait} seconds timeout. Breaking"
                )
                limit_reached = True

            cluster_used_space = get_percent_used_capacity()
            if cluster_used_space > 60:
                logger.warning(
                    f"Cluster used space is {cluster_used_space}%. Could "
                    f"not reach the cluster IOPS limit before the "
                    f"used spaced reached 60%. Breaking"
                )
                limit_reached = True

        self.cluster_limit = cluster_limit
        logger.info(
            f"\n===================================\nThe cluster IOPS limit "
            f"is {self.cluster_limit:.2f}\n==================================="
        )
        logger.info(
            f"Deleting all DC FIO pods that have FIO rate parameter of {rate}"
        )
        while self.dc_objs:
            self.decrease_load(wait=False)

        # Creating the first pod of small FIO 'rate' param, to speed up the process.
        # In the meantime, the load will drop, following the deletion of the
        # FIO pods with large FIO 'rate' param
        rate = '15M'
        logger.info(
            f"Creating FIO pods with a rate parameter of {rate}, one by "
            f"one, until the target percentage is reached"
        )
        self.increase_load(rate=rate)
        target_iops = self.cluster_limit * self.target_percentage
        current_iops = self.get_query(query=constants.IOPS_QUERY)
        logger.info(f"Target IOPS: {target_iops}")
        logger.info(f"Current IOPS: {current_iops}")

        while current_iops < target_iops * 0.95:
            wait = False if current_iops < target_iops / 2 else True
            self.increase_load(rate=rate, wait=wait)
            previous_iops = current_iops
            current_iops = self.get_query(query=constants.IOPS_QUERY)
            logger.info(msg.format(current_iops, previous_iops, len(self.dc_objs)))
            self.print_metrics()

        logger.info(
            f"\n========================================\n"
            f"The target load, of {self.target_percentage * 100}%, has been reached"
            f"\n=========================================="
        )

    def get_query(self, query):
        """
        Get query from Prometheus and parse it

        Args:
            query (str): Query to be done

        Returns:
            float: the query result

        """
        now = datetime.now
        timestamp = datetime.timestamp
        return float(
            self.prometheus_api.query(query, str(timestamp(now())))[0]['value'][1]
        )

    def calc_trim_metric_mean(self, metric=constants.LATENCY_QUERY, samples=5):
        """
        Get the trimmed mean of a given metric

        Args:
            metric (str): The metric to calculate the average result for
            samples (int): The number of samples to take

        Returns:
            float: The average result for the metric

        """
        vals = list()
        for i in range(samples):
            vals.append(round(self.get_query(metric), 5))
            if i == samples - 1:
                break
            time.sleep(5)
        return round(get_trim_mean(vals), 5)

    def get_metrics(self):
        """
        Get different cluster load and utilization metrics
        """
        return {
            "throughput": self.get_query(constants.THROUGHPUT_QUERY) * (
                constants.TP_CONVERSION.get(' B/s')
            ),
            "latency": self.get_query(constants.LATENCY_QUERY) * 1000,
            "iops": self.get_query(constants.IOPS_QUERY),
            "used_space": self.get_query(constants.USED_SPACE_QUERY) / 1e+9
        }

    def print_metrics(self):
        """
        Print metrics

        """
        high_latency = 500
        metrics = self.get_metrics()
        limit_msg = ""
        pods_msg = ""
        if self.cluster_limit:
            limit_msg = (
                f"({metrics.get('iops') / self.cluster_limit * 100:.2f}% of the "
                f"{self.cluster_limit:.1f} limit)\n"
            )
        if self.dc_objs:
            pods_msg = (
                f"\nNumber of pods running FIO: {len(self.dc_objs)}"
            )
        logger.info(
            f"\n===============================\n"
            f"Cluster throughput: {metrics.get('throughput'):.2f} MB/s\n"
            f"Cluster latency: {metrics.get('latency'):.2f} ms\n"
            f"Cluster IOPS: {metrics.get('iops'):.2f}\n{limit_msg}"
            f"Cluster used space: {metrics.get('used_space'):.2f} GB{pods_msg}"
            f"\n==============================="
        )
        if metrics.get('latency') > high_latency:
            logger.warning(f"Cluster latency is higher than {high_latency} ms!")
