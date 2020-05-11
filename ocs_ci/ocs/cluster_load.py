"""
A module for cluster load related functionalities

"""
import logging
import time

import ocs_ci.ocs.constants as constant
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import (
    CephCluster, get_osd_pods_memory_sum, get_percent_used_capacity
)


class ClusterLoad:
    """
    A class for cluster load functionalities

    """

    def __init__(self, propagate_logs=True):
        """
        Initializer for ClusterLoad

        Args:
            propagate_logs (bool): True for logging, False otherwise

        """
        self.logger = logging.getLogger(__name__)
        self.logger.propagate = propagate_logs
        self.cl_obj = CephCluster()

    def reach_cluster_load_percentage_in_throughput(
        self, pvc_factory, pod_factory, target_percentage=0.3, cluster_limit=None
    ):
        """
        Reach the cluster throughput limit and then drop to the requested target percentage.
        The number of pods needed for the desired target percentage is determined by
        creating pods one by one, while examining if the cluster throughput is increased
        by more than 10%. When it doesn't increased by more than 10% anymore after
        the new pod started running IO, it means that the cluster throughput limit is
        reached. Then, the function deletes the pods that are not needed as they
        are the difference between the limit (100%) and the target percentage
        (the default target percentage is 30%). This leaves the number of pods needed
        running IO for cluster throughput to be around the desired percentage.

        Args:
            pvc_factory (function): A call to pvc_factory function
            pod_factory (function): A call to pod_factory function
            target_percentage (float): The percentage of cluster load that is required.
                The value should be greater than 0 and smaller than 1
            cluster_limit (float): The cluster pre-known throughput limit in Mb/s.
                If passed, the function will calculate the target throughput based on it
                multiplied by 'target_percentage'

        Returns:
            tuple: The cluster limit in Mb/s (float) and the current throughput
                in Mb/s (float)

        """
        pvc_objs = list()
        pod_objs = list()
        limit_reached = False

        # FIO params:
        # 'runtime' is set with a large value of seconds to
        # make sure that the pods are running
        io_run_time = 100**3
        rate = '200M'
        bs = '256K'

        if 0.1 < target_percentage > 0.95:
            self.logger.warning(
                f"The target percentage is {target_percentage * 100}% which is not "
                f"within the accepted range. Therefore, IO will not be started"
            )
            return

        target_throughput = None
        if cluster_limit:
            target_throughput = cluster_limit * target_percentage

        pvc_size = int(get_osd_pods_memory_sum() * 0.75)

        self.logger.info(
            f"In order to eliminate the OSD pods cache effect, creating 1 pod with a "
            f"PVC size of {pvc_size} GB, which equals to 0.75 of the memory sum of "
            f"all the OSD pods. Then, starting IOs on it"
        )
        pvc_obj = pvc_factory(
            interface=constant.CEPHBLOCKPOOL, size=pvc_size,
            volume_mode=constants.VOLUME_MODE_BLOCK
        )
        pvc_objs.append(pvc_obj)
        pod_obj = pod_factory(
            pvc=pvc_obj, pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
            raw_block_pv=True
        )
        pod_objs.append(pod_obj)

        io_file_size = f"{pvc_size-1}G"

        pod_obj.run_io(
            storage_type='block', size=io_file_size,
            runtime=io_run_time, rate=rate, bs=bs, rw_ratio=25
        )

        pvc_size = int(get_osd_pods_memory_sum() * 0.25)
        pvc_obj = pvc_factory(
            interface=constant.CEPHBLOCKPOOL, size=pvc_size,
            volume_mode=constants.VOLUME_MODE_BLOCK
        )
        pvc_objs.append(pvc_obj)
        pod_obj = pod_factory(
            pvc=pvc_obj, pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
            raw_block_pv=True
        )
        pod_objs.append(pod_obj)

        current_throughput = self.cl_obj.calc_average_throughput()

        if target_throughput:
            if current_throughput > target_throughput * 0.8:
                self.logger.info(
                    f"Cluster limit has been reached. It is {current_throughput} Mb/s"
                )
                limit_reached = True

        time_to_wait = 60 * 30
        time_before = time.time()
        if not target_throughput:
            self.logger.info(
                f"\n=====================================================================\n"
                f"Determining the cluster throughput limit. Once determined, IOs will be"
                f"\nreduced to load at {target_percentage * 100}% of the cluster limit"
                f"\n====================================================================="
            )

        while not limit_reached:

            self.logger.info(
                f"The cluster average collected throughput BEFORE starting "
                f"IOs on the newly created pod is {current_throughput} Mb/s"
            )

            io_file_size = f"{pvc_size - 1}G"
            pod_obj.run_io(
                storage_type='block', size=io_file_size,
                runtime=io_run_time, rate=rate, bs=bs, rw_ratio=25
            )

            self.logger.info(
                f"While IO kicks-in on the previously created pod ({pod_obj.name}), "
                f"creating a new pod and PVC for the next iteration"
            )
            pvc_size = int(get_osd_pods_memory_sum() * 0.25)
            pvc_obj = pvc_factory(
                interface=constant.CEPHBLOCKPOOL, size=pvc_size,
                volume_mode=constants.VOLUME_MODE_BLOCK
            )
            pvc_objs.append(pvc_obj)
            pod_obj = pod_factory(
                pvc=pvc_obj, pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
                raw_block_pv=True
            )
            pod_objs.append(pod_obj)
            previous_throughput = current_throughput
            current_throughput = self.cl_obj.calc_average_throughput()
            self.logger.info(
                f"The cluster average collected throughput AFTER starting IOs on the newly"
                f" created pod is {current_throughput} Mb/s, while before IOs started "
                f"on the newly created pod it was {previous_throughput}.\nThe number of "
                f"pods running IOs is {len(pod_objs) - 1}")

            if not target_throughput:
                if current_throughput > previous_throughput * 0.8 and (
                    current_throughput > 20
                ):
                    tp_diff = (current_throughput / previous_throughput * 100) - 100
                    if tp_diff < 10:
                        limit_reached = True
                        cluster_limit = current_throughput
                        self.logger.info(
                            f"\n===================================================\n"
                            f"The cluster throughput limit is {cluster_limit} Mb/s\n"
                            f"==================================================="
                        )
                    else:
                        self.logger.info(
                            f"\n================================================================\n"
                            f"The throughput difference after starting FIO on the newly created\n"
                            f"pod is {tp_diff:.2f}%. We are waiting for it to be less than 10%"
                            f"\n================================================================"
                        )
                        continue
            else:
                if current_throughput > target_throughput * 0.8:
                    self.logger.info(
                        f"Cluster limit has been reached. It is {current_throughput} Mb/s"
                    )
                    limit_reached = True
            if time.time() > time_before + time_to_wait:
                if not target_throughput:
                    self.logger.warning(
                        f"Could not determine the cluster throughput limit "
                        f"within the given {time_to_wait} timeout. Breaking"
                    )
                else:
                    self.logger.warning(
                        f"Could not reach the cluster throughput percentage "
                        f"within the given {time_to_wait} timeout. Breaking"
                    )
                limit_reached = True
                cluster_limit = current_throughput

            if current_throughput < 20:
                if time.time() > time_before + (time_to_wait * 0.5):
                    self.logger.warning(
                        f"Waited for {time_to_wait * 0.5} seconds and the"
                        f" throughput is less than 20 Mb/s. Breaking"
                    )
                    cluster_limit = current_throughput
                if len(pod_objs) > 8:
                    self.logger.warning(
                        f"The number of pods running IO is {len(pod_objs)} "
                        f"and the throughput is less than 20 Mb/s. Breaking"
                    )
                    limit_reached = True
                    cluster_limit = current_throughput

            cluster_used_space = get_percent_used_capacity()
            if cluster_used_space > 50:
                if not target_throughput:
                    self.logger.warning(
                        f"Cluster used space is {cluster_used_space}%. Could not find the "
                        f"cluster throughput limit before the used spaced reached 50%. Breaking"
                    )
                else:
                    self.logger.warning(
                        f"Cluster used space is {cluster_used_space}%. Could not reach the "
                        f"cluster throughput target percentage before the used spaced reached"
                        f" 50%. Breaking"
                    )
                limit_reached = True
                cluster_limit = current_throughput
        if not target_throughput:
            target_throughput = cluster_limit * target_percentage
            self.logger.info(f"The target throughput is {target_throughput}")
            current_throughput = cluster_limit
            self.logger.info(f"The current throughput is {current_throughput}")

            self.logger.info(
                "Start deleting pods that are running IO one by one while comparing "
                "the current throughput utilization with the target one. The goal is "
                "to reach cluster throughput utilization that is more or less the target"
                " throughput percentage"
            )
            while current_throughput > (target_throughput * 1.2) and len(pod_objs) > 1:
                pod_name = pod_objs[-1].name
                pod_objs[-1].delete()
                pod_objs[-1].ocp.wait_for_delete(pod_objs[-1].name)
                pod_objs.remove(pod_objs[-1])
                pvc_objs[-1].delete()
                pvc_objs[-1].ocp.wait_for_delete(pvc_objs[-1].name)
                pvc_objs.remove(pvc_objs[-1])
                self.logger.info(f"Waiting for IO to be stopped on pod {pod_name}")
                time.sleep(10)
                current_throughput = self.cl_obj.calc_average_throughput()
                self.logger.info(
                    f"The cluster average collected throughput after deleting "
                    f"pod {pod_name} is {current_throughput} Mb/s"
                )

        self.logger.info(
            f"\n==============================================\n"
            f"The number of pods that will continue running"
            f"\nIOs is {len(pod_objs)} at a load of {current_throughput} Mb/s"
            f"\n=============================================="
        )
        return cluster_limit, current_throughput
