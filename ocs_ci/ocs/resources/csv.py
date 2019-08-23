"""
CSV related functionalities
"""
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


class CSV(OCP):
    def __init__(self, name="", *args, **kwargs):
        """
        Initializer function for CSV class

        Args:
            name (str): Name of CSV

        """
        super(CSV, self).__init__(*args, **kwargs)
        self.name = name

    def check_phase(self, phase):
        """
        Check phase of CSV resource

        Args:
            phase (str): Phase of CSV object

        Returns:
            bool: True if phase of object is the same as passed one, False
                otherwise.

        """
        data = {}
        try:
            data = self.get(resource_name=self.name)
        except CommandFailed:
            logger.info(f"Cannot find CSV object {self.name}")
        try:
            return data['status']['phase'] == phase
        except KeyError:
            logger.info(
                f"Problem while reading phase status of CSV {self.name}, "
                f"data: {data}"
            )
        return False

    def wait_for_phase(self, phase, timeout=300, sleep=5):
        """
        Wait till phase of CSV resource is the same as required one passed in
        the phase parameter.

        Args:
            phase (str): Desired phase of CSV object
            timeout (int): Timeout in seconds to wait for desired phase
            sleep (int): Time in seconds to sleep between attempts

        """
        sampler = TimeoutSampler(
            timeout, sleep, self.check_phase, phase=phase
        )
        sampler.wait_for_func_status(True)
