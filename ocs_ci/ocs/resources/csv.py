"""
CSV related functionalities
"""
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed, ResourceInUnexpectedState
from ocs_ci.utility.utils import TimeoutSampler


logger = logging.getLogger(__name__)


class CSV(OCP):
    """
    This class represent ClusterServiceVersion (CSV) and contains all related
    methods we need to do with CSV.
    """

    def __init__(self, resource_name="", *args, **kwargs):
        """
        Initializer function for CSV class

        Args:
            resource_name (str): Name of CSV

        """
        super(CSV, self).__init__(
            resource_name=resource_name, *args, **kwargs
        )

    def check_phase(self, phase):
        """
        Check phase of CSV resource

        Args:
            phase (str): Phase of CSV object

        Returns:
            bool: True if phase of object is the same as passed one, False
                otherwise.

        """
        self.check_name_is_specified()
        try:
            data = self.get()
        except CommandFailed:
            logger.info(f"Cannot find CSV object {self.resource_name}")
            return False
        try:
            current_phase = data['status']['phase']
            logger.info(f"CSV {self.resource_name} is in phase: {current_phase}!")
            return current_phase == phase
        except KeyError:
            logger.info(
                f"Problem while reading phase status of CSV "
                f"{self.resource_name}, data: {data}"
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

        Raises:
            ResourceInUnexpectedState: In case the CSV is not in expected
                phase.

        """
        self.check_name_is_specified()
        sampler = TimeoutSampler(
            timeout, sleep, self.check_phase, phase=phase
        )
        if not sampler.wait_for_func_status(True):
            raise ResourceInUnexpectedState(
                f"CSV: {self.resource_name} is not in expected phase: {phase}"
            )
