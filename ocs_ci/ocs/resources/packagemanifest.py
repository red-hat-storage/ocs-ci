"""
Package manifest related functionalities
"""
import logging

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP, defaults
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler


log = logging.getLogger(__name__)


class PackageManifest(OCP):
    """
    This class represent PackageManifest and contains all related methods.
    """

    def __init__(
            self, resource_name='', namespace=defaults.MARKETPLACE_NAMESPACE,
            **kwargs
    ):
        """
        Initializer function for PackageManifest class

        Args:
            resource_name (str): Name of package manifest
            namespace (str): Namespace of package manifest

        """
        super(PackageManifest, self).__init__(
            namespace=namespace, resource_name=resource_name, kind='packagemanifest',
            **kwargs
        )

    @retry((CommandFailed), tries=100, delay=5, backoff=1)
    def get_default_channel(self):
        """
        Returns default channel for package manifest

        Returns:
            str: default channel name

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.

        """
        self.check_name_is_specified()
        return self.data['status']['defaultChannel']

    def get_channels(self):
        """
        Returns channels for package manifest

        Returns:
            list: available channels for package manifest

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.

        """
        self.check_name_is_specified()
        return self.data['status']['channels']

    def get_current_csv(self, channel=None):
        """
        Returns current csv for default or specified channel

        Returns:
            str: Current CSV name

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.

        """
        self.check_name_is_specified()
        channel = channel if channel else self.get_default_channel()
        for _channel in self.get_channels():
            if _channel['name'] == channel:
                return _channel['currentCSV']

    def wait_for_resource(
        self, resource_name='', timeout=60, sleep=3
    ):
        """
        Wait for a packagemanifest exists.

        Args:
            resource_name (str): The name of the resource to wait for.
                If not specified the self.resource_name will be used. At least
                on of those has to be set!
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds

        Raises:
            ResourceNameNotSpecifiedException: in case the name is not
                specified.
            TimeoutExpiredError: in case the resource not found in timeout

        """
        log.info(
            f"Waiting for a resource(s) of kind {self._kind}"
            f" identified by name '{resource_name}'"
        )
        resource_name = resource_name if resource_name else self.resource_name
        self.check_name_is_specified(resource_name)

        for sample in TimeoutSampler(
            timeout=timeout, sleep=sleep, func=self.get
        ):
            if sample.get('metadata', {}).get('name') == resource_name:
                log.info(f"package manifest {resource_name} found!")
                return
            log.info(f"package manifest {resource_name} not found!")
