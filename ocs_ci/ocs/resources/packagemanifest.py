"""
Package manifest related functionalities
"""
import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.catalog_source import CatalogSource
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import TimeoutSampler


log = logging.getLogger(__name__)


class PackageManifest(OCP):
    """
    This class represent PackageManifest and contains all related methods.
    """

    def __init__(
            self, resource_name='', namespace=constants.MARKETPLACE_NAMESPACE,
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

    def get(self, **kwargs):
        resource_name = kwargs.get("resource_name", "")
        resource_name = resource_name if resource_name else self.resource_name

        data = super(PackageManifest, self).get(**kwargs)
        if type(data) == dict and (data.get('kind') == 'List'):
            items = data['items']
            data_len = len(items)
            if data_len == 1:
                return items[0]
            if data_len > 1 and resource_name:
                items_match_name = [
                    i for i in items if i['metadata']['name'] == resource_name
                ]
                if len(items_match_name) == 1:
                    return items_match_name[0]
                else:
                    return items_match_name
        return data

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
        self, resource_name='', timeout=60, sleep=3, label=None, selector=None,
    ):
        """
        Wait for a packagemanifest exists.

        Args:
            resource_name (str): The name of the resource to wait for.
                If not specified the self.resource_name will be used. At least
                on of those has to be set!
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds
            selector (str): The resource selector to search with.

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
        selector = selector if selector else self.selector
        self.check_name_is_specified(resource_name)

        for sample in TimeoutSampler(
            timeout=timeout, sleep=sleep, func=self.get
        ):
            if sample.get('metadata', {}).get('name') == resource_name:
                log.info(f"package manifest {resource_name} found!")
                return
            log.info(f"package manifest {resource_name} not found!")


def get_selector_for_ocs_operator():
    """
    This is the helper function which returns selector for package manifest.
    It's needed because of conflict with live content and multiple package
    manifests with the ocs-operator name. In case we are using internal builds
    we label catalog source or operator source and using the same selector for
    package manifest.

    Returns:
        str: Selector for package manifest if we are on internal
            builds, otherwise it returns None
    """
    catalog_source = CatalogSource(
        resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    try:
        catalog_source.get()
        return constants.OPERATOR_INTERNAL_SELECTOR
    except CommandFailed:
        log.info("Catalog source not found!")
    operator_source = OCP(
        kind="OperatorSource", resource_name=constants.OPERATOR_SOURCE_NAME,
        namespace=constants.MARKETPLACE_NAMESPACE,
    )
    try:
        operator_source.get()
        return constants.OPERATOR_INTERNAL_SELECTOR
    except CommandFailed:
        log.info("Catalog source not found!")
