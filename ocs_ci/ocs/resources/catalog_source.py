"""
CatalogSource related functionalities
"""
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.exceptions import CommandFailed, ResourceInUnexpectedState
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


class CatalogSource(OCP):
    """
    This class represent CatalogSource and contains all related
    methods we need to do with it.
    """

    def __init__(
        self, resource_name="", namespace=None, *args, **kwargs
    ):
        """
        Initializer function for CatalogSource class

        Args:
            resource_name (str): Name of catalog source
            namespace (str): Namespace to which this catalogsource belongs

        """
        super(CatalogSource, self).__init__(
            resource_name=resource_name, namespace=namespace,
            kind='CatalogSource', *args, **kwargs,
        )

    def get_image_name(self):
        """
        Fetch image name from catalog source resource

        Returns:
            image info (str): especially version info extracted from image
                name

        """
        self.check_name_is_specified()
        try:
            data = self.get()
        except CommandFailed:
            logger.warning(
                f"Cannot find CatalogSource object {self.resource_name}"
            )
            return None
        return data['spec']['image'].split(":")[1]

    def get_image_url(self):
        """
        Fetch image url from catalog source resource

        Returns:
            image url (str): URL of image

        """
        self.check_name_is_specified()
        try:
            data = self.get()
        except CommandFailed:
            logger.warning(
                f"Cannot find CatalogSource object {self.resource_name}"
            )
            return None
        return data['spec']['image'].split(":")[0]

    def check_state(self, state):
        """
        Check state of catalog source

        Args:
            state (str): State of CatalogSource object

        Returns:
            bool: True if state of object is the same as desired one, False
                otherwise.

        """
        self.check_name_is_specified()
        try:
            data = self.get()
        except CommandFailed:
            logger.info(
                f"Cannot find CatalogSource object {self.resource_name}"
            )
            return False
        try:
            current_state = data['status']['connectionState'][
                'lastObservedState'
            ]
            logger.info(
                f"Catalog source {self.resource_name} is in state: "
                f"{current_state}!"
            )
            return current_state == state
        except KeyError:
            logger.info(
                f"Problem while reading state status of catalog source "
                f"{self.resource_name}, data: {data}"
            )
        return False

    @retry(ResourceInUnexpectedState, tries=4, delay=5, backoff=1)
    def wait_for_state(self, state, timeout=900, sleep=5):
        """
        Wait till state of catalog source resource is the same as required one
        passed in the state parameter.

        Args:
            state (str): Desired state of catalog source object
            timeout (int): Timeout in seconds to wait for desired state
            sleep (int): Time in seconds to sleep between attempts

        Raises:
            ResourceInUnexpectedState: In case the catalog source is not in
                expected state.

        """
        self.check_name_is_specified()
        sampler = TimeoutSampler(
            timeout, sleep, self.check_state, state=state
        )
        if not sampler.wait_for_func_status(True):
            raise ResourceInUnexpectedState(
                f"Catalog source: {self.resource_name} is not in expected "
                f"state: {state}"
            )
