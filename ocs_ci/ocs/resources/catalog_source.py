"""
CatalogSource related functionalities
"""

import logging
from time import sleep

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, ResourceWrongStatusException
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


class CatalogSource(OCP):
    """
    This class represent CatalogSource and contains all related
    methods we need to do with it.
    """

    def __init__(self, resource_name="", namespace=None, *args, **kwargs):
        """
        Initializer function for CatalogSource class

        Args:
            resource_name (str): Name of catalog source
            namespace (str): Namespace to which this catalogsource belongs

        """
        super(CatalogSource, self).__init__(
            resource_name=resource_name,
            namespace=namespace,
            kind="CatalogSource",
            *args,
            **kwargs,
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
            logger.warning(f"Cannot find CatalogSource object {self.resource_name}")
            return None
        return data["spec"]["image"].rsplit(":", 1)[1]

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
            logger.warning(f"Cannot find CatalogSource object {self.resource_name}")
            return None
        return data["spec"]["image"].rsplit(":", 1)[0]

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
            logger.info(f"Cannot find CatalogSource object {self.resource_name}")
            return False
        try:
            current_state = data["status"]["connectionState"]["lastObservedState"]
            logger.info(
                f"Catalog source {self.resource_name} is in state: " f"{current_state}!"
            )
            return current_state == state
        except KeyError:
            logger.info(
                f"Problem while reading state status of catalog source "
                f"{self.resource_name}, data: {data}"
            )
        return False

    @retry(ResourceWrongStatusException, tries=4, delay=5, backoff=1)
    def wait_for_state(self, state, timeout=480, sleep=5):
        """
        Wait till state of catalog source resource is the same as required one
        passed in the state parameter.

        Args:
            state (str): Desired state of catalog source object
            timeout (int): Timeout in seconds to wait for desired state
            sleep (int): Time in seconds to sleep between attempts

        Raises:
            ResourceWrongStatusException: In case the catalog source is not in
                expected state.

        """
        self.check_name_is_specified()
        sampler = TimeoutSampler(timeout, sleep, self.check_state, state=state)
        if not sampler.wait_for_func_status(True):
            raise ResourceWrongStatusException(
                f"Catalog source: {self.resource_name} is not in expected "
                f"state: {state}"
            )


def disable_default_sources():
    """
    Disable default sources
    """
    logger.info("Disabling default sources")
    run_cmd(constants.PATCH_DEFAULT_SOURCES_CMD.format(disable="true"))
    logger.info("Waiting 20 seconds after disabling default sources")
    sleep(20)


def enable_default_sources():
    """
    Enable default sources
    """
    logger.info("Enabling default sources")
    run_cmd(constants.PATCH_DEFAULT_SOURCES_CMD.format(disable="false"))
    logger.info("Waiting 20 seconds after enabling default sources")
    sleep(20)


def disable_specific_source(source_name):
    """
    Disable specific default source

    Args:
        source_name (str): Source name (e.g. redhat-operators)

    """
    logger.info(f"Disabling default source: {source_name}")
    run_cmd(
        constants.PATCH_SPECIFIC_SOURCES_CMD.format(
            disable="true", source_name=source_name
        )
    )
    logger.info(f"Waiting 20 seconds after disabling source: {source_name}")
    sleep(20)


def enable_specific_source(source_name):
    """
    Enable specific default source

    Args:
        source_name (str): Source name (e.g. redhat-operators)

    """
    logger.info(f"Enabling default source: {source_name}")
    run_cmd(
        constants.PATCH_SPECIFIC_SOURCES_CMD.format(
            disable="false", source_name=source_name
        )
    )
    logger.info(f"Waiting 20 seconds after enabling source: {source_name}")
    sleep(20)


def get_odf_tag_from_redhat_catsrc():
    """
    Get the ODF tag from the default redhat-operators Catalog Source

    Returns:
        str: ODF tag from redhat-operators Catalog Source
    """
    from ocs_ci.ocs.ocp import OCP

    catsrc_data = OCP(
        kind=constants.CATSRC,
        namespace=constants.MARKETPLACE_NAMESPACE,
        resource_name="redhat-operators",
    ).get()
    registry_image = catsrc_data.get("spec").get("image")
    return registry_image.split(":")[-1]
