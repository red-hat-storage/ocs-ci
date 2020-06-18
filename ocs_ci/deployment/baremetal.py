import logging

from ocs_ci.deployment.deployment import Deployment

logger = logging.getLogger(__name__)


class BAREMETALUPI(Deployment):
    """
    A class to handle Bare metal UPI specific deployment
    """

    def __init__(self):
        logger.info("BAREMETAL UPI")
        super().__init__()
        # TODO: OCP,deployment