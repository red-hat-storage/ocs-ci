import logging

logger = logging.getLogger(__name__)


class BAREMETALUPI:
    """
    A class to handle vSphere UPI specific deployment
    """

    def __init__(self):
        logger.info("BAREMETAL UPI")
        # TODO: OCP,OCS deployment
