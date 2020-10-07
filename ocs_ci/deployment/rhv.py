import logging

logger = logging.getLogger(__name__)

class RHVIPI:
    """
    Base structure class to handle RHV IPI specific deployment
    """

    def __init__(self):
        logger.info("RHV IPI");

