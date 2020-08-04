import logging

logger = logging.getLogger(__name__)


class OPENSTACKIPI:
    """
    A class to handle OpenStack IPI specific deployment
    """

    def __init__(self):
        # TODO: OCP,OCS deployment
        # https://github.com/red-hat-storage/ocs-ci/issues/4822
        logger.info("OPENSTACK IPI (WIP)")
        raise NotImplementedError()
