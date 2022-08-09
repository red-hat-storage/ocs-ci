from ocs_ci.ocs.acm.acm import AcmAddClusters, login_to_acm
import logging


logger = logging.getLogger(__name__)


def install_submariner_in_acm_ui():
    """
    Install submariner via ACM UI, automate login and installation
    Returns:
        None

    """
    # Get the Selenium driver obj after logging in to ACM
    get_driver = login_to_acm()
    acm_obj = AcmAddClusters(get_driver)
    acm_obj.install_submariner_ui()
    acm_obj.submariner_validation_ui()
