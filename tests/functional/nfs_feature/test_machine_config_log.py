from ocs_ci.utility.utils import clean_up_pods_for_provider


def test_machine_controller_logs():
    """
    This is to test the log messahes for the machine-controller
    """
    clean_up_pods_for_provider(node_type="master")
