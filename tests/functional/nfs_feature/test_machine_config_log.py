from ocs_ci.utility.utils import wait_for_machineconfigpool_status


def test_machine_controller_logs():
    """
    This is to test the log messahes for the machine-controller
    """
    wait_for_machineconfigpool_status(node_type="worker", force_delete_pods=True)
