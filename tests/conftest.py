import pytest


def pytest_collection_modifyitems(session, config, items):
    """
    When collecting tests look at the test path
    and if it includes the name of a type of ceph
    daemon in it, tag the test with the appropriate
    marker. This allows for us to filter and test
    by specific daemon type.

    For example, all tests that live in a file at
    tests/mon/test_mons.py would be tagged with
    a 'mons' marker.
    """
    for item in items:
        test_path = item.location[0]
        if "mon" in test_path:
            item.add_marker(pytest.mark.mons)
        elif "osd" in test_path:
            item.add_marker(pytest.mark.osds)
        elif "mds" in test_path:
            item.add_marker(pytest.mark.mdss)
        elif "mgr" in test_path:
            item.add_marker(pytest.mark.mgrs)
        elif "rbd-mirror" in test_path:
            item.add_marker(pytest.mark.rbdmirrors)
        elif "rgw" in test_path:
            item.add_marker(pytest.mark.rgws)
        elif "nfs" in test_path:
            item.add_marker(pytest.mark.nfss)
        elif "iscsi" in test_path:
            item.add_marker(pytest.mark.iscsigws)
        else:
            item.add_marker(pytest.mark.all)
