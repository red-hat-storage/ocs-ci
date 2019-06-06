import os
os.sys.path.append(os.path.dirname(os.getcwd()))

from resources import pod
from tests import test_radosbench as radosbench


def test_run():
    tools_pod = pod.get_ceph_tools_pod()
    tools_pod.add_role(role='client')

    return radosbench.run(
        ceph_pods=[tools_pod],
        config={'time': 10, 'cleanup': False}
    )
