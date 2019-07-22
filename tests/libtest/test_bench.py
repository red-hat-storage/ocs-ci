import os
os.sys.path.append(os.path.dirname(os.getcwd()))

from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import libtest
from tests.libtest import test_radosbench as radosbench


@libtest
def test_run():
    tools_pod = pod.get_ceph_tools_pod()
    tools_pod.add_role(role='client')

    return radosbench.run(
        ceph_pods=[tools_pod],
        config={'time': 10, 'cleanup': False}
    )
