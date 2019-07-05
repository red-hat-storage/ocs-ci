import os
os.sys.path.append(os.path.dirname(os.getcwd()))

from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs.resources import pod


@libtest
def test_main():
    tools_pod = pod.get_ceph_tools_pod()
    cmd = "ceph osd df"

    out, err, ret = tools_pod.exec_ceph_cmd(ceph_cmd=cmd)
    if out:
        print(out)
    if err:
        print(err)
    print(ret)
