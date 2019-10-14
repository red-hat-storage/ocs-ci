import os
import logging
os.sys.path.append(os.path.dirname(os.getcwd()))

from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs.resources import pod

logger = logging.getLogger(__name__)


@libtest
def test_pod_exec():
    tools_pod = pod.get_ceph_tools_pod()
    cmd = "ceph osd df"

    out, err, ret = tools_pod.exec_ceph_cmd(ceph_cmd=cmd)
    if out:
        logger.info(out)
    if err:
        logger.error(err)
    logger.info(ret)
