import logging
import re

from ocs_ci.ocs.resources.pod import get_pod_logs
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.framework.testlib import tier2, bugzilla, skipif_ocs_version, polarion_id

log = logging.getLogger(__name__)


@tier2
@bugzilla("2155743")
@skipif_ocs_version("<4.13")
@polarion_id("OCS-4876")
def test_get_requests_by_kube_probe():
    """
    Verify GET requests initiated by kube-probe on odf-console pod

    """
    log.info("Verify GET requests initiated by kube-probe on odf-console pod")
    pod_odf_console_name = get_pod_name_by_pattern("odf-console")
    pod_odf_console_logs = get_pod_logs(pod_name=pod_odf_console_name[0])
    if (
        re.search("GET /plugin-manifest.json HTTP.*kube-probe", pod_odf_console_logs)
        is None
    ):
        raise ValueError("GET request initiated by kube-probe does not exist")
