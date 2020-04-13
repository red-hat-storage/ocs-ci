import logging

from ocs_ci.framework.pytest_customization.marks import tier2
from ocs_ci.framework.testlib import polarion_id, bugzilla
from ocs_ci.ocs import defaults
from ocs_ci.utility.utils import run_async

log = logging.getLogger(__name__)


@tier2
@polarion_id('OCS-2084')
@bugzilla('1799077')
def test_verify_noobaa_status():
    """
    Verify noobaa status output is clean without any errors
    """
    # Get noobaa status
    noobaa_status = run_async(
        f'noobaa status -n {defaults.ROOK_CLUSTER_NAMESPACE} 2>&1'
    )
    ret, out, _ = noobaa_status.async_communicate()
    assert not ret, (
        f"noobaa status command failed.\nreturn code: {ret}\nstdout:\n{out}"
    )

    # Verify noobaa status
    for content, count in defaults.NOOBAA_STATUS_CONTENT_COUNT.items():
        assert count == out.count(f'Exists: {content} '), (
            f"Could not find expected match for {content} in noobaa status "
            f"output. noobaa status:\n{out}"
        )
    assert 'System Phase is \\"Ready\\"' in out, (
        f"System Phase is not 'Ready'. noobaa status:\n{out}"
    )
    assert 'Exists:  \\"noobaa-admin\\"' in out, (
        f"'noobaa-admin' does not exists. noobaa status:\n{out}"
    )

    for line in out.split('\n'):
        if 'Not Found' in line:
            assert 'Optional' in line, f"Error in noobaa status output- {line}"
    log.info("Verified: noobaa status does not contain any error.")
