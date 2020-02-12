import logging
import pytest

from ocs_ci.ocs import defaults
from ocs_ci.utility.utils import run_async
from ocs_ci.framework.pytest_customization.marks import (
    tier1, noobaa_cli_required
)

log = logging.getLogger(__name__)


@tier1
@noobaa_cli_required
@pytest.mark.polarion_id('OCS-2084')
def test_verify_noobaa_status():
    """
    Verify noobaa status output is clean without any errors
    """
    contents_count = [
        'CustomResourceDefinition-5', 'Namespace-1', 'ServiceAccount-1',
        'Role-1', 'RoleBinding-1', 'ClusterRole-1', 'ClusterRoleBinding-1',
        'Deployment-1', 'NooBaa-1', 'StatefulSet-1', 'Service-2', 'Secret-3',
        'StorageClass-1', 'BucketClass-1', 'PersistentVolumeClaim-1'
    ]

    noobaa_status = run_async(
        f'noobaa status -n {defaults.ROOK_CLUSTER_NAMESPACE} 2>&1'
    )
    ret, out, _ = noobaa_status.async_communicate()
    assert not ret, (
        f"noobaa status command failed.\nreturn code: {ret}\nstdout:\n{out}"
    )

    for content in contents_count:
        value, count = content.split('-')
        assert int(count) == out.count(f'Exists: {value} '), (
            f"Could not find expected match for {value} in noobaa status "
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
            assert 'optional' in line, f"Error in noobaa status output- {line}"
    log.info("Verified: noobaa status does not contain any error.")
