import pytest
import shutil
from ocs_ci.utility.utils import clone_repo
from ocs_ci.ocs import constants


@pytest.fixture(scope="session")
def clone_upstream_ceph(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'upstream ceph' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("upstream_ceph_dir")

    def finalizer():
        shutil.rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(
        constants.CEPH_UPSTREAM_REPO, str(repo_dir), branch="main", tmp_repo=True
    )
    return repo_dir


@pytest.fixture(scope="session")
def clone_ocs_operator(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'ocs operator' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("ocs_operator_dir")

    def finalizer():
        shutil.rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(constants.OCS_OPERATOR_REPO, str(repo_dir), branch="main", tmp_repo=True)
    return repo_dir


@pytest.fixture(scope="session")
def clone_odf_monitoring_compare_tool(request, tmp_path_factory):
    """
    fixture to make temporary directory for the 'ODF monitor compare tool' and clone repo to it
    """
    repo_dir = tmp_path_factory.mktemp("monitor_tool_dir")

    def finalizer():
        shutil.rmtree(repo_dir, ignore_errors=True)

    request.addfinalizer(finalizer)
    clone_repo(
        constants.ODF_MONITORING_TOOL_REPO, str(repo_dir), branch="main", tmp_repo=True
    )
    return repo_dir
