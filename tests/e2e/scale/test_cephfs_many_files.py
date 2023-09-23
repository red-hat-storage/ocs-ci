import logging
import os.path
import pytest
import yaml
from shutil import rmtree
from tempfile import mkdtemp, NamedTemporaryFile
import uuid

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import scale, orange_squad
from ocs_ci.framework.testlib import E2ETest, ignore_leftovers
from ocs_ci.ocs import ocp, constants
from ocs_ci.utility.utils import run_cmd, ceph_health_check
from ocs_ci.helpers import helpers, disruption_helpers
from ocs_ci.ocs.resources.pod import wait_for_storage_pods
from ocs_ci.helpers.storageclass_helpers import storageclass_name

TARFILE = "cephfs.tar.gz"
SIZE = "20Gi"
TFILES = 1000000
SAMPLE_TEXT = b"A"

log = logging.getLogger(__name__)


def add_million_files(pod_name, ocp_obj):
    """
    Create a directory with one million files in it.
    Tar that directory to a zipped tar file.
    rsynch that tar file to the cephfs pod
    Extract the tar files on ceph pod onto the mounted ceph filesystem.

    Returns:
        list: list of ten of the files created.
    """
    log.info(f"Creating {TFILES} files on Cephfs")
    onetenth = TFILES / 10
    endoften = onetenth - 1
    ntar_loc = mkdtemp()
    tarfile = os.path.join(ntar_loc, TARFILE)
    new_dir = mkdtemp()
    test_file_list = []
    for i in range(0, TFILES):
        tmpfile = NamedTemporaryFile(dir=new_dir, delete=False)
        fname = tmpfile.name
        with tmpfile:
            tmpfile.write(SAMPLE_TEXT)
        if i % onetenth == endoften:
            dispv = i + 1
            log.info(f"{dispv} local files created")
            test_file_list.append(fname.split(os.sep)[-1])
    tmploc = ntar_loc.split("/")[-1]
    run_cmd(f"tar cfz {tarfile} -C {new_dir} .", timeout=1800)
    ocp_obj.exec_oc_cmd(
        f"rsync {ntar_loc} {pod_name}:{constants.MOUNT_POINT}", timeout=300
    )
    ocp_obj.exec_oc_cmd(f"exec {pod_name} -- mkdir {constants.MOUNT_POINT}/x")
    ocp_obj.exec_oc_cmd(
        f"exec {pod_name} -- /bin/tar xf"
        f" {constants.MOUNT_POINT}/{tmploc}/{TARFILE}"
        f" -C {constants.MOUNT_POINT}/x",
        timeout=3600,
    )
    rmtree(new_dir)
    os.remove(tarfile)
    return test_file_list


class MillionFilesOnCephfs(object):
    """
    Create pvc and cephfs pod, make sure that the pod is running.
    """

    def __init__(self):
        with open(constants.CSI_CEPHFS_POD_YAML, "r") as pod_fd:
            pod_info = yaml.safe_load(pod_fd)
        pvc_name = pod_info["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"]
        # Make sure the pvc and pod names are unique, so AlreadyExists
        # exceptions are not thrown.
        pvc_name += str(uuid.uuid4())
        self.pod_name = pod_info["metadata"]["name"] + str(uuid.uuid4())
        config.RUN["cli_params"]["teardown"] = True
        self.cephfs_pvc = helpers.create_pvc(
            sc_name=storageclass_name(constants.OCS_COMPONENTS_MAP["cephfs"]),
            namespace=config.ENV_DATA["cluster_namespace"],
            pvc_name=pvc_name,
            size=SIZE,
        )
        helpers.wait_for_resource_state(
            self.cephfs_pvc, constants.STATUS_BOUND, timeout=1200
        )
        self.cephfs_pod = helpers.create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            namespace=config.ENV_DATA["cluster_namespace"],
            pvc_name=pvc_name,
            pod_name=self.pod_name,
        )
        helpers.wait_for_resource_state(
            self.cephfs_pod, constants.STATUS_RUNNING, timeout=300
        )
        log.info("pvc and cephfs pod created")
        self.ocp_obj = ocp.OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
        )

        self.test_file_list = add_million_files(self.pod_name, self.ocp_obj)
        log.info("cephfs test files created")

    def cleanup(self):
        self.cephfs_pod.delete()
        self.cephfs_pvc.delete()
        log.info("Teardown complete")


@pytest.fixture(scope="class")
def million_file_cephfs(request):
    million_file_cephfs = MillionFilesOnCephfs()

    def teardown():
        million_file_cephfs.cleanup()

    request.addfinalizer(teardown)

    return million_file_cephfs


@orange_squad
@scale
@ignore_leftovers
@pytest.mark.parametrize(
    argnames=["resource_to_delete"],
    argvalues=[
        pytest.param(*["mgr"], marks=pytest.mark.polarion_id("OCS-2606")),
        pytest.param(*["mon"], marks=pytest.mark.polarion_id("OCS-2607")),
        pytest.param(*["osd"], marks=pytest.mark.polarion_id("OCS-2608")),
        pytest.param(*["mds"], marks=pytest.mark.polarion_id("OCS-2609")),
    ],
)
class TestMillionCephfsFiles(E2ETest):
    """
    Million cephfs files tester.
    """

    def test_scale_million_cephfs_files(
        self,
        million_file_cephfs,
        resource_to_delete,
    ):
        """
        Add a million files to the ceph filesystem
        Delete each instance of the parametrized ceph pod once
        the ceph cluster is healthy.  Make sure the ceph cluster comes back
        up and that rename operations function as expected.

        Args:
            million_file_cephfs (MillionFilesOnCephfs object):
                Tracks cephfs pod, pvcs, and list of files to rename.
            resource_to_delete (str): resource deleted for each testcase

        """
        log.info(f"Testing respin of {resource_to_delete}")
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        disruption.delete_resource()
        ocp_obj = million_file_cephfs.ocp_obj
        for sfile in million_file_cephfs.test_file_list:
            sample = os.sep.join([constants.MOUNT_POINT, "x", sfile])
            newname = str(uuid.uuid4())
            fullnew = os.sep.join([constants.MOUNT_POINT, "x", newname])
            ocp_obj.exec_oc_cmd(
                f"exec {million_file_cephfs.pod_name} -- mv {sample} {fullnew}"
            )
            ocp_obj.exec_oc_cmd(
                f"exec {million_file_cephfs.pod_name} -- mv {fullnew} {sample}"
            )
        log.info("Tests complete")

        # Validate storage pods are running
        wait_for_storage_pods()

        # Validate cluster health ok and all pods are running
        assert ceph_health_check(delay=180), "Ceph health in bad state"
