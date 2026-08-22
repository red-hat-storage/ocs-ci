import logging
import time

from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.framework.testlib import (
    ui,
    skipif_ocs_version,
    tier2,
    skipif_ibm_cloud_managed,
    polarion_id,
    fdf_required,
    runs_on_provider,
    cnsa_remote_mount,
)
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import black_squad
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)

SCALE_CONNECTION_NAME = "scale-cluster-1"
FILESYSTEM_1 = "fs1"
FILESYSTEM_2 = "fs2"
FILESYSTEM_3 = "fs3"


def create_scale_filesystem(filesystem_name, ssh_key_path):
    """
    Create a new filesystem on the Scale cluster

    Args:
        filesystem_name (str): Name of the filesystem to create
        ssh_key_path (str): Path to SSH private key for Scale cluster

    Returns:
        bool: True if filesystem created successfully
    """
    scale_endpoint = config.ENV_DATA.get("scale_endpoint")

    # Define NSDs for the new filesystem (using available partitions)
    nsd_stanza = (
        "%nsd: device=/dev/sdb5 nsd=nsd_fs3_1 servers=ebondare-scale1 usage=dataAndMetadata pool=system\n"
        "%nsd: device=/dev/sdb5 nsd=nsd_fs3_2 servers=ebondare-scale3 usage=dataAndMetadata pool=system\n"
        "%nsd: device=/dev/sdc1 nsd=nsd_fs3_3 servers=ebondare-scale1 usage=dataAndMetadata pool=system\n"
    )

    # Create NSD stanza file
    create_stanza_cmd = (
        f"ssh root@{scale_endpoint} -i {ssh_key_path} "
        f"\"echo '{nsd_stanza}' > /tmp/{filesystem_name}_nsd.stanza\""
    )
    logger.info("Creating NSD stanza file for %s", filesystem_name)
    exec_cmd(create_stanza_cmd)

    # Create NSDs
    create_nsd_cmd = (
        f"ssh root@{scale_endpoint} -i {ssh_key_path} "
        f'"/usr/lpp/mmfs/bin/mmcrnsd -F /tmp/{filesystem_name}_nsd.stanza"'
    )
    logger.info("Creating NSDs for %s", filesystem_name)
    exec_cmd(create_nsd_cmd)

    # Create filesystem
    create_fs_cmd = (
        f"ssh root@{scale_endpoint} -i {ssh_key_path} "
        f"/usr/lpp/mmfs/bin/mmcrfs {filesystem_name} -F "
        f"/tmp/{filesystem_name}_nsd.stanza -B 4M -Q yes -T /ibm/{filesystem_name}"
    )
    logger.info("Creating filesystem %s", filesystem_name)
    exec_cmd(create_fs_cmd)

    # Mount filesystem
    mount_fs_cmd = (
        f"ssh root@{scale_endpoint} -i {ssh_key_path} "
        f'"/usr/lpp/mmfs/bin/mmmount {filesystem_name} -a"'
    )
    logger.info("Mounting filesystem %s", filesystem_name)
    exec_cmd(mount_fs_cmd)

    return True


def delete_scale_filesystem(filesystem_name, ssh_key_path):
    """
    Delete a filesystem from the Scale cluster

    Args:
        filesystem_name (str): Name of the filesystem to delete
        ssh_key_path (str): Path to SSH private key for Scale cluster

    Returns:
        bool: True if filesystem deleted successfully
    """
    scale_endpoint = config.ENV_DATA.get("scale_endpoint")

    # Unmount filesystem
    unmount_fs_cmd = (
        f"ssh root@{scale_endpoint} -i {ssh_key_path} "
        f'"/usr/lpp/mmfs/bin/mmumount {filesystem_name} -a"'
    )
    logger.info("Unmounting filesystem %s", filesystem_name)
    exec_cmd(unmount_fs_cmd, ignore_error=True)

    # Delete filesystem
    delete_fs_cmd = (
        f"ssh root@{scale_endpoint} -i {ssh_key_path} "
        f'"/usr/lpp/mmfs/bin/mmdelfs {filesystem_name} -p"'
    )
    logger.info("Deleting filesystem %s", filesystem_name)
    exec_cmd(delete_fs_cmd)

    # Delete NSDs
    delete_nsd_cmd = (
        f"ssh root@{scale_endpoint} -i {ssh_key_path} "
        f'"/usr/lpp/mmfs/bin/mmdelnsd -p nsd_fs3_1,nsd_fs3_2,nsd_fs3_3"'
    )
    logger.info("Deleting NSDs for %s", filesystem_name)
    exec_cmd(delete_nsd_cmd, ignore_error=True)

    # Clean up stanza file
    cleanup_cmd = (
        f"ssh root@{scale_endpoint} -i {ssh_key_path} "
        f'"rm -f /tmp/{filesystem_name}_nsd.stanza"'
    )
    exec_cmd(cleanup_cmd, ignore_error=True)

    return True


@fdf_required
@runs_on_provider
@cnsa_remote_mount
class TestScaleConnection(object):
    """
    Test connecting Scale cluster

    To be executed on FDF only
    """

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.20")
    @black_squad
    @polarion_id("OCS-7757")
    def test_connect_scale(self, setup_ui_class):
        """
        Test connecting Scale cluster as External system
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.connect_scale(
            system_name=SCALE_CONNECTION_NAME,
            endpoint=config.ENV_DATA["scale_endpoint"],
            port="443",
            username=config.ENV_DATA["scale_username"],
            password=config.ENV_DATA["scale_password"],
            filesystem_name=FILESYSTEM_1,
        )
        assert external_systems.scale_present_on_page(SCALE_CONNECTION_NAME)
        # checking status temporarily disabled
        # until https://issues.redhat.com/browse/DFBUGS-4352 is fixed
        # assert external_systems.scale_status_ok(SCALE_CONNECTION_NAME)

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.21")
    @black_squad
    @polarion_id("OCS-7758")
    def test_add_delete_filesystem(self, setup_ui_class):
        """
        Test creating a filesystem on Scale cluster, connecting it via UI,
        then deleting the connection and the filesystem
        """
        ssh_key_path = config.ENV_DATA.get(
            "scale_ssh_key", "/home/lena/scale_files/openshift-dev.pem"
        )

        # Create filesystem on Scale cluster
        logger.info("Creating filesystem %s on Scale cluster", FILESYSTEM_3)
        create_scale_filesystem(FILESYSTEM_3, ssh_key_path)

        # Connect to the filesystem via UI
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.connect_scale_filesystem(
            scale_name=SCALE_CONNECTION_NAME, filesystem_name=FILESYSTEM_3
        )

        # Delete the filesystem connection from UI
        external_systems.delete_scale_filesystem(
            scale_name=SCALE_CONNECTION_NAME, filesystem_name=FILESYSTEM_3
        )

        # Delete the filesystem from Scale cluster
        logger.info("Deleting filesystem %s from Scale cluster", FILESYSTEM_3)
        delete_scale_filesystem(FILESYSTEM_3, ssh_key_path)

    @ui
    @skipif_ibm_cloud_managed
    @tier2
    @skipif_ocs_version("<4.20")
    @black_squad
    @polarion_id("OCS-7759")
    def test_disconnect_scale(self, setup_ui_class):
        """
        Test that disconnecting scale removes it from External systems page
        """
        scale_connect_obj = PageNavigator()
        external_systems = scale_connect_obj.nav_external_systems_page()
        external_systems.disconnect_scale(
            scale_name=SCALE_CONNECTION_NAME,
        )
        time.sleep(10)
        assert not external_systems.scale_present_on_page(
            scale_name=SCALE_CONNECTION_NAME
        )
