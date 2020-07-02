import logging
import os

from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions
from .deployment import Deployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from .flexy import FlexyBaremetalPSI
from ocs_ci.utility.utils import (
    run_cmd, TimeoutSampler, load_auth_config, get_infra_id
)
from ocs_ci.utility import psiutils

from ocs_ci.deployment.deployment import Deployment

logger = logging.getLogger(__name__)


class BAREMETALUPI(Deployment):
    """
    A class to handle Bare metal UPI specific deployment
    """

    def __init__(self):
        logger.info("BAREMETAL UPI")
        super().__init__()
        # TODO: OCP,deployment


class BaremetalPSIUPI(Deployment):
    """
    All the functionalities related to BaremetalPSI- UPI deployment
    lives here
    """
    def __init__(self):
        self.cluster_name = config.ENV_DATA['cluster_name']
        super().__init__()

    class OCPDeployment(BaseOCPDeployment):
        def __init__(self):
            super().__init__()
            self.flexy_instance = FlexyBaremetalPSI()
            self.psi_conf = load_auth_config()['psi']
            logger.info(self.psi_conf)
            self.utils = psiutils.PSIUtils(self.psi_conf)

        def deploy_prereq(self):
            """
            Instantiate proper flexy class here

            """
            super().deploy_prereq()
            self.flexy_instance.deploy_prereq()

        def deploy(self, log_level=''):
            self.flexy_instance.deploy(log_level)
            # Point cluster_path to cluster-dir created by flexy
            abs_cluster_path = os.path.abspath(self.cluster_path)
            flexy_cluster_path = os.path.join(
                self.flexy_instance.flexy_host_dir,
                'flexy/workdir/install-dir'
            )
            logger.info(
                "Symlinking %s to %s", abs_cluster_path, flexy_cluster_path
            )
            if os.path.exists(abs_cluster_path):
                os.rmdir(abs_cluster_path)
            os.symlink(flexy_cluster_path, abs_cluster_path)
            self.test_cluster()
            # We need NTP for OCS cluster to become clean
            logger.info("creating ntp chrony")
            run_cmd(f"oc create -f {constants.NTP_CHRONY_CONF}")
            # add disks to instances
            # Get all instances and for each instance add
            # one disk
            pattern = "-".join(
                [get_infra_id(config.ENV_DATA['cluster_path']), "compute"]
            )
            for instance in self.utils.get_instances_with_pattern(pattern):
                vol = self.utils.create_volume(
                    name=f'{pattern}-disk0-{instance.name[-1]}',
                    size=config.FLEXY['volume_size'],
                )
                # wait till volume is available
                sample = TimeoutSampler(
                    300, 10,
                    self.utils.check_expected_vol_status,
                    vol,
                    'available'
                )
                if not sample.wait_for_func_status(True):
                    logger.info("Volume failed to reach 'available'")
                    raise exceptions.PSIVolumeNotInExpectedState
                # attach the volume
                self.utils.attach_volume(vol, instance.id)

        def destroy(self, log_level=''):
            """
            Destroy volumes attached if any and then the cluster
            """
            # Get all the additional volumes and detach,delete.
            volumes = self.utils.get_volumes_with_tag(
                {'cluster_name': config.ENV_DATA['cluster_name']}
            )
            self.flexy_instance.destroy()
            self.utils.detach_and_delete_vols(volumes)
