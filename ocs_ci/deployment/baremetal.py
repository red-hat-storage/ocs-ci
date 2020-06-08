import logging
import os
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from .deployment import Deployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from .flexy import FlexyBaremetalPSI
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
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


class BAREMETALPSIUPI(Deployment):
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

        def deploy_prereq(self):
            """
            Instantiate proper flexy class here

            """
            super().deploy_prereq()
            self.flexy_instance.deploy_prereq()

        def deploy(self):
            self.flexy_instance.deploy()
            # Point cluster_path to cluster-dir created by flexy
            abs_cluster_path = os.path.abspath(self.cluster_path)
            flexy_cluster_path = os.path.join(
                self.flexy_instance.flexy_mnt_host_dir,
                'flexy/workdir/install-dir'
            )
            logger.info(
                "Symlinking %s to %s", abs_cluster_path, flexy_cluster_path
            )
            os.symlink(abs_cluster_path, flexy_cluster_path)
            self.test_cluster()
            # We need NTP for OCS cluster to become clean
            logger.info("creating ntp chrony")
            run_cmd(f"oc create -f {constants.NTP_CHRONY_CONF}")
            # add disks to instances
            psi_conf = self.build_psi_conf()
            utils = psiutils.PSIUtils(psi_conf)
            # Get all instances and for each instance add
            # one disk
            pattern = config.ENV_DATA['cluster_name']
            for instance in utils.get_instances_with_pattern(pattern):
                vol = utils.create_volume(
                    name=f'disk0',
                    size=config.ENV_DATA['volume_size'],
                )
                # wait till volume is available
                for res in TimeoutSampler(300, 1, vol.status == "available"):
                    if not res:
                        logger("waiting for volume to be avaialble")

                # attach the volume
                utils.attach_volume(vol, instance.id, "/dev/vdb")

        def build_psi_conf(self):
            """
            Get PSI config so that we can access the
            PSI openstack instances and volumes

            """
            psi_conf = dict()
            fd = open(constants.FLEXY_SERVICE_CONF, "r")
            conf = yaml.safe_load(fd)
            upshift_conf = conf['services']['openstack_upshift']
            psi_conf['auth_url'] = upshift_conf['url']
            psi_conf['username'] = upshift_conf['user']
            psi_conf['password'] = upshift_conf['password']
            psi_conf['project_id'] = upshift_conf['project_id']
            psi_conf['user_domain_name'] = upshift_conf['domain']['name']

        def destroy(self):
            self.flexy_instance.destroy()
