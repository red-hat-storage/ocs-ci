"""
A module for all PSI-openstack related utilities
"""

import logging

from cinderclient import client as cinderc
from cinderclient import exceptions as cinderexception
from keystoneauth1 import loading, session
from novaclient import client as novac

from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class PSIUtils(object):
    """
    A class for handling PSI functionalities
    """
    def __init__(self, psi_conf):
        self.auth_url = psi_conf['auth_url']
        self.username = psi_conf['username']
        self.password = psi_conf['password']
        self.project_id = psi_conf['project_id']
        self.user_domain_name = psi_conf['user_domain_name']

        self.loader = loading.get_plugin_loader('password')
        self.auth = self.loader.load_from_options(
            auth_url=self.auth_url,
            username=self.username,
            password=self.password,
            project_id=self.project_id,
            user_domain_name=self.user_domain_name
        )
        self.sess = session.Session(auth=self.auth)
        self.nova_clnt = novac.Client(
            constants.NOVA_CLNT_VERSION, session=self.sess
        )
        self.cinder_clnt = cinderc.Client(
            constants.CINDER_CLNT_VERSION, session=self.sess
        )

    def create_volume(
        self, name, size=100, volume_type='tripleo', metadata=None
    ):
        """
        A function to create openstack volumes

        Args:
            name (str): display name of the volume
            size (int): of the volume in GBs
            volume_type (str): type of volume to be created
            metadata (dict): Any {k,v} to be associated with volume

        Returns:
           Volume : cinderclient.Client.Volumes

        """
        cluster_tag = {'cluster_name': config.ENV_DATA['cluster_name']}
        meta_data = metadata if metadata else cluster_tag

        vol = self.cinder_clnt.volumes.create(
            name=name,
            size=size,
            volume_type=volume_type,
            metadata=meta_data
        )
        if not vol:
            raise exceptions.PSIVolumeCreationFailed(
                "Failed to create PSI openstack volume"
            )

        return vol

    def attach_volume(self, vol, instance_id):
        """
        Attach the given volume to the specific PSI openstack instance

        Args:
            vol (Volume): cinder volume object
            instance_id (str): uuid of the instance

        """
        self.nova_clnt.volumes.create_server_volume(
            instance_id, vol.id
        )

    def get_instances_with_pattern(self, pattern):
        """
        Get instances matching pattern

        Args:
            pattern (str): Pattern for matching instance name
                note: we are looking for only 'name' param for
                pattern matching

        Returns:
            novaclient.base.ListWithMeta

        """
        return self.nova_clnt.servers.list(
            search_opts={'name': pattern}
        )

    def get_volumes_with_tag(self, tag):
        """
        Get PSI volumes having this tag (k,v)

        Args:
            tag (dict): of desired (k,v)

        Returns:
            list: of cinderclient.<vers>.volumes object

        """
        matching_vols = []
        for vol in self.cinder_clnt.volumes.list():
            for k, v in tag.items():
                if vol.metadata.get(k) == v:
                    matching_vols.append(vol)

        return matching_vols

    def detach_and_delete_vols(self, volumes):
        """
        Detach and delete volumes from the list

        Args:
            volumes (list): of Volume objects

        """
        for v in volumes:
            if v.status == 'in-use':
                v.detach()
                v.get()
                sample = TimeoutSampler(
                    100,
                    5,
                    self.check_expected_vol_status,
                    vol=v,
                    expected_state='available'
                )
                if not sample.wait_for_func_status(True):
                    logger.error(f"Volume {v.name} failed to detach")
                    raise exceptions.PSIVolumeNotInExpectedState()

            v.delete()
            sample = TimeoutSampler(
                100,
                5,
                self.check_vol_deleted,
                vol=v
            )
            if not sample.wait_for_func_status(True):
                logger.error(f"Failed to delete Volume {v.name}")
                raise exceptions.PSIVolumeDeletionFailed()

    def check_vol_deleted(self, vol):
        """
        Check whether its delete

        Args:
            vol (cinderclient.Volume): volume object

        Returns:
            bool: True if deleted else False

        """
        try:
            vol.get()
            return False
        except cinderexception.NotFound:
            logger.info(f"Volume {vol.name} deleted successfully")
            return True

    def check_expected_vol_status(self, vol, expected_state):
        """
        Check status of the volume and return true if it matches
        the expected state

        Args:
            vol (cinderclient.volume): Volume object for which state needs
                to be checked
            expected_state (str): Expected state of the volume

        Returns:
            bool: True if state is same as expected else False

        """
        vol.get()
        return vol.status == expected_state
