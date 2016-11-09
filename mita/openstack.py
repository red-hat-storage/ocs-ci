import os
import logging
import socket
from time import sleep

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from ssl import SSLError
import libcloud.security

logger = logging.getLogger(__name__)

# libcloud does not have a timeout enabled for Openstack calls to
# ``create_node``, and it uses the default timeout value from socket which is
# ``None`` (meaning: it will wait forever). This setting will set the default
# to a magical number, which is 280 (4 minutes). This is 1 minute less than the
# timeouts for production settings that should allow enough time to handle the
# exception and return a response
socket.setdefaulttimeout(280)

# FIXME
# At the time this example was written, https://nova-api.trystack.org:5443
# was using a certificate issued by a Certificate Authority (CA) which is
# not included in the default Ubuntu certificates bundle (ca-certificates).
# Note: Code like this poses a security risk (MITM attack) and that's the
# reason why you should never use it for anything else besides testing. You
# have been warned.
libcloud.security.VERIFY_SSL_CERT = False

OpenStack = get_driver(Provider.OPENSTACK)


class CephVMNode(object):
    
    def __init__(self, **kw):
        self.image_name = kw['image-name']
        self.node_name = kw['node-name']
        self.vm_size = kw['vm-size']
        self.role = kw['role']
        self.no_of_volumes = None
        if self.role == 'osd':
            self.no_of_volumes = kw['no-of-volumes']
            self.size_of_disk = kw['size-of-disks']
        self.cd_file = os.path.abspath(kw['cloud-data'])
        with open(self.cd_file) as cd:
            self.cloud_data = cd.read()
        self.username =  kw['username']
        self.password = kw['password']
        self.auth_url = kw['auth-url']
        self.auth_version = kw['auth-version']
        self.tenant_name = kw['tenant-name']
        self.service_region = kw['service-region']
        self.keypair = kw['keypair']
        self.root_login = kw['root-login']
        self.create_node()
        sleep(10)


    def get_driver(self, **kw):
        self.driver = OpenStack(
                self.username,
                self.password,
                ex_force_auth_url=self.auth_url,
                ex_force_auth_version=self.auth_version,
                ex_tenant_name=self.tenant_name,
                ex_force_service_region=self.service_region
            )
        return self.driver
    
    
    def create_node(self, **kw):
        name = self.node_name
        driver = self.get_driver()
        images = driver.list_images()
        sizes = driver.list_sizes()
        available_sizes = [s for s in sizes if s.name == self.vm_size]
        if not available_sizes:
            logger.error("provider does not have a matching 'size' for %s", self.vm_size)
            logger.error(
                "no vm will be created. Ensure that '%s' is an available size and that it exists",
                self.vm_size
            )
            return  
        vm_size = available_sizes[0]
        image = [i for i in images if i.name == self.image_name][0]
    
        try:
            new_node = driver.create_node(
                name=name, image=image, size=vm_size,
                ex_userdata=self.cloud_data, ex_keyname=self.keypair
            )
        except SSLError:
            new_node = None
            logger.error("failed to connect to provider, probably a timeout was reached")
    
        if not new_node:
            logger.error("provider could not create node with details: %s", str(kw))
            return
        self.node = new_node
        logger.info("created node: %s", new_node)
        # wait for the new node to become available
        logger.info("Waiting for node %s to become available", name)
        driver.wait_until_running([new_node])
        logger.info(" ... available")
        logger.info("Attaching floating ip")
        self.attach_floating_ip()
        self.volumes = []
        if self.role == 'osd':
            total_vols = self.no_of_volumes
            size = self.size_of_disk
            for vol in range(0, total_vols):
                name = self.node_name + str(vol)
                logger.info("Creating %sgb of storage for: %s", size, name)
                new_volume = driver.create_volume(size, name)
                # wait for the new volume to become available
                logger.info("Waiting for volume %s to become available", name)
                self._wait_until_volume_available(new_volume, maybe_in_use=True)
                logger.info("Attaching volume %s...", name)
                if driver.attach_volume(new_node, new_volume, device=None) is not True:
                    raise RuntimeError("Could not attach volume %s" % name)
                logger.info("Successfully attached volume %s", name)
                self.volumes.append(new_volume)
    
    
    def _wait_until_volume_available(self, volume, maybe_in_use=False):
        """
        Wait until a StorageVolume's state is "available".
        Set "maybe_in_use" to True in order to wait even when the volume is
        currently in_use. For example, set this option if you're recycling
        this volume from an old node that you've very recently
        destroyed.
        """
        ok_states = ['creating']  # it's ok to wait if the volume is in this
        tries = 0
        if maybe_in_use:
            ok_states.append('in_use')
        logger.info('Volume: %s is in state: %s', volume.name, volume.state)
        while volume.state in ok_states:
            sleep(3)
            volume = self.get_volume(volume.name)
            tries = tries + 1
            if tries > 10:
                logger.info("Maximum amount of tries reached..")
                break
            if volume.state == 'notfound':
                logger.error('no volume was found for: %s', volume.name)
                break
            logger.info(' ... %s', volume.state)
        if volume.state != 'available':
            # OVH uses a non-standard state of 3 to indicate an available volume
            logger.info('Volume %s is %s (not available)', volume.name, volume.state)
            logger.info('The volume %s is not available, but will continue anyway...', volume.name)
        return True


    def get_volume(self, name):
        """ Return libcloud.compute.base.StorageVolume """
        driver = self.driver
        volumes = driver.list_volumes()
        try:
            return [v for v in volumes if v.name == name][0]
        except IndexError:
            raise RuntimeError("Unable to get volume")
    

    def destroy_node(self):
        """
        Relies on the fact that names **should be** unique. Along the chain we
        prevent non-unique names to be used/added.
        TODO: raise an exception if more than one node is matched to the name, that
        can be propagated back to the client.
        """
        driver = self.driver
        driver.ex_detach_floating_ip_from_node(self.node, self.floating_ip)
        driver.destroy_node(self.node)
        sleep(15)
        for volume in self.volumes:
            driver.destroy_volume(volume)
    

    def destroy_volume(self, name):
        driver = self.driver
        volume = get_volume(name)
        # check to see if this is a valid volume
        if volume.state != "notfound":
            logger.info("Destroying volume %s", name)
            driver.destroy_volume(volume)


    def attach_floating_ip(self):
        driver = self.driver
        pool = driver.ex_list_floating_ip_pools()[0]
        self.floating_ip = pool.create_floating_ip()
        self.ip_address = self.floating_ip.ip_address
        count = 0
        host = None
        while True:
            try:
                count += 1
                host, _, _ = socket.gethostbyaddr(self.ip_address)
            except:
                if count > 3:
                    logger.info("Failed to get hostbyaddr in 3 retries")
                    break
                else:
                    logger.info("Retrying gethostbyaddr in 10 seconds")
                    sleep(10)
            if host is not None:
                break
        self.hostname = host
        logger.info("ip: %s and hostname: %s", self.ip_address, self.hostname)
        driver.ex_attach_floating_ip_to_node(self.node, self.floating_ip)
        sleep(10)

