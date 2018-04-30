import ast
import time
import random
import string
import logging
import json

log = logging.getLogger(__name__)


class RbdMirror:
    def __init__(self, cluster, cluster_name):

        self.rbd_client = 'client.admin'
        self.cluster_name = cluster_name if cluster_name else 'ceph'
        self.ceph_nodes = cluster
        self.cluster_spec = self.rbd_client + '@' + self.cluster_name
        self.ceph_args = ' --cluster {}'.format(self.cluster_name)

        # Identifying Monitor And Client node
        for node in self.ceph_nodes:
            if node.role == 'mon':
                self.ceph_mon = node
                continue
            if node.role == 'client':
                self.ceph_client = node
                continue

    def exec_cmd(self, **kw):
        cmd = kw.get('cmd')
        node = kw.get('node') if kw.get('node') else self.ceph_client

        if kw.get('ceph_args', True):
            cmd = cmd + self.ceph_args

        out, err = node.exec_command(
            sudo=True, cmd=cmd, long_running=kw.get('long_running', False),
            check_ec=kw.get('check_ec', True))

        if kw.get('output', False):
            return out.read()

        if out:
            return 0
        else:
            return 1

    def copy_file(self, file_name, src, dest):
        out, err = src.exec_command(sudo=True, cmd='cat {}'.format(file_name))
        contents = out.read()
        key_file = dest.write_file(sudo=True, file_name=file_name,
                                   file_mode='w')
        key_file.write(contents)
        key_file.flush()

    # Retrieve required details from json output
    def value(self, key, dictionary):
        return str(list(self.find(key, dictionary))[0])

    # Finding required details from json output
    def find(self, key, dictionary):
        for k, v in dictionary.iteritems():
            if k == key:
                yield v
            elif isinstance(v, dict):
                for result in self.find(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in self.find(key, d):
                        yield result

    # Handling of clusters with same name
    def handle_same_name(self, name):
        self.cluster_name = name
        self.cluster_spec = self.rbd_client + '@' + self.cluster_name
        self.ceph_args = ' --cluster {}'.format(self.cluster_name)
        self.exec_cmd(
            ceph_args=False,
            cmd="grep -v 'CLUSTER=ceph' /etc/sysconfig/ceph | tee temp " +
                "&& mv temp /etc/sysconfig/ceph")
        self.exec_cmd(
            ceph_args=False,
            cmd="echo 'CLUSTER={}' | tee -a /etc/sysconfig/ceph"
                .format(name))
        self.exec_cmd(ceph_args=False,
                      cmd="ln -s /etc/ceph/ceph.conf /etc/ceph/{}.conf"
                      .format(name))
        self.exec_cmd(ceph_args=False, node=self.ceph_mon,
                      cmd="ln -s /etc/ceph/ceph.client.admin.keyring " +
                          "/etc/ceph/{}.client.admin.keyring".format(name))

    # Enable, Start or Stop Rbd Mirror Daemon
    def mirror_daemon(self, enable=None, start=None, stop=None, restart=None):

        if enable:
            self.exec_cmd(ceph_args=False,
                          cmd='systemctl enable ceph-rbd-mirror.target')
            self.exec_cmd(ceph_args=False,
                          cmd='systemctl enable ceph-rbd-mirror@admin')
        if start:
            self.exec_cmd(ceph_args=False,
                          cmd='systemctl start ceph-rbd-mirror@admin')
        if stop:
            self.exec_cmd(ceph_args=False,
                          cmd='systemctl stop ceph-rbd-mirror@admin')
        if restart:
            self.exec_cmd(ceph_args=False,
                          cmd='systemctl restart ceph-rbd-mirror@admin')

    # Initial setup of rbd mirroring
    def setup_mirror(self, peer_cluster, **kw):
        self.exec_cmd(ceph_args=False, cmd="yum install -y rbd-mirror")

        # Copy keyring from monitor node to client node
        self.copy_file(file_name='/etc/ceph/{}.client.admin.keyring'
                       .format(self.cluster_name), src=self.ceph_mon,
                       dest=self.ceph_client)

        if 'one-way' not in kw.get('way', ''):
            self.copy_file(file_name='/etc/ceph/{}.conf'
                           .format(self.cluster_name),
                           src=self.ceph_client,
                           dest=peer_cluster.ceph_mon)
            self.copy_file(file_name='/etc/ceph/{}.client.admin.keyring'
                           .format(self.cluster_name),
                           src=self.ceph_client,
                           dest=peer_cluster.ceph_mon)
            self.copy_file(file_name='/etc/ceph/{}.conf'
                           .format(self.cluster_name),
                           src=self.ceph_client,
                           dest=peer_cluster.ceph_client)
            self.copy_file(file_name='/etc/ceph/{}.client.admin.keyring'
                           .format(self.cluster_name),
                           src=self.ceph_client,
                           dest=peer_cluster.ceph_client)

    def config_mirror(self, peer_cluster, **kw):
        poolname = kw.get('poolname')
        mode = kw.get('mode')

        self.mirror_daemon(enable=True, start=True, restart=True)
        peer_cluster.mirror_daemon(enable=True, start=True, restart=True)

        self.enable_mirroring('pool', poolname, mode=mode)
        peer_cluster.enable_mirroring('pool', poolname, mode=mode)

        if 'one-way' in kw.get('way', ''):
            self.peer_add(poolname=poolname,
                          cluster_spec=peer_cluster.cluster_spec)
        else:
            self.peer_add(poolname=poolname,
                          cluster_spec=peer_cluster.cluster_spec)
            peer_cluster.peer_add(poolname=poolname,
                                  cluster_spec=self.cluster_spec)

        if self.mirror_info(poolname, 'peers') is not None and \
                peer_cluster.mirror_info(poolname, 'peers') is not None:
            log.info('Peers were successfully added')

        else:
            log.error('Peers were not added')

        self.wait_for_status(poolname=poolname, health_pattern='OK')
        peer_cluster.wait_for_status(poolname=poolname, health_pattern='OK')

    # Wait for required status
    def wait_for_status(self, **kw):
        for _ in range(0, 30):
            if kw.get('poolname', False):
                if kw.get('health_pattern'):
                    out = self.mirror_status('pool', kw.get('poolname'),
                                             'health')
                    log.info('Health of {} pool in {} cluster: {}'
                             .format(kw.get('poolname'),
                                     self.cluster_name, out))
                    if kw.get('health_pattern') in out:
                        return 0
                if kw.get('images_pattern'):
                    out = self.mirror_status('pool', kw.get('poolname'),
                                             'states')
                    out = ast.literal_eval(out)
                    state_pattern = kw.get('state', 'total')
                    num_image = 0
                    if 'total' in state_pattern:
                        for k, v in out.iteritems():
                            num_image = num_image + v
                    else:
                        num_image = out[state_pattern]
                    log.info(
                        'Images in {} pool in {} cluster {}: {}'
                        .format(kw.get('poolname'), self.cluster_name,
                                state_pattern, num_image))
                    if kw.get('images_pattern') == num_image:
                        return 0
            else:
                if kw.get('state_pattern'):
                    out = self.mirror_status('image', kw.get('imagespec'),
                                             'state')
                    log.info('State of {} image in {} cluster: {}'
                             .format(kw.get('imagespec'),
                                     self.cluster_name, out))
                    if kw.get('state_pattern') in out:
                        return 0
                if kw.get('description_pattern'):
                    out = self.get_position(
                        imagespec=kw.get('imagespec'),
                        pattern=kw.get('description_pattern'))
                    log.info('Description of {} image in {} cluster: {}'
                             .format(kw.get('imagespec'),
                                     self.cluster_name, out))
                    return out
                    if out:
                        return out
            time.sleep(20)
        log.error('Required status can not be attained')
        return 1

    # Wait for replay to complete, check every 60 seconds
    def wait_for_replay_complete(self, imagespec):
        while 1:
            out = self.wait_for_status(imagespec=imagespec,
                                       description_pattern='entries')
            if int(out.split('=')[-1]) == 0:
                return 0
            time.sleep(60)

    # Get Position
    def get_position(self, imagespec, pattern=None):
        out = self.mirror_status('image', imagespec, 'description')
        if pattern is not None:
            master_pos = out.find('master_position')
            mirror_pos = out.find('mirror_position')
            entries_behind = out.find('entries')
            pos = [out[master_pos: mirror_pos - 2],
                   out[mirror_pos: entries_behind - 2],
                   out[entries_behind:]]
            if 'master' in pattern:
                return pos[0]
            elif 'mirror' in pattern:
                return pos[1]
            else:
                return pos[2]
        else:
            return out

    # Check data consistency
    def check_data(self, peercluster, imagespec):
        self.wait_for_status(imagespec=imagespec, state_pattern='up+stopped')
        peercluster.wait_for_status(imagespec=imagespec,
                                    state_pattern='up+replaying')
        peercluster.wait_for_replay_complete(imagespec)
        export_path = '/home/cephuser/image.export'

        self.export_image(imagespec=imagespec, path=export_path)
        peercluster.export_image(imagespec=imagespec, path=export_path)

        time.sleep(5)
        local_md5 = self.exec_cmd(
            ceph_args=False, output=True,
            cmd='md5sum {}'.format(export_path))
        rmt_md5 = peercluster.exec_cmd(
            ceph_args=False, output=True,
            cmd='md5sum {}'.format(export_path))
        print local_md5
        print rmt_md5
        if local_md5 == rmt_md5:
            log.info('Data is consistent')
            self.exec_cmd(ceph_args=False, cmd='rm -f {}'.format(export_path))
            peercluster.exec_cmd(ceph_args=False,
                                 cmd='rm -f {}'.format(export_path))
            return 0
        else:
            log.error('Data Inconsistency found')
            return 1

    # CLIs
    def benchwrite(self, **kw):
        self.exec_cmd(
            cmd='rbd bench {} --io-type write --io-threads 16 --io-total {} '
                '--io-pattern rand'.format(kw.get('imagespec'),
                                           kw.get('io', '500M')),
            long_running=True)

    def create_pool(self, **kw):
        self.exec_cmd(cmd='ceph osd pool create {} 64 64'
                      .format(kw.get('poolname')))

    def create_image(self, **kw):
        self.exec_cmd(cmd='rbd create -s {} {} '
                      .format(kw.get('size', '2G'), kw.get('imagespec')) +
                      '--image-feature exclusive-lock,journaling')

    def export_image(self, **kw):
        self.exec_cmd(cmd='rbd export {} {}'.
                      format(kw.get('imagespec'), kw.get('path')),
                      long_running=True)

    # Enable Pool or Image Mirroring
    def enable_mirroring(self, *args, **kw):
        self.exec_cmd(cmd='rbd mirror {} enable {} {}'
                      .format(args[0], args[1], kw.get('mode', '')))

    # Disable Pool or Image Mirroring
    def disable_mirroring(self, *args):
        self.exec_cmd(cmd='rbd mirror {} disable {}'.format(args[0], args[1]))

    # Mirroring Info
    def mirror_info(self, *args):
        output = self.exec_cmd(output=True,
                               cmd='rbd mirror pool info {} --format=json'.
                               format(args[0]))
        json_dict = json.loads(output)
        return self.value(args[1], json_dict)

    # Mirroring Status
    def mirror_status(self, *args):
        output = self.exec_cmd(output=True,
                               cmd="rbd mirror {} status {} --format=json"
                               .format(args[0], args[1]))
        json_dict = json.loads(output)
        return self.value(args[2], json_dict)

    # Add Peer
    def peer_add(self, **kw):
        return self.exec_cmd(cmd='rbd mirror pool peer add {} {}'
                             .format(kw.get('poolname'),
                                     kw.get('cluster_spec')))

    # Remove Peer
    def peer_remove(self, **kw):

        peer_uuid = self.mirror_info(kw.get('poolname'), 'uuid')
        return self.exec_cmd(cmd='rbd mirror pool peer remove {} {}'
                             .format(kw.get('poolname'), peer_uuid))

    # Promote Image
    def promote(self, **kw):
        if kw.get('force'):
            return self.exec_cmd(output=True,
                                 cmd='rbd mirror image promote --force {}'
                                 .format(kw.get('imagespec')))
        else:
            return self.exec_cmd(output=True,
                                 cmd='rbd mirror image promote {}'
                                 .format(kw.get('imagespec')))

    # Demote Image
    def demote(self, imagespec):
        return self.exec_cmd(cmd='rbd mirror image demote {}'
                             .format(imagespec))

    # Demote Image
    def resync(self, imagespec):
        self.exec_cmd(cmd='rbd mirror image resync {}'.format(imagespec))

    def random_string(self):
        temp_str = ''.join(
            [random.choice(string.ascii_letters) for _ in xrange(10)])
        return temp_str

    def delete_pool(self, poolname):
        self.exec_cmd(cmd='ceph osd pool delete {pool} {pool} '
                          '--yes-i-really-really-mean-it'.format(pool=poolname))

    def delete_image(self, imagespec):
        self.exec_cmd(cmd='rbd rm {}'.format(imagespec))
