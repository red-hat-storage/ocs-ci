from tests.cephfs.cephfs_utils import FsUtils
from ceph.ceph import CommandFailed
import traceback
import logging
from ceph.parallel import parallel
logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        tc = 'nfs-ganesha'
        nfs_mounting_dir = '/mnt/nfs_mount/'
        log.info("Running cephfs %s test case" % (tc))
        ceph_nodes = kw.get('ceph_nodes')
        fs_util = FsUtils(ceph_nodes)
        client_info, rc = fs_util.get_clients()
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        nfs_server = client_info['kernel_clients'][0]
        nfs_client = [client_info['kernel_clients'][1]]
        client1 = [client_info['fuse_clients'][0]]
        client2 = [client_info['fuse_clients'][1]]
        client3 = [client_info['kernel_clients'][0]]
        client4 = [client_info['kernel_clients'][1]]
        rc1 = fs_util.auth_list(client1, client_info['mon_node'])
        rc2 = fs_util.auth_list(client2, client_info['mon_node'])
        rc3 = fs_util.auth_list(client3, client_info['mon_node'])
        rc4 = fs_util.auth_list(client4, client_info['mon_node'])
        print rc1, rc2, rc3, rc4
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            raise CommandFailed("auth list failed")

        rc1 = fs_util.fuse_mount(client1, client_info['mounting_dir'])
        rc2 = fs_util.fuse_mount(client2, client_info['mounting_dir'])

        if rc1 == 0 and rc2 == 0:
            log.info("Fuse mount passed")
        else:
            raise CommandFailed("Fuse mount failed")

        rc3 = fs_util.kernel_mount(
            client3,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])
        rc4 = fs_util.kernel_mount(
            client4,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])
        if rc3 == 0 and rc4 == 0:
            log.info("kernel mount passed")
        else:
            raise CommandFailed("kernel mount failed")
        dirs, rc = fs_util.mkdir(
            client1, 0, 4, client_info['mounting_dir'], 'dir')
        if rc == 0:
            log.info("Directories created")
        else:
            raise CommandFailed("Directory creation failed")
        dirs = dirs.split('\n')

        rc = fs_util.nfs_ganesha_install(nfs_server)
        if rc == 0:
            log.info('NFS ganesha installed successfully')
        else:
            raise CommandFailed('NFS ganesha installation failed')
        rc = fs_util.nfs_ganesha_conf(nfs_server, 'admin')
        if rc == 0:
            log.info('NFS ganesha config added successfully')
        else:
            raise CommandFailed('NFS ganesha config adding failed')
        rc = fs_util.nfs_ganesha_mount(
            nfs_client[0],
            nfs_mounting_dir,
            nfs_server.hostname)
        if rc == 0:
            log.info('NFS-ganesha mount passed')
        else:
            raise CommandFailed('NFS ganesha mount failed')
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                nfs_client,
                nfs_mounting_dir + 'ceph/',
                dirs[0],
                0,
                5,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                nfs_client,
                nfs_mounting_dir + 'ceph/',
                dirs[2],
                0,
                5,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                nfs_client,
                nfs_mounting_dir + 'ceph/',
                dirs[1],
                0,
                1,
                iotype='crefi')
            p.spawn(
                fs_util.stress_io,
                nfs_client,
                nfs_mounting_dir + 'ceph/',
                dirs[3],
                0,
                1,
                iotype='smallfile_create', fnum=1000, fsize=1024)

        for client in nfs_client:
            log.info('Unmounting nfs-ganesha mount on client:')
            client.exec_command(cmd='sudo umount %s -l' % (nfs_mounting_dir))
            log.info('Removing nfs-ganesha mount dir on client:')
            client.exec_command(cmd='sudo rm -rf  %s' % (nfs_mounting_dir))

        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            rc = fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'],
                'umount')
        else:
            rc = fs_util.client_clean_up(
                client_info['fuse_clients'],
                '',
                client_info['mounting_dir'],
                'umount')
        if rc == 0:
            log.info('Cleaning up successfull')
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
