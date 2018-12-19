import logging
import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import node_power_failure
from tests.cephfs.cephfs_utils import FsUtils

logger = logging.getLogger(__name__)
log = logger


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        log.info("Running test 11262")
        config = kw.get('config')
        osp_cred = config.get('osp_cred')
        fs_util = FsUtils(ceph_cluster)
        client_info, rc = fs_util.get_clients()
        if rc == 0:
            log.info('Got client info')
        else:
            raise CommandFailed('fetching client info failed')
        client1, client2, client3, client4 = ([] for _ in range(4))
        client1.append(client_info['fuse_clients'][0])
        client2.append(client_info['fuse_clients'][1])
        client3.append(client_info['kernel_clients'][0])
        client4.append(client_info['kernel_clients'][1])
        rc1 = fs_util.auth_list(client1)
        rc2 = fs_util.auth_list(client2)
        rc3 = fs_util.auth_list(client3)
        rc4 = fs_util.auth_list(client4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info('got auth keys')
        else:
            raise CommandFailed('auth list failed')
        rc1 = fs_util.fuse_mount(client1, client_info['mounting_dir'])
        rc2 = fs_util.fuse_mount(client2, client_info['mounting_dir'])
        if rc1 == 0 and rc2 == 0:
            log.info('Fuse mount passed')
        else:
            raise CommandFailed('Fuse mount failed')

        rc3 = fs_util.kernel_mount(
            client3,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])
        rc4 = fs_util.kernel_mount(
            client4,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])
        if rc3 == 0 and rc4 == 0:
            log.info('kernel mount passed')
        else:
            raise CommandFailed('kernel mount failed')
        dir_name = 'dir'
        client1[0].exec_command(
            cmd='sudo mkdir %s%s' %
                (client_info['mounting_dir'], dir_name))
        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'g', 'write')
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'], 'g', 'read')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir_name,
                0,
                50,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                1,
                iotype='smallfile_create', fnum=10000, fsize=100)
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                dir_name,
                0,
                1,
                iotype='crefi')
        for node in client_info['mds_nodes']:
            rc = fs_util.heartbeat_map(node)
            if rc == 0:
                log.info('heartbeat_map entry not found')
            else:
                return 1
        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'g', 'write')
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'], 'g', 'read')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir_name,
                0,
                50,
                iotype='fio')
            for osd in client_info['osd_nodes']:
                node_power_failure(osp_cred, name=osd.hostname)
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                1,
                iotype='smallfile_create', fnum=10000, fsize=100)
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                dir_name,
                0,
                1,
                iotype='crefi')
        log.info('Test completed for CEPH-11262')
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            rc = fs_util.client_clean_up(client_info['fuse_clients'],
                                         client_info['kernel_clients'],
                                         client_info['mounting_dir'], 'umount')
        else:
            rc = fs_util.client_clean_up(client_info['fuse_clients'],
                                         '',
                                         client_info['mounting_dir'], 'umount')
        if rc == 0:
            log.info('Cleaning up successfull')
        else:
            raise CommandFailed('Cleanup failed')
        print'Script execution time:------'
        stop = timeit.default_timer()
        total_time = stop - start
        mins, secs = divmod(total_time, 60)
        hours, mins = divmod(mins, 60)
        print ("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
