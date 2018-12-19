import logging
import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utils import FsUtils

logger = logging.getLogger(__name__)
log = logger


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        tc = '11298'
        source_dir = '/mnt/source'
        target_dir = 'target'
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        client_info, rc = fs_util.get_clients()
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client1 = []
        client2 = []
        client3 = []
        client4 = []
        client1.append(client_info['fuse_clients'][0])
        client2.append(client_info['fuse_clients'][1])
        client3.append(client_info['kernel_clients'][0])
        client4.append(client_info['kernel_clients'][1])
        rc1 = fs_util.auth_list(client1)
        rc2 = fs_util.auth_list(client2)
        rc3 = fs_util.auth_list(client3)
        rc4 = fs_util.auth_list(client4)
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
        for client in client_info['clients']:
            client.exec_command(cmd='sudo rm -rf  %s' % source_dir)
            client.exec_command(cmd='sudo mkdir %s' % source_dir)

        for client in client_info['clients']:
            client.exec_command(
                cmd='sudo mkdir %s%s' %
                    (client_info['mounting_dir'], target_dir))
            break
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                source_dir,
                '',
                0,
                100,
                iotype='touch')
            p.spawn(fs_util.read_write_IO, client1,
                    source_dir, 'g', 'write')
            p.spawn(
                fs_util.stress_io,
                client2,
                source_dir,
                '',
                0,
                10,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                client3,
                source_dir,
                '',
                0,
                10,
                iotype='crefi')
            p.spawn(
                fs_util.stress_io,
                client4,
                source_dir,
                '',
                0,
                1,
                iotype='fio')
            for op in p:
                return_counts1, rc = op

        with parallel() as p:
            p.spawn(fs_util.rsync, client1, source_dir, '%s%s' %
                    (client_info['mounting_dir'], target_dir))
            p.spawn(fs_util.rsync, client2, source_dir, '%s%s' %
                    (client_info['mounting_dir'], target_dir))
            p.spawn(fs_util.rsync, client3, source_dir, '%s%s' %
                    (client_info['mounting_dir'], target_dir))
            p.spawn(fs_util.rsync, client4, source_dir, '%s%s' %
                    (client_info['mounting_dir'], target_dir))
            for op in p:
                return_counts2, rc = op

        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                target_dir,
                0,
                100,
                iotype='touch')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                target_dir,
                0,
                11,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                target_dir,
                0,
                3,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                target_dir,
                0,
                1,
                iotype='fio')
            for op in p:
                return_counts3, rc = op
        with parallel() as p:
            p.spawn(fs_util.rsync, client1, '%s%s/*' %
                    (client_info['mounting_dir'], target_dir), source_dir)
            p.spawn(fs_util.rsync, client2, '%s%s/*' %
                    (client_info['mounting_dir'], target_dir), source_dir)
            p.spawn(fs_util.rsync, client3, '%s%s/*' %
                    (client_info['mounting_dir'], target_dir), source_dir)
            p.spawn(fs_util.rsync, client4, '%s%s/*' %
                    (client_info['mounting_dir'], target_dir), source_dir)
            for op in p:
                return_counts4, rc = op

        rc = return_counts1.values() + return_counts2.values() + return_counts3.values() + return_counts4.values()
        rc_set = set(rc)
        if len(rc_set) == 1:
            print "Test case CEPH-%s passed" % (tc)
        else:
            print("Test case CEPH-%s failed" % (tc))
        log.info("Test completed for CEPH-%s" % (tc))
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
            return 1
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
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            fs_util.client_clean_up(client_info['fuse_clients'],
                                    client_info['kernel_clients'],
                                    client_info['mounting_dir'], 'umount')
        else:
            fs_util.client_clean_up(client_info['fuse_clients'],
                                    '',
                                    client_info['mounting_dir'], 'umount')
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
