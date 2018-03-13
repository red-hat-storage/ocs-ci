from tests.cephfs.cephfs_utils import FsUtils
from ceph.parallel import parallel
import timeit
import traceback
from ceph.ceph import CommandFailed
import logging

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        start = timeit.default_timer()
        tc = '10528'
        log.info('Running cephfs %s test case' % tc)
        ceph_nodes = kw.get('ceph_nodes')
        fs_util = FsUtils(ceph_nodes)
        client_info, rc = fs_util.get_clients()
        if rc == 0:
            log.info('Got client info')
        else:
            raise CommandFailed('fetching client info failed')
        client1 = []
        client2 = []
        client3 = []
        client4 = []
        client1.append(client_info['fuse_clients'][0])
        client2.append(client_info['fuse_clients'][1])
        client3.append(client_info['kernel_clients'][0])
        client4.append(client_info['kernel_clients'][1])

        rc1 = fs_util.auth_list(client1, client_info['mon_node'])
        rc2 = fs_util.auth_list(client2, client_info['mon_node'])
        rc3 = fs_util.auth_list(client3, client_info['mon_node'])
        rc4 = fs_util.auth_list(client4, client_info['mon_node'])
        print rc1, rc2, rc3, rc4
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

        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'g', 'write')
            p.spawn(fs_util.read_write_IO, client2,
                    client_info['mounting_dir'], 'g', 'read')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                '',
                0,
                2,
                iotype='crefi',
            )
            p.spawn(fs_util.read_write_IO, client4,
                    client_info['mounting_dir'], 'g', 'readwrite')
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'])
            for op in p:
                (return_counts, rc) = op

        log.info('Test completed for CEPH-%s' % tc)
        print 'Results:'
        result = fs_util.rc_verify(tc, return_counts)
        print result

        tc = '10529'
        log.info('Test for CEPH-%s will start:' % tc)
        md5sum_file_lock = []
        with parallel() as p:
            p.spawn(fs_util.file_locking, client1[0],
                    client_info['mounting_dir'])
            p.spawn(fs_util.file_locking, client3[0],
                    client_info['mounting_dir'])
            for output in p:
                md5sum_file_lock = output

        if 0 in md5sum_file_lock:
            log.info('file locking success')
        else:
            raise CommandFailed('file locking failed')

        if len(md5sum_file_lock) == 2:
            log.info('File Locking mechanism is working,data is not corrupted,'
                     'test case CEPH-%s passed' % (tc))
        else:
            log.error(
                'File Locking mechanism is failed,data is corrupted,'
                'test case CEPH-%s failed' % (tc))

        log.info('Test completed for CEPH-%s' % (tc))
        log.info('Cleaning up!-----')
        rc = fs_util.client_clean_up(client1,
                                     client_info['kernel_clients'],
                                     client_info['mounting_dir'], 'umount')
        if rc == 0:
            log.info('Cleaning up successfull')
        else:
            raise CommandFailed('Cleanup failed')
        print 'Script execution time:------'
        stop = timeit.default_timer()
        total_time = stop - start
        (mins, secs) = divmod(total_time, 60)
        (hours, mins) = divmod(mins, 60)

        print 'Hours:%d Minutes:%d Seconds:%f' % (hours, mins, secs)

        return 0
    except CommandFailed as e:

        log.info(e)
        log.info(traceback.format_exc())
        return 1
    except Exception as e:

        log.info(e)
        log.info(traceback.format_exc())
        return 1
