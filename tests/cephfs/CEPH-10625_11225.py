import logging
import random
import string
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
        tc = '10625,11225'
        dir_name = 'dir'
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        client_info, rc = fs_util.get_clients()
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client1, client2, client3, client4 = ([] for _ in range(4))
        client1.append(client_info['fuse_clients'][0])
        client2.append(client_info['fuse_clients'][1])
        client3.append(client_info['kernel_clients'][0])
        client4.append(client_info['kernel_clients'][1])
        rc1 = fs_util.auth_list(client1)
        rc2 = fs_util.auth_list(client2)
        rc3 = fs_util.auth_list(client3)
        rc4 = fs_util.auth_list(client4)
        print(rc1, rc2, rc3, rc4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            log.error("auth list failed")
            return 1
        rc1 = fs_util.fuse_mount(client1, client_info['mounting_dir'])
        rc2 = fs_util.fuse_mount(client2, client_info['mounting_dir'])

        if rc1 == 0 and rc2 == 0:
            log.info("Fuse mount passed")
        else:
            log.error("Fuse mount failed")
            return 1
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
            log.error("kernel mount failed")
            return 1
        rc = fs_util.activate_multiple_mdss(client_info['mds_nodes'])
        if rc == 0:
            log.info("Activate multiple mdss successfully")
        else:
            log.error("Activate multiple mdss failed")
            return 1
        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'g', 'write')
            p.spawn(fs_util.read_write_IO, client2,
                    client_info['mounting_dir'], 'g', 'read')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                '',
                0,
                1,
                iotype='crefi',
            )
            p.spawn(fs_util.read_write_IO, client4,
                    client_info['mounting_dir'], 'g', 'readwrite')
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'])
            for op in p:
                return_counts, rc = op
        result = fs_util.rc_verify('', return_counts)
        if result == 'Data validation success':
            dirs, rc = fs_util.mkdir(
                client1, 0, 6, client_info['mounting_dir'], dir_name)
            if rc == 0:
                log.info("Directories created")
            else:
                raise CommandFailed("Directory creation failed")
            dirs = dirs.split('\n')
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1000, fsize=10)
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info['mounting_dir'],
                    dirs[1],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1000, fsize=10)
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[2],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1000, fsize=10)
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info['mounting_dir'],
                    dirs[2],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1000, fsize=10)

            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='smallfile_rename', fnum=1000, fsize=10)
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[0],
                    0,
                    1,
                    iotype='smallfile_delete-renamed', fnum=1000, fsize=10)
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info['mounting_dir'],
                    dirs[2],
                    0,
                    1,
                    iotype='smallfile_delete', fnum=1000, fsize=10)
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[3],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1, fsize=1000000)
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info['mounting_dir'],
                    dirs[4],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1, fsize=1000000)
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dirs[5],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1, fsize=1000000)
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info['mounting_dir'],
                    dirs[6],
                    0,
                    1,
                    iotype='smallfile_create', fnum=1, fsize=1000000)

            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[3],
                    0,
                    1,
                    iotype='smallfile_rename', fnum=1, fsize=1000000)
            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dirs[3],
                    0,
                    1,
                    iotype='smallfile_delete-renamed', fnum=1, fsize=1000000)
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info['mounting_dir'],
                    dirs[4],
                    0,
                    1,
                    iotype='smallfile_delete', fnum=1, fsize=1000000)
        dir_name = '!@#$%^&*()-_=+[]{};:,.<>?'
        out, rc = client1[0].exec_command(
            cmd="sudo mkdir '%s%s'" %
                (client_info['mounting_dir'], dir_name))
        if client1[0].node.exit_status == 0:
            log.info("Directory created")
        else:
            raise CommandFailed("Directory creation failed")
        for client in client_info['fuse_clients']:
            file_name = ''.join(
                random.choice(
                    string.ascii_lowercase
                    + string.digits) for _ in range(255))
            client.exec_command(
                cmd="sudo touch '%s%s/%s'" %
                    (client_info['mounting_dir'], dir_name, file_name))
        for client in client_info['kernel_clients']:
            if client.pkg_type == 'rpm':
                file_name = ''.join(
                    random.choice(
                        string.ascii_lowercase
                        + string.digits) for _ in range(255))
                client.exec_command(
                    cmd="sudo touch '%s%s/%s'" %
                        (client_info['mounting_dir'], dir_name, file_name))
        for num in range(0, 5):
            for client in client_info['fuse_clients']:
                client.exec_command(
                    cmd="sudo crefi %s'%s' --fop create -t %s "
                        "--multi -b 10 -d 10 -n 10 -T 10 "
                        "--random --min=1K --max=%dK" %
                        (client_info['mounting_dir'], dir_name, 'text',
                         5), long_running=True)
                for i in range(0, 6):
                    ops = [
                        'create',
                        'rename',
                        'chmod',
                        'chown',
                        'chgrp',
                        'setxattr']
                    rand_ops = random.choice(ops)
                    ftypes = ['text', 'sparse', 'binary', 'tar']
                    rand_filetype = random.choice(ftypes)
                    rand_count = random.randint(2, 10)
                    client.exec_command(
                        cmd='sudo crefi %s%s --fop %s -t %s '
                            '--multi -b 10 -d 10 -n 10 -T 10 '
                            '--random --min=1K --max=%dK' %
                            (client_info['mounting_dir'], dir_name, rand_ops,
                             rand_filetype, rand_count),
                        long_running=True)
        log.info('Cleaning up!-----')
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
        else:
            return 1
        log.info("Execution of Test cases CEPH-%s ended:" % (tc))
        print('Script execution time:------')
        stop = timeit.default_timer()
        total_time = stop - start
        mins, secs = divmod(total_time, 60)
        hours, mins = divmod(mins, 60)
        print("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))
        return 0
    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        log.info('Cleaning up!-----')
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
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
