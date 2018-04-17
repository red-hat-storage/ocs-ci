import traceback
from tests.cephfs.cephfs_utils import FsUtils
import timeit
from ceph.ceph import CommandFailed
import logging
import random
import string

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        start = timeit.default_timer()
        ceph_nodes = kw.get('ceph_nodes')
        fs_util = FsUtils(ceph_nodes)
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
        tc1 = '11293'
        tc2 = '11296'
        tc3 = '11297'
        tc4 = '11295'
        dir1 = ''.join(
            random.choice(
                string.lowercase +
                string.digits) for _ in range(10))
        dir2 = ''.join(
            random.choice(
                string.lowercase +
                string.digits) for _ in range(10))
        dir3 = ''.join(
            random.choice(
                string.lowercase +
                string.digits) for _ in range(10))
        results = []
        return_counts = []
        log.info("Create files and directories of 1000 depth and 1000 breadth")
        for node in client_info['fuse_clients']:
            node.exec_command(
                cmd='sudo mkdir %s%s' %
                (client_info['mounting_dir'], dir1))
            node.exec_command(
                cmd='sudo mkdir %s%s' %
                (client_info['mounting_dir'], dir2))
            node.exec_command(
                cmd='sudo mkdir %s%s' %
                (client_info['mounting_dir'], dir3))
            log.info('Execution of testcase %s started' % tc1)
            out, rc = node.exec_command(
                cmd='sudo crefi %s%s --fop create --multi -b 1000 -d 1000 '
                    '-n 1 -T 5 --random --min=1K --max=10K' %
                (client_info['mounting_dir'], dir1), long_running=True)
            log.info('Execution of testcase %s ended' % tc1)
            if node.exit_status == 0:
                results.append("TC %s passed" % tc1)

            log.info('Execution of testcase %s started' % tc2)
            node.exec_command(
                cmd='sudo cp -r  %s%s/* %s%s/' %
                (client_info['mounting_dir'], dir1,
                 client_info['mounting_dir'], dir2))
            node.exec_command(
                cmd="diff -qr  %s%s %s%s/" %
                (client_info['mounting_dir'], dir1,
                 client_info['mounting_dir'], dir2))
            log.info('Execution of testcase %s ended' % tc2)
            if node.exit_status == 0:
                results.append("TC %s passed" % tc2)

            log.info('Execution of testcase %s started' % tc3)
            out, rc = node.exec_command(
                cmd='sudo mv  %s%s/* %s%s/' %
                (client_info['mounting_dir'], dir1,
                 client_info['mounting_dir'], dir3))
            log.info('Execution of testcase %s ended' % tc3)
            if node.exit_status == 0:
                results.append("TC %s passed" % tc3)
            log.info('Execution of testcase %s started' % tc4)
            for client in client_info['clients']:
                if client.pkg_type != 'deb':
                    client.exec_command(
                        cmd='sudo dd if=/dev/zero of=%s%s.txt bs=100M '
                            'count=5' %
                        (client_info['mounting_dir'], client.hostname))
                    out1, rc1 = client.exec_command(
                        cmd='sudo  ls -c -ltd -- %s%s.*' %
                        (client_info['mounting_dir'], client.hostname))
                    client.exec_command(
                        cmd='sudo dd if=/dev/zero of=%s%s.txt bs=200M '
                            'count=5' %
                        (client_info['mounting_dir'], client.hostname))
                    out2, rc2 = client.exec_command(
                        cmd='sudo  ls -c -ltd -- %s%s.*' %
                        (client_info['mounting_dir'], client.hostname))
                    a = out1.read()
                    print "------------"
                    b = out2.read()
                    if a != b:
                        return_counts.append(out1.channel.recv_exit_status())
                        return_counts.append(out2.channel.recv_exit_status())
                    else:
                        raise CommandFailed("Metadata info command failed")
                    break
            log.info('Execution of testcase %s ended' % tc4)
            print return_counts
            rc_set = set(return_counts)
            if len(rc_set) == 1:
                results.append("TC %s passed" % tc4)

            print "Testcase Results:"
            for res in results:
                print res
            break
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
            rc = fs_util.client_clean_up(client_info['fuse_clients'],
                                         client_info['kernel_clients'],
                                         client_info['mounting_dir'], 'umount')
        else:
            rc = fs_util.client_clean_up(client_info['fuse_clients'],
                                         '',
                                         client_info['mounting_dir'], 'umount')
        if rc == 0:
            log.info('Cleaning up successfull')
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
