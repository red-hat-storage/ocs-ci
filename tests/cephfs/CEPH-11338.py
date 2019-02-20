import logging
import timeit
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utils import FsUtils

logger = logging.getLogger(__name__)
log = logger


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        dir_name = 'dir'
        log.info("Running cephfs 11338 test case")
        fs_util = FsUtils(ceph_cluster)
        client_info, rc = fs_util.get_clients()
        if rc == 0:
            log.info("Got client info")
        else:
            log.error("fetching client info failed")
            return 1
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
        dirs, rc = fs_util.mkdir(
            client1, 1, 3, client_info['mounting_dir'], dir_name)
        if rc == 0:
            log.info("Directories created")
        dirs = dirs.split('\n')
        '''
        new clients with restrictions
        '''
        new_client1_name = client_info['fuse_clients'][0].node.hostname + '_%s' % (dirs[0])
        new_client2_name = client_info['fuse_clients'][1].node.hostname + '_%s' % (dirs[0])
        new_client3_name = client_info['kernel_clients'][0].node.hostname + '_%s' % (dirs[1])
        new_client3_mouting_dir = '/mnt/%s_%s/' % (client_info['kernel_clients'][0].node.hostname, dirs[1])
        new_client2_mouting_dir = '/mnt/%s_%s/' % (client_info['fuse_clients'][1].node.hostname, dirs[0])
        new_client1_mouting_dir = '/mnt/%s_%s/' % (client_info['fuse_clients'][0].node.hostname, dirs[0])
        rc1 = fs_util.auth_list(client1, path=dirs[0], permission='rw', mds=True)
        rc2 = fs_util.auth_list(client2, path=dirs[0], permission='r', mds=True)
        rc3 = fs_util.auth_list(client3, path=dirs[1], permission='*', mds=True)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            log.error("auth list failed")
            return 1
        rc1 = fs_util.fuse_mount(
            client1,
            new_client1_mouting_dir,
            new_client=new_client1_name,
            sub_dir=dirs[0])
        rc2 = fs_util.fuse_mount(
            client2,
            new_client2_mouting_dir,
            new_client=new_client2_name,
            sub_dir=dirs[0])
        rc3 = fs_util.kernel_mount(
            client3,
            new_client3_mouting_dir,
            client_info['mon_node_ip'],
            new_client=new_client3_name,
            sub_dir=dirs[1])
        if rc1 == 0 and rc2 == 0:
            log.info("Fuse mount passed")
        else:
            log.error("Fuse mount failed")
            return 1
        if rc3 == 0:
            log.info("kernel mount passed")
        else:
            log.error("kernel mount failed")
            return 1
        _, rc = fs_util.stress_io(client1, new_client1_mouting_dir,
                                  '', 0, 1, iotype='smallfile_create', fnum=1000, fsize=10)

        if rc == 0:
            log.info(
                'Permissions set  for client %s is working ' %
                new_client1_name)
        else:
            log.error(
                'Permissions set  for client %s is failed' %
                new_client1_name)
            return 1
        _, rc = fs_util.stress_io(client1, new_client1_mouting_dir,
                                  '', 0, 1, iotype='smallfile_delete', fnum=1000, fsize=10)
        if rc == 0:
            log.info(
                'Permissions set  for client %s is working properly' %
                new_client1_name)
        else:
            log.error(
                'Permissions set  for client %s is failed' %
                new_client1_name)
            return 1
        try:
            _, rc = fs_util.stress_io(
                client2, new_client2_mouting_dir, '', 0, 1, iotype='touch')
        except CommandFailed:
            log.info(
                'Permissions set  for client %s is working properly' %
                new_client2_name)

        _, rc = fs_util.stress_io(client3, new_client3_mouting_dir,
                                  '', 0, 1, iotype='smallfile_create', fnum=1000, fsize=10)

        if rc == 0:
            log.info(
                'Permissions set  for client %s is working properly' %
                new_client3_name)
        else:
            log.error(
                'Permissions set  for client %s is failed' %
                new_client3_name)
            return 1
        _, rc = fs_util.stress_io(client3, new_client3_mouting_dir,
                                  '', 0, 1, iotype='smallfile_delete', fnum=1000, fsize=10)
        if rc == 0:
            log.info('Permissions set  for client %s is working properly')
        else:
            log.error('Permissions set  for client %s is failed')
            return 1

        fs_util.client_clean_up(
            client1,
            '',
            new_client1_mouting_dir,
            'umount',
            client_name=new_client1_name)
        fs_util.client_clean_up(
            client2,
            '',
            new_client2_mouting_dir,
            'umount',
            client_name=new_client2_name)
        fs_util.client_clean_up(
            '',
            client3,
            new_client3_mouting_dir,
            'umount',
            client_name=new_client3_name)

        fs_util.auth_list(
            client1,
            path=dirs[0],
            permission='rw',
            osd=True)
        fs_util.auth_list(
            client3,
            path=dirs[1],
            permission='r',
            osd=True)

        fs_util.fuse_mount(
            client1,
            new_client1_mouting_dir,
            new_client=new_client1_name)
        fs_util.kernel_mount(
            client3,
            new_client3_mouting_dir,
            client_info['mon_node_ip'],
            new_client=new_client3_name)

        fs_util.stress_io(
            client1,
            new_client1_mouting_dir,
            '',
            0,
            1,
            iotype='smallfile_delete',
            fnum=1000,
            fsize=10)
        try:
            if client_info['kernel_clients'][0].pkg_type == 'rpm':
                client_info['kernel_clients'][0].exec_command(
                    cmd='sudo dd if=/dev/zero of=%s/file bs=10M count=10' %
                        new_client3_mouting_dir)

        except CommandFailed as e:
            log.info(e)
            log.info('Permissions set  for client %s is working properly' % (
                client_info['kernel_clients'][0].node.hostname + '_' + (dirs[1])))

        fs_util.client_clean_up(
            client1,
            '',
            new_client1_mouting_dir,
            'umount',
            client_name=client_info['fuse_clients'][0].node.hostname + '_%s' % (dirs[0]))

        fs_util.client_clean_up(
            '',
            client3,
            new_client3_mouting_dir,
            'umount',
            client_name=new_client3_name)
        fs_util.auth_list(client1, path=dirs[0], layout_quota='p_flag')
        fs_util.auth_list(client3, path=dirs[1], layout_quota='!p_flag')

        fs_util.fuse_mount(client1, new_client1_mouting_dir, new_client=new_client1_name)
        fs_util.kernel_mount(client3, new_client3_mouting_dir, client_info['mon_node_ip'], new_client=new_client3_name)
        file_name = 'file1'
        client_info['fuse_clients'][0].exec_command(cmd='sudo touch %s/%s' % (new_client1_mouting_dir, file_name))
        client_info['fuse_clients'][0].exec_command(cmd='sudo mkdir  %s/%s' % (new_client1_mouting_dir, dirs[0]))

        try:
            fs_util.setfattr(client3, 'stripe_unit', '1048576', new_client3_mouting_dir, file_name)
            fs_util.setfattr(client3, 'max_bytes', '100000000', new_client3_mouting_dir, dirs[1])
        except CommandFailed:
            log.info('Permission denied for setting attrs,success')
        fs_util.setfattr(client1, 'stripe_unit', '1048576', new_client1_mouting_dir, file_name)
        fs_util.setfattr(client1, 'max_bytes', '100000000', new_client1_mouting_dir, dirs[0])
        fs_util.client_clean_up(client1, '', new_client1_mouting_dir, 'umount', client_name=new_client1_name)

        fs_util.client_clean_up('', client3, new_client3_mouting_dir, 'umount', client_name=new_client3_name)
        fs_util.client_clean_up(client_info['fuse_clients'], client_info['kernel_clients'], client_info['mounting_dir'],
                                'umount')
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
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
