from tests.cephfs.cephfs_utils import FsUtils
import timeit
from ceph.ceph import CommandFailed
import traceback
import logging

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        start = timeit.default_timer()
        tc = '11334'
        file_name = 'file'
        log.info("Running cephfs %s test case" % (tc))
        ceph_nodes = kw.get('ceph_nodes')
        fs_util = FsUtils(ceph_nodes)
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

        rc1 = fs_util.auth_list(client1, client_info['mon_node'])
        rc2 = fs_util.auth_list(client2, client_info['mon_node'])
        rc3 = fs_util.auth_list(client3, client_info['mon_node'])
        rc4 = fs_util.auth_list(client4, client_info['mon_node'])
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
        vals, rc = fs_util.getfattr(
            client1, client_info['mounting_dir'], file_name)
        rc = fs_util.setfattr(
            client1,
            'stripe_unit',
            '1048576',
            client_info['mounting_dir'],
            file_name)
        if rc == 0:
            log.info("Setfattr stripe_unit for file %s success" % file_name)
        else:
            raise CommandFailed(
                "Setfattr stripe_unit for file %s success" %
                file_name)
        rc = fs_util.setfattr(
            client1,
            'stripe_count',
            '8',
            client_info['mounting_dir'],
            file_name)
        if rc == 0:
            log.info("Setfattr stripe_count for file %s success" % file_name)
        else:
            raise CommandFailed(
                "Setfattr stripe_count for file %s success" %
                file_name)
        rc = fs_util.setfattr(
            client1,
            'object_size',
            '10485760',
            client_info['mounting_dir'],
            file_name)
        if rc == 0:
            log.info("Setfattr object_size for file %s success" % file_name)
        else:
            raise CommandFailed(
                "Setfattr object_size for file %s success" %
                file_name)
        fs_info = fs_util.get_fs_info(client_info['mon_node'][0])
        fs_util.create_pool(
            client_info['mon_node'][0],
            'new_data_pool',
            64,
            64)
        rc = fs_util.add_pool_to_fs(
            client_info['mon_node'][0],
            fs_info.get('fs_name'),
            'new_data_pool')
        if 0 in rc:
            log.info("Adding new pool to cephfs success")
        else:
            raise CommandFailed("Adding new pool to cephfs failed")
        rc = fs_util.setfattr(
            client1,
            'pool',
            'new_data_pool',
            client_info['mounting_dir'],
            file_name)
        if rc == 0:
            log.info("Setfattr pool for file %s success" % file_name)
        else:
            raise CommandFailed(
                "Setfattr pool for file %s success" %
                file_name)

        vals, rc = fs_util.getfattr(
            client1, client_info['mounting_dir'], file_name)
        log.info("Read individual layout fields by using getfattr:")
        for client in client1:
            out, rc = client.exec_command(
                cmd="sudo getfattr -n ceph.file.layout.pool %s%s" %
                (client_info['mounting_dir'], file_name))
            if vals['pool'] in out.read():
                log.info("reading pool by getfattr successfull")
            out, rc = client.exec_command(
                cmd="sudo getfattr -n ceph.file.layout.stripe_unit  %s%s" %
                (client_info['mounting_dir'], file_name))
            if vals['stripe_unit'] in out.read():
                log.info("reading stripe_unit by getfattr successfull")
            out, rc = client.exec_command(
                cmd="sudo getfattr -n ceph.file.layout.stripe_count  %s%s" %
                (client_info['mounting_dir'], file_name))
            if vals['stripe_count'] in out.read():
                log.info("reading stripe_count by getfattr successfull")
            out, rc = client.exec_command(
                cmd="sudo getfattr -n ceph.file.layout.object_size  %s%s" %
                (client_info['mounting_dir'], file_name))
            if vals['object_size'] in out.read():
                log.info("reading object_size by getfattr successfull")
            break
        rc = fs_util.remove_pool_from_fs(
            client_info['mon_node'][0],
            fs_info.get('fs_name'),
            'new_data_pool')
        if 0 in rc:
            log.info("Pool removing success")
        else:
            raise CommandFailed("Pool removing  failed")
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            rc_client = fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'],
                'umount')

        else:
            rc_client = fs_util.client_clean_up(
                client_info['fuse_clients'], '',
                client_info['mounting_dir'], 'umount')

        if rc_client == 0:
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
            rc_client = fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'],
                'umount')
        else:
            rc_client = fs_util.client_clean_up(
                client_info['fuse_clients'], '',
                client_info['mounting_dir'], 'umount')

        if rc_client == 0:
            log.info('Cleaning up successfull')
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
