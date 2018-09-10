from tests.cephfs.cephfs_utils import FsUtils
import logging
import time
logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    ceph_nodes = kw.get('ceph_nodes')
    new_fs_name = 'cephfs_ec'
    new_fs_datapool = 'ec_data_pool'
    fs_util = FsUtils(ceph_nodes)
    client_info, rc = fs_util.get_clients()
    config = kw.get('config')
    bluestore = config.get('bluestore')
    k_and_m = config.get('ec-pool-k-m')
    if (bluestore is not None and k_and_m is None) or (bluestore is None and k_and_m is None):
        log.info('tests will run on replicated pool')
        return 0
    elif bluestore is None and k_and_m is not None:
        log.error('Filestore does not support ecpools')
        return 1

    fs_info = fs_util.get_fs_info(client_info['mon_node'][0])
    fs_util.del_cephfs(
        client_info['mds_nodes'], fs_info.get('fs_name'))
    profile_name = fs_util.create_erasure_profile(
        client_info['mon_node'][0],
        'ec_profile',
        k_and_m[0],
        k_and_m[2])
    fs_util.create_pool(
        client_info['mon_node'][0],
        new_fs_datapool,
        64,
        64,
        pool_type='erasure',
        profile_name=profile_name)
    fs_util.create_fs(
        client_info['mds_nodes'],
        new_fs_name,
        new_fs_datapool,
        fs_info.get('metadata_pool_name'),
        pool_type='erasure_pool')
    time.sleep(100)
    return 0
