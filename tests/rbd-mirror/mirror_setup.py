import logging
import rbd_mirror_utils as rbdmirror

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Starting mirroring")
    config = kw.get('config')

    mirror1 = rbdmirror.RbdMirror(kw.get('ceph_cluster_dict').get('ceph-rbd1'),
                                  config)
    mirror2 = rbdmirror.RbdMirror(kw.get('ceph_cluster_dict').get('ceph-rbd2'),
                                  config)
    kw.get('test_data').update({'mirror1': mirror1, 'mirror2': mirror2})

    # Handling of clusters with same name
    if mirror1.cluster_name == mirror2.cluster_name:
        mirror1.handle_same_name('master')
        if 'two-way' in config.get('way', ''):
            mirror2.handle_same_name('slave')

    if 'one-way' in config.get('way', ''):
        mirror1.setup_mirror(mirror2)
        mirror2.setup_mirror(mirror1, way='one-way')
    else:
        mirror1.setup_mirror(mirror2)
        mirror2.setup_mirror(mirror1)

    return 0
