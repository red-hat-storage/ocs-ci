import datetime
import logging
import random
import string
from time import sleep

import tests.rbd.rbd_utils as rbd
from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    start = datetime.datetime.now()
    log.info("Running rbd export tests")
    ceph_nodes = kw.get('ceph_nodes')
    node = None
    pool = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    image = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    snap = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    dir_name = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    clone = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    for node in ceph_nodes:
        if node.role == 'mon':
            break
    rbd.create_dir(node, dir_name)
    rbd.create_pool(node, pool)
    rbd.create_image(node, '10G', pool, image)
    rbd.bench_write(node, pool, image)
    rbd.create_snap(node, pool, image, snap)
    rbd.protect_snap(node, pool, image, snap)
    rbd.create_clone(node, pool, image, snap, clone)

    with parallel() as p:
        p.spawn(rbd.bench_write, node, pool, clone)
        p.spawn(rbd.export_image, node, pool, clone, dir_name + '/export9876')

    sleep(5)
    with parallel() as p:
        p.spawn(rbd.resize_image, node, '20G', pool, image)
        p.spawn(rbd.export_image, node, pool, image, dir_name + '/export9877_1')
    sleep(5)
    with parallel() as p:
        p.spawn(rbd.resize_image, node, '8G', pool, image)
        p.spawn(rbd.export_image, node, pool, image, dir_name + '/export9877_2')

    sleep(5)
    with parallel() as p:
        p.spawn(rbd.flatten, node, pool, clone)
        p.spawn(rbd.export_image, node, pool, image, dir_name + '/export9878')

    sleep(5)
    with parallel() as p:
        p.spawn(rbd.export_image, node, pool, clone, dir_name + '/export9879')
        p.spawn(rbd.lock, node, pool, clone)

    sleep(5)
    with parallel() as p:
        p.spawn(rbd.export_image, node, pool, clone, dir_name + '/export9880_1')
        p.spawn(rbd.resize_image, node, '20G', pool, image)
    sleep(5)
    with parallel() as p:
        p.spawn(rbd.export_image, node, pool, clone, dir_name + '/export9880_2')
        p.spawn(rbd.resize_image, node, '8G', pool, image)

    log.info('Script execution time : ' + str(datetime.datetime.now() - start))
    return 0
