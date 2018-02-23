from time import sleep
from ceph.parallel import *
from tests.rbd.rbd_utils import *
import datetime
import random
import string


def run(**kw):
    start = datetime.datetime.now()
    log.info("Running rbd export tests")
    ceph_nodes = kw.get('ceph_nodes')
    pool = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    image = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    snap = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    dir_name = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    clone = ''.join([random.choice(string.ascii_letters) for _ in xrange(16)])
    for node in ceph_nodes:
        if node.role == 'mon':
            break
    create_dir(node, dir_name)
    create_pool(node, pool)
    create_image(node, '10G', pool, image)
    bench_write(node, pool, image)
    create_snap(node, pool, image, snap)
    protect_snap(node, pool, image, snap)
    create_clone(node, pool, image, snap, clone)

    with parallel() as p:
        p.spawn(bench_write, node, pool, clone)
        p.spawn(export_image, node, pool, clone, dir_name+'/export9876')

    sleep(5)
    with parallel() as p:
        p.spawn(resize_image, node, '20G', pool, image)
        p.spawn(export_image, node, pool, image, dir_name+'/export9877_1')
    sleep(5)
    with parallel() as p:
        p.spawn(resize_image, node, '8G', pool, image)
        p.spawn(export_image, node, pool, image, dir_name+'/export9877_2')

    sleep(5)
    with parallel() as p:
        p.spawn(flatten, node, pool, clone)
        p.spawn(export_image, node, pool, image, dir_name+'/export9878')

    sleep(5)
    with parallel() as p:
        p.spawn(export_image, node, pool, clone, dir_name+'/export9879')
        p.spawn(lock, node, pool, clone)

    sleep(5)
    with parallel() as p:
        p.spawn(export_image, node, pool, clone, dir_name+'/export9880_1')
        p.spawn(resize_image, node, '20G', pool, image)
    sleep(5)
    with parallel() as p:
        p.spawn(export_image, node, pool, clone, dir_name+'/export9880_2')
        p.spawn(resize_image, node, '8G', pool, image)

    log.info('Script execution time : ' + str(datetime.datetime.now() - start))
    return 0
