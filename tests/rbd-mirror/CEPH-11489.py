import logging
import time
log = logging.getLogger(__name__)


def check_entries(peercluster, imagespec):
    out = peercluster.wait_for_status(imagespec=imagespec,
                                      description_pattern='entries')
    return int(out.split('=')[-1])


def check_replication_start(peercluster, imagespec, prev_entries):
    curr_entries = check_entries(peercluster, imagespec)
    if curr_entries < prev_entries:
        return 0
    else:
        return 1


def run(**kw):
    log.info("Starting CEPH-11489")
    mirror1 = kw.get('test_data')['mirror1']
    mirror2 = kw.get('test_data')['mirror2']
    config = kw.get('config')
    poolname = mirror1.random_string() + '11489pool'
    imagename = mirror1.random_string() + '11489image'
    imagespec = poolname + '/' + imagename
    delay = config.get('delay')
    mirror1.create_pool(poolname=poolname)
    mirror2.create_pool(poolname=poolname)
    mirror1.config_mirror(mirror2, poolname=poolname, mode='image')
    mirror1.create_image(imagespec=imagespec, size=config.get('imagesize'))
    mirror1.exec_cmd(
        cmd='rbd image-meta set {} conf_rbd_mirroring_replay_delay {}'
            .format(imagespec, config.get('delay')))
    mirror1.enable_mirroring('image', imagespec)
    mirror1.wait_for_status(poolname=poolname, health_pattern='OK')
    mirror2.wait_for_status(poolname=poolname, health_pattern='OK')
    mirror2.wait_for_status(poolname=poolname, images_pattern=1)
    mirror1.benchwrite(imagespec=imagespec, io=config.get('io-total'))
    time.sleep(delay / 4)
    curr_entries1 = check_entries(mirror2, imagespec)
    time.sleep(delay / 4)
    rc = check_replication_start(mirror2, imagespec, curr_entries1)
    if rc == 0:
        return 1
    mirror1.create_image(imagespec=imagespec + '2',
                         size=config.get('imagesize'))
    mirror1.exec_cmd(
        cmd='rbd image-meta set {} conf_rbd_mirroring_replay_delay {}'
            .format(imagespec + '2', config.get('delay')))
    mirror1.enable_mirroring('image', imagespec + '2')
    mirror1.wait_for_status(poolname=poolname, health_pattern='OK')
    mirror2.wait_for_status(poolname=poolname, health_pattern='OK')
    mirror2.wait_for_status(poolname=poolname, images_pattern=2)
    mirror1.benchwrite(imagespec=imagespec + '2', io=config.get('io-total'))
    time.sleep(delay / 4)
    curr_entries2 = check_entries(mirror2, imagespec + '2')
    time.sleep(delay / 4)
    rc = check_replication_start(mirror2, imagespec + '2', curr_entries2)
    if rc == 0:
        return 1
    for iterator in range(0, 11):
        rc = check_replication_start(mirror2, imagespec, curr_entries1)
        if rc == 0:
            break
        else:
            if iterator == 10:
                return 1
            time.sleep(10)

    time.sleep(delay / 2)
    for iterator in range(0, 11):
        rc = check_replication_start(mirror2, imagespec + '2', curr_entries2)
        if rc == 0:
            break
        else:
            if iterator == 10:
                return 1
            time.sleep(10)

    mirror1.delete_pool(poolname=poolname)
    mirror2.delete_pool(poolname=poolname)
    return 0
