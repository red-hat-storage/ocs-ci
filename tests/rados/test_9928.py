import json
import logging
import random
import time
import traceback

from ceph.rados_utils import RadosHelper

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-9928 RADOS:
    Corrupt snap info of an object and run
    list-inconsistent-snapset
    Steps:
        1. create a replica 3 pool
        2. take few pool snaps with writes on objects b/w every snap
        3. chose primary osd and bring it down
        4. go to backend and using ceph-object-store tool corrupt the
           snapset of the object
        5. run deep-scrub on the pg
        6. check rados list-inconsistent-pg <pool>
        7. rados list-inconsistent-snapset <pg>

    Args:
        ceph_cluster (ceph.ceph.Ceph):
    """
    log.info("Running CEPH-9928")
    log.info(run.__doc__)
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    mons = []

    role = 'client'
    for mnode in ceph_nodes:
        if mnode.role == role:
            mons.append(mnode)

    ctrlr = mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format(
        cmon=ctrlr.hostname))
    helper = RadosHelper(ctrlr, config, log)

    """create a replica pool"""
    pname = "snapcorrupt_{rand}".format(rand=random.randint(0, 10000))
    try:
        helper.create_pool(pname, 1)
        log.info("Pool {pname} created".format(pname=pname))
    except Exception:
        log.error("Failed to create pool")
        log.error(traceback.format_exc())
        return 1
    time.sleep(5)

    """Get the target PG,osd for corruption operation"""
    oname = "UNIQUEOBJECT{i}".format(i=random.randint(0, 10000))
    cmd = "osd map {pname} {obj} --format json".format(pname=pname, obj=oname)
    (out, err) = helper.raw_cluster_cmd(cmd)
    outbuf = out.read().decode()
    log.info(outbuf)
    cmdout = json.loads(outbuf)
    targt_pg = cmdout['pgid']
    targt_osd_id = cmdout['up'][0]
    '''write data and take snaps'''
    putobj = "sudo rados -p {pool} put {obj} {path}".format(
        pool=pname, obj=oname, path="/etc/hosts"
    )
    for i in range(10):
        (out, err) = ctrlr.exec_command(cmd=putobj)
        snapcmd = "sudo rados mksnap -p {pool} {sname}".format(
            pool=pname, sname="snap" + str(i)
        )
        (out, err) = ctrlr.exec_command(cmd=snapcmd)
        log.info("put {obj}, snap {snap}".format(
            obj=oname, snap="snap" + str(i)
        ))
    '''
    Goto destination osd, stop the osd
    use ceph-objectstore-tool to corrupt
    snap info
    '''
    target_osd = ceph_cluster.get_osd_by_id(targt_osd_id)
    target_osd_node = target_osd.node
    cot_environment = target_osd_node
    osd_service = ceph_cluster.get_osd_service_name(targt_osd_id)
    partition_path = ceph_cluster.get_osd_data_partition_path(targt_osd_id)
    helper.kill_osd(target_osd_node, osd_service)
    time.sleep(10)
    osd_metadata = ceph_cluster.get_osd_metadata(targt_osd_id)
    osd_data = osd_metadata.get('osd_data')
    osd_journal = osd_metadata.get('osd_journal')
    if ceph_cluster.containerized:
        docker_image_string = '{docker_registry}/{docker_image}:{docker_tag}'.format(
            docker_registry=ceph_cluster.ansible_config.get('ceph_docker_registry'),
            docker_image=ceph_cluster.ansible_config.get('ceph_docker_image'),
            docker_tag=ceph_cluster.ansible_config.get('ceph_docker_image_tag'))
        cot_environment = helper.get_mgr_proxy_container(target_osd_node, docker_image_string)
        out, err = cot_environment.exec_command(
            cmd='mount | grep "{partition_path} "'.format(partition_path=partition_path),
            check_ec=False)
        device_mount_data = out.read().decode()  # type: str
        if not device_mount_data:
            cot_environment.exec_command(
                cmd='sudo mount {partition_path} {directory}'.format(partition_path=partition_path, directory=osd_data))

    slist_cmd = "sudo ceph-objectstore-tool --data-path \
            {osd_data} --journal-path \
            {osd_journal} \
            --head --op list {obj}".format(osd_data=osd_data, osd_journal=osd_journal,
                                           obj=oname)
    (out, err) = cot_environment.exec_command(cmd=slist_cmd)
    outbuf = out.read().decode()
    log.info(outbuf)

    corrupt_cmd = "sudo ceph-objectstore-tool --data-path \
            {osd_data} --journal-path \
            {osd_journal} \
            {outbuf} clear-snapset \
            corrupt".format(osd_data=osd_data, osd_journal=osd_journal, outbuf="'" + (outbuf) + "'")
    (out, err) = cot_environment.exec_command(cmd=corrupt_cmd)
    outbuf = out.read().decode()
    log.info(outbuf)

    helper.revive_osd(target_osd_node, osd_service)
    time.sleep(10)
    run_scrub = "pg deep-scrub {pgid}".format(pgid=targt_pg)
    (out, err) = helper.raw_cluster_cmd(run_scrub)
    outbuf = out.read().decode()
    log.info(outbuf)

    while 'HEALTH_ERR' and 'active+clean+inconsistent' not in outbuf:
        status = "-s --format json"
        (out, err) = helper.raw_cluster_cmd(status)
        outbuf = out.read().decode()
    log.info("HEALTH_ERR found as expected")
    log.info("inconsistent foud as expected")

    timeout = 300
    found = 0
    while timeout:
        incon_pg = "sudo rados list-inconsistent-pg \
                    {pname}".format(pname=pname)
        (out, err) = ctrlr.exec_command(cmd=incon_pg)
        outbuf = out.read().decode()
        log.info(outbuf)
        if targt_pg not in outbuf:
            time.sleep(1)
            timeout = timeout - 1
        else:
            found = 1
            break
    if timeout == 0 and found == 0:
        log.error("pg not listed as inconsistent")
        return 1

    timeout = 300
    found = 0
    while timeout:
        incon_snap = "sudo rados list-inconsistent-snapset \
                      {pg}".format(pg=targt_pg)
        (out, err) = ctrlr.exec_command(cmd=incon_snap)
        outbuf = out.read().decode()
        log.info(outbuf)
        if oname not in outbuf:
            time.sleep(1)
            timeout = timeout - 1
        else:
            found = 1
            break
    if timeout == 0 and found == 0:
        log.error("object is not listed in inconsistent snap")
        return 1

    return 0
