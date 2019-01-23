import json
import logging
import random
import time
import traceback

from ceph.rados_utils import RadosHelper

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83571453-RADOS:
    Corrupt an object in ec pool followed by
    list-inconsistent-* commands
    1. create a jerasure ec pool with k=4,m=2
    2. create an object in the pool
    3. chose primary osd from the acting set and go to the backend
    4. corrupt object attrib from the backend
    5. run deep-scrub on the pool
    6. rados list-inconsistent-pg <pool>
    7. rados list-inconsistent-obj <pg>

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    log.info("Running CEPH-83571453")
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
    """create ec pool with k=4, m=2"""
    k = 4
    m = 2
    pname = "eccorrupt_{rand}_{k}_{m}".format(
        rand=random.randint(0, 10000), k=k, m=m)
    profile = pname
    prof_cmd = "osd erasure-code-profile set {profile}\
                k={k}\
                m={m}\
                rulset-failure-domain=osd\
                crush-failure-domain=osd".format(profile=profile,
                                                 k=k, m=m)
    try:
        (out, err) = helper.raw_cluster_cmd(prof_cmd)
        outbuf = out.read().decode()
        log.info(outbuf)
        log.info("created profile {ec}".format(
            ec=profile))
    except Exception:
        log.error("ec profile creation failed")
        log.error(traceback.format_exc())
        return 1
    '''create ec pool'''
    try:
        helper.create_pool(pname, 1, profile)
        log.info("Pool {pname} is create".format(pname=pname))
    except Exception:
        log.error("failed to create pool")
        log.error(traceback.format_exc())
        return 1
    '''check whether pool exists'''
    try:
        helper.get_pool_num(pname)
    except Exception:
        log.error("Unable to find pool")
        log.error(traceback.format_exc())
        return 1
    time.sleep(10)

    oname = "OBJ_{pname}".format(pname=pname)
    cmd = "osd map {pname} {obj} --format json".format(
        pname=pname, obj=oname
    )
    (out, err) = helper.raw_cluster_cmd(cmd)
    outbuf = out.read().decode()
    log.info(outbuf)
    cmdout = json.loads(outbuf)
    targt_pg = cmdout['pgid']
    '''considering primary only as of now because of bug
    1544680
    '''
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
        incon_pg = "sudo rados list-inconsistent-pg {pname}".format(pname=pname)
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
        incon_obj = "sudo rados list-inconsistent-snapset \
                     {pg}".format(pg=targt_pg)
        (out, err) = ctrlr.exec_command(cmd=incon_obj)
        outbuf = out.read().decode()
        log.info(outbuf)
        if oname not in outbuf:
            time.sleep(1)
            timeout = timeout - 1
        else:
            found = 1
            break
    if timeout == 0 and found == 0:
        log.error("object is not listed in inconsistent obj")
        return 1

    return 0
