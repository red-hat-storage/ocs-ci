import logging
import traceback
import hashlib
import os
import time
import random
import json

from ceph.rados_utils import RadosHelper
from ceph.parallel import parallel

logger = logging.getLogger(__name__)
log=logger

def run(**kw):
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
    """
    log.info("Running CEPH-83571453")
    log.info(run.__doc__)

    ceph_nodes=kw.get('ceph_nodes')
    config=kw.get('config')
    mons=[]
    osds=[]
    role='mon'
    for mnode in ceph_nodes:
        if mnode.role==role:
            mons.append(mnode)

    role='osd'
    for osd in ceph_nodes:
        if osd.role==role:
            osds.append(osd)

    ctrlr=mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format( \
                        cmon=ctrlr.hostname))
    Helper=RadosHelper(ctrlr, config, log)
    """create ec pool with k=4, m=2"""
    k=4
    m=2
    pname="eccorrupt_{rand}_{k}_{m}".format(
                rand=random.randint(0,10000), k=k, m=m)
    profile=pname
    prof_cmd="osd erasure-code-profile set {profile}\
                k={k}\
                m={m}\
                rulset-failure-domain=osd\
                crush-failure-domain=osd".format(profile=profile,
                                k=k, m=m)
    try:
        (out,err)=Helper.raw_cluster_cmd(prof_cmd)
        outbuf=out.read()
        log.info(outbuf)
        log.info("created profile {ec}".format(
                ec=profile))
    except exception as e:
        log.error("ec profile creation failed")
        log.error(traceback.format_exc())
        return 1
    '''create ec pool'''
    try:
        Helper.create_pool(pname, 1, profile)
        log.info("Pool {pname} is create".format(pname=pname))
    except Exception as e:
        log.error("failed to create pool")
        log.error(traceback.format_exc())
        return 1
    '''check whether pool exists'''
    try:
        Helper.get_pool_num(pname)
    except Exception as e:
        log.error("Unable to find pool")
        log.error(traceback.format_exc())
        return 1
    time.sleep(10)

    oname="OBJ_{pname}".format(pname=pname)
    cmd="osd map {pname} {obj} --format json".format(
            pname=pname, obj=oname
            )
    (out, err)=Helper.raw_cluster_cmd(cmd)
    outbuf=out.read()
    log.info(outbuf)
    cmdout=json.loads(outbuf)
    targt_pg=cmdout['pgid']
    '''considering primary only as of now because of bug
    1544680
    '''
    targt_osd=cmdout['up'][0]
    '''write data and take snaps'''
    putobj="sudo rados -p {pool} put {obj} {path}".format(
            pool=pname, obj=oname, path="/etc/hosts"
            )
    for i in range(10):
        (out,err)=ctrlr.exec_command(cmd=putobj)
        snapcmd="sudo rados mksnap -p {pool} {sname}".format(
                 pool=pname, sname="snap"+str(i)
                 )
        (out,err)=ctrlr.exec_command(cmd=snapcmd)
        log.info("put {obj}, snap {snap}".format(
                    obj=oname, snap="snap"+str(i)
            ))
    '''
    Goto destination osd, stop the osd
    use ceph-objectstore-tool to corrupt
    snap info
    '''
    ctx=Helper.get_osd_obj(targt_osd, osds)
    Helper.kill_osd(targt_osd, "SIGTERM", osds)
    time.sleep(10)
    slist_cmd="sudo ceph-objectstore-tool --data-path \
            /var/lib/ceph/osd/ceph-{id} --journal-path \
            /var/lib/ceph/osd/ceph-{id}/journal \
            --head --op list {obj}".format(id=targt_osd,
                    obj=oname)
    (out, err)=ctx.exec_command(cmd=slist_cmd)
    outbuf=out.read()
    log.info(outbuf)
    corrupt_cmd="sudo ceph-objectstore-tool --data-path \
            /var/lib/ceph/osd/ceph-{id} --journal-path \
            /var/lib/ceph/osd/ceph-{id}/journal \
            {outbuf} clear-snapset corrupt".format(id=targt_osd,outbuf="'"+(outbuf)+"'")
    (out, err)=ctx.exec_command(cmd=corrupt_cmd)
    outbuf=out.read()
    log.info(outbuf)

    Helper.revive_osd(targt_osd, osds)
    time.sleep(10)
    run_scrub="pg deep-scrub {pgid}".format(pgid=targt_pg)
    (out, err)=Helper.raw_cluster_cmd(run_scrub)
    outbuf=out.read()
    log.info(outbuf)

    while 'HEALTH_ERR' and 'active+clean+inconsistent' not in outbuf:
        status="-s --format json"
        (out, err)=Helper.raw_cluster_cmd(status)
        outbuf=out.read()
    log.info("HEALTH_ERR found as expected")
    log.info("inconsistent foud as expected")

    timeout=15
    found=0
    while timeout:
        incon_pg="sudo rados list-inconsistent-pg {pname}".format(pname=pname)
        (out, err)=ctrlr.exec_command(cmd=incon_pg)
        outbuf=out.read()
        log.info(outbuf)
        if targt_pg not in outbuf:
            time.sleep(1)
            timeout=timeout-1
        else:
            found=1
            break
    if timeout==0 and found==0:
            log.error("pg not listed as inconsistent")
            return 1

    timeout=15
    found=0
    while timeout:
        incon_obj="sudo rados list-inconsistent-snapset {pg}".format(pg=targt_pg)
        (out, err)=ctrlr.exec_command(cmd=incon_obj)
        outbuf=out.read()
        log.info(outbuf)
        if oname not in outbuf:
            time.sleep(1)
            timeout=timeout-1
        else:
            found=1
            break
    if timeout==0 and found==0:
        log.error("object is not listed in inconsistent obj")
        return 1

    return 0
