import os
import logging
import sys
import time

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    log.info("Running workunit test")
    ceph_nodes = kw.get('ceph_nodes')
    config  = kw.get('config')
    
    clients = []
    role = 'client'
    if config.get('role'):
        role = config.get('role')
    for cnode in ceph_nodes:
        if cnode.role == role:
            clients.append(cnode)
    
    idx=0
    client = clients[idx]
    
    if config.get('idx'):
        idx = config['idx']
        client = clients[idx]


    if config.get('repo'):
        repo = config.get('repo')
    else:
        repo = 'git://git.ceph.com/ceph.git'
        
    if config.get('branch'):
        branch = config.get('branch')
    else:
        branch = 'master'
    
    git_cmd = 'git clone -b ' + branch + ' ' + repo
    if config.get('test_name'):
        test_name = config.get('test_name')
        
    tout=600
    if config.get('timeout'):
        tout = config.get('timeout')
    cmd1  = 'mkdir cephtest ; cd cephtest ; {git_cmd}'.format(git_cmd=git_cmd)
    client.exec_command(cmd=cmd1, long_running=True)
    cmd2 = 'CEPH_REF={ref} sudo -E sh cephtest/ceph/qa/workunits/{name}'.format(ref=branch, name=test_name)
    output, ec = client.exec_command(cmd=cmd2, long_running=True)
    if ec == 0:
        log.info("Workunit completed successfully")
    else:
        log.info("Error during workunit")
    return ec