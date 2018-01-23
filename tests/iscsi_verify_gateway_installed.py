import datetime
import yaml
import logging
import json
import re
import random

from ceph.utils import setup_deb_repos
from ceph.utils import setup_repos, create_ceph_conf
from time import sleep

logger = logging.getLogger(__name__)
log = logger
def run(**kw):
    log.info("Running iscsi configuration")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    IQN="iqn.2003-01.com.redhat.iscsi-gw:ceph-igw"
    iscsi_gw_nodes=[]
    count=0
    if config.get('no_of_gateways'):
        no_of_gateways = int(config.get('no_of_gateways'))
    else:
        no_of_gateways = 2
    for ceph in ceph_nodes:
        if ceph.role == 'osd':
            out, rc = ceph.exec_command(sudo=True, cmd="rpm -qa |grep ceph-iscsi-config")
            check = out.read()
            log.info(check)
            if check.find("ceph-iscsi-config") != -1:
                iscsi_gw_nodes.append(ceph.hostname)
    #<------------ gateways--------
    check_count=1
    for ceph in ceph_nodes:
        for ceph_gw in range(len(iscsi_gw_nodes)):
            if ceph.role == 'osd' and check_count<=no_of_gateways:
                out, rc = ceph.exec_command(sudo=True, cmd="rpm -qa |grep ceph-iscsi-config")
                check = out.read()
                if check.find("ceph-iscsi-config") != -1:
                    ceph.exec_command(sudo=True ,cmd="gwcli ls")
                    gwcli_output = out.read()
                    if (gwcli_output.find(iscsi_gw_nodes[ceph_gw])):
                        log.info("found" + ceph.hostname)
                        count=count+1
            check_count=check_count+1
    log.info("No of gateways found "+str(count))
    count=0
    check_count=1
    for ceph in ceph_nodes:
        if ceph.role == 'osd' and check_count<=no_of_gateways:
            out, rc = ceph.exec_command(sudo=True, cmd="rpm -qa |grep ceph-iscsi-config")
            check = out.read()
            if check.find("ceph-iscsi-config")!= -1:
                ceph.exec_command(sudo=True ,cmd="gwcli ls")
                gwcli_output = out.read()
                if (gwcli_output.find(IQN)):
                    log.info("found IQN on" + ceph.hostname)
                    count=count+1
                check_count = check_count + 1
    if(count==no_of_gateways):
        return 0
    else:
        return 1
    log.info("No of IQN found "+str(count))



