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
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    for ceph in ceph_nodes:
        if ceph.role == 'osd':
            ceph.exec_command(cmd='sudo yum install -y ceph-iscsi-cli', timeout=240)
            ceph.exec_command(cmd='sudo yum install -y tcmu-runner', timeout=240)
            ceph.exec_command(cmd='sudo systemctl enable rbd-target-api')
            ceph.exec_command(cmd='sudo systemctl start rbd-target-api')
            sleep(10)
        elif ceph.role == 'iscsi-clients':
            ceph.exec_command(cmd='sudo yum install -y iscsi-initiator-utils')
            ceph.exec_command(cmd='sudo yum install -y device-mapper-multipath')
            sleep(5)

    return 0




