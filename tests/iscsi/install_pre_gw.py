import logging
from time import sleep

log = logging


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    for ceph in ceph_nodes:
        if ceph.role == 'osd':
            ceph.exec_command(
                cmd='sudo yum install -y ceph-iscsi-cli',
                timeout=240)
            ceph.exec_command(
                cmd='sudo yum install -y tcmu-runner',
                timeout=240)
            ceph.exec_command(cmd='sudo systemctl enable rbd-target-api')
            ceph.exec_command(cmd='sudo systemctl start rbd-target-api')
            sleep(10)
        elif ceph.role == 'iscsi-clients':
            ceph.exec_command(cmd='sudo yum install -y iscsi-initiator-utils')
            ceph.exec_command(
                cmd='sudo yum install -y device-mapper-multipath')
            ceph.exec_command(
                cmd="sudo yum install -y fio",
                long_running=True)
            sleep(5)
    return 0
