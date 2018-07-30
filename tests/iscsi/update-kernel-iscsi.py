import logging
import os
import time

from ceph.ceph import CommandFailed
from ceph.parallel import parallel

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    log.info("Running workunit test")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    role = 'osd'
    if not check_rhel_7_4(ceph_nodes[0]):
        return 0
    if config.get('role'):
        role = config.get('role')
    with parallel() as p:
        for cnode in ceph_nodes:
            if cnode.role == role:
                if config.get('kernel-repo'):
                    repo = config.get('kernel-repo')
                    log.info("writing " + repo)
                    p.spawn(update_kernel_and_reboot, cnode, repo)
                elif os.environ.get('KERNEL-REPO-URL', None) is not None:
                    log.info("writing from ENV " + repo)
                    repo = os.environ['KERNEL-REPO-URL']
                    p.spawn(update_kernel_and_reboot, cnode, repo)
    return 0


def check_rhel_7_4(node):
    try:
        node.exec_command(sudo=True,
                          cmd="cat /etc/redhat-release | grep 'release 7.4'",
                          check_ec=True)
    except CommandFailed:
        log.info("Skipping kernel upgrade for RHEL 7.4")
        return False
    else:
        log.info("RHEL version is 7.4. Runnig kernel upgrade")
        return True


def update_kernel_and_reboot(client, repo_url):
    kernel_repo_file = """
[KernelUpdate]
name=KernelUpdate
baseurl= {base_url}
gpgcheck=0
enabled=1
""".format(base_url=repo_url)
    client.exec_command(cmd="sudo yum install -y wget")
    client.exec_command(
        cmd="sudo wget -O /etc/yum.repos.d/rh_7_nightly.repo "
            "http://file.rdu.redhat.com/~kdreyer/repos/rhel-7-nightly.repo")
    kernel_repo = client.write_file(
        sudo=True,
        file_name='/etc/yum.repos.d/rh_kernel.repo',
        file_mode='w')
    kernel_repo.write(kernel_repo_file)
    kernel_repo.flush()
    o, e = client.exec_command(cmd='uname -a')
    log.info(o.read())
    client.exec_command(cmd='sudo yum update metadata')
    o, e = client.exec_command(cmd='sudo yum update -y kernel')
    client.exec_command(cmd='sudo reboot', check_ec=False)
    time.sleep(300)
    client.reconnect()
    client.exec_command(
        sudo=True,
        cmd="rm -f /etc/yum.repos.d/rh_7_nightly.repo")
    o, e = client.exec_command(cmd='uname -a')
    log.info(o.read())
