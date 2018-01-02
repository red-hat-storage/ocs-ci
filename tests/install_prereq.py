
import logging
import time
from ceph.parallel import parallel

log = logging.getLogger(__name__)

rpm_pkgs = ['wget', 'git', 'epel-release', 'redhat-lsb', 'python-virtualenv', 'python-nose']
deb_pkgs = ['wget', 'git', 'python-virtualenv']

epel_rpm = 'https://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-9.noarch.rpm'
epel_pkgs = ['python-pip']
deb_all_pkgs = " ".join(deb_pkgs)
rpm_all_pkgs = ' '.join(rpm_pkgs)

def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    with parallel() as p:
        for ceph in ceph_nodes:
            p.spawn(install_prereq, ceph)
            time.sleep(20)
    return 0

def install_prereq(ceph):
        log.info("Waiting for cloud config to complete on " + ceph.hostname)
        ceph.exec_command(cmd='while [ ! -f /ceph-qa-ready ]; do sleep 15; done')
        log.info("cloud config to completed on " + ceph.hostname)
        if ceph.pkg_type == 'deb':
            ceph.exec_command(cmd='sudo apt-get install -y ' + deb_all_pkgs, long_running=True)
        else:
            ceph.exec_command(
                cmd='sudo subscription-manager --force register  --serverurl=subscription.rhsm.stage.redhat.com:443/subscription  --baseurl=https://cdn.stage.redhat.com --username=qa@redhat.com --password=redhatqa --auto-attach && sudo subscription-manager attach --pool=8a85f9823e3d5e43013e3ddd4e9509c4', timeout=240)
            ceph.exec_command(cmd='sudo subscription-manager repos --disable=*', long_running=True)
            ceph.exec_command(cmd='sudo subscription-manager repos --enable=rhel-7-server-rpms  --enable=rhel-7-server-optional-rpms --enable=rhel-7-server-extras-rpms', long_running=True)
            ceph.exec_command(cmd='sudo yum install -y ' + rpm_all_pkgs, long_running=True)
            ceph.exec_command(cmd='sudo yum install -y ' + rpm_all_pkgs, long_running=True)
            # install epel package
            ceph.exec_command(cmd='sudo yum clean metadata')
            # finally install python2-pip directly using rpm since its available only in epel
            ceph.exec_command(cmd='sudo yum install -y http://dl.fedoraproject.org/pub/fedora-secondary/releases/26/Everything/i386/os/Packages/p/python2-pip-9.0.1-9.fc26.noarch.rpm')
            #add GPG key
            ceph.exec_command(cmd='curl --insecure -O -L https://prodsec.redhat.com/keys/00da75f2.txt && sudo rpm --import 00da75f2.txt')
