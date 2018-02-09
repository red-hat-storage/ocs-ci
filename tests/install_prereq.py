import datetime
import itertools
import logging
import time
import traceback
from ceph.parallel import parallel
from ceph.utils import update_ca_cert
log = logging.getLogger(__name__)
# rpm_pkgs = ['wget', 'git', 'epel-release', 'redhat-lsb', 'python-virtualenv', 'python-nose']
rpm_pkgs = ['wget', 'git', 'python-virtualenv', 'python-nose']
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
def install_prereq(ceph,timeout=1800):
        log.info("Waiting for cloud config to complete on " + ceph.hostname)
        ceph.exec_command(cmd='while [ ! -f /ceph-qa-ready ]; do sleep 15; done')
        log.info("cloud config to completed on " + ceph.hostname)
        update_ca_cert(ceph, 'https://password.corp.redhat.com/RH-IT-Root-CA.crt')
        update_ca_cert(ceph, 'https://password.corp.redhat.com/legacy.crt')
        if ceph.pkg_type == 'deb':
            ceph.exec_command(cmd='sudo apt-get install -y ' + deb_all_pkgs, long_running=True)
        else:
            timeout = datetime.timedelta(seconds=timeout)
            starttime = datetime.datetime.now()
            log.info(
                "Subscribing {ip} host with {timeout} timeout".format(ip=ceph.ip_address, timeout=timeout))
            while True:
                try:
                    ceph.exec_command(
                        cmd='sudo subscription-manager --force register  --serverurl=subscription.rhsm.stage.redhat.com:443/subscription  --baseurl=https://cdn.stage.redhat.com --username=qa@redhat.com --password=redhatqa --auto-attach && sudo subscription-manager attach --pool=8a85f9823e3d5e43013e3ddd4e9509c4',
                        timeout=720)

                    break
                except:
                    if datetime.datetime.now() - starttime > timeout:
                        try:
                            out, err = ceph.exec_command(
                                cmd='cat /var/log/rhsm/rhsm.log', timeout=120)
                            rhsm_log = out.read()
                        except:
                            rhsm_log = 'No Log Available'
                        raise RuntimeError(
                            "Failed to subscribe {ip} with {timeout} timeout:\n {stack_trace}\n\n rhsm.log:\n{log}".format(
                                ip=ceph.ip_address,
                                timeout=timeout, stack_trace=traceback.format_exc(), log=rhsm_log))
                    else:
                        wait = iter(x for x in itertools.count(1, 10))
                        time.sleep(next(wait))
            ceph.exec_command(cmd='sudo subscription-manager repos --disable=*', long_running=True)
            ceph.exec_command(cmd='sudo subscription-manager repos --enable=rhel-7-server-rpms  --enable=rhel-7-server-optional-rpms --enable=rhel-7-server-extras-rpms', long_running=True)
            ceph.exec_command(cmd='sudo yum install -y ' + rpm_all_pkgs, long_running=True)
            ceph.exec_command(cmd='sudo yum install -y ' + rpm_all_pkgs, long_running=True)
            if ceph.role == 'client':
                ceph.exec_command(cmd='sudo yum install -y attr',long_running=True)
                ceph.exec_command(cmd='sudo pip install crefi', long_running=True)

            # install epel package
            ceph.exec_command(cmd='sudo yum clean metadata')
            # finally install python2-pip directly using rpm since its available only in epel
            ceph.exec_command(cmd='sudo yum install -y http://dl.fedoraproject.org/pub/fedora-secondary/releases/26/Everything/i386/os/Packages/p/python2-pip-9.0.1-9.fc26.noarch.rpm')
            #add GPG key
            ceph.exec_command(cmd='curl --insecure -O -L https://prodsec.redhat.com/keys/00da75f2.txt && sudo rpm --import 00da75f2.txt')
