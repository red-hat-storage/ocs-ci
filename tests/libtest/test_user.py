import logging
import os
from time import sleep

from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


def test_user_creation(user_factory):
    user = user_factory()
    kubeconfig = os.getenv('KUBECONFIG')
    kube_data = ""
    with open(kubeconfig, 'r') as kube_file:
        kube_data = kube_file.readlines()
    sleep(30)
    exec_cmd(['oc', 'login', '-u', user[0], '-p', user[1]], secrets=[user[1]])
    exec_cmd(['oc', 'logout'])
    with open(kubeconfig, 'w') as kube_file:
        kube_file.writelines(kube_data)
