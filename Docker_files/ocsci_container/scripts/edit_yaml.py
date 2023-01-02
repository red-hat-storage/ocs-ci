import subprocess
import yaml
import sys
import os

from pathlib import Path

cli_args = sys.argv[1:]


def send_cmd(cmd=None):
    return subprocess.check_output(cmd, shell=True, universal_newlines=True)


def yaml_to_dict(path=None):
    """
    Convert Yaml File to Dictionary

    Args:
        path (str): path to yaml file

    Returns:
        res (dic): return dictionary [yaml source]

    """
    full_path = os.path.join(Path(__file__).parent, path)
    with open(full_path, "r") as file:
        res = yaml.load(file.read(), Loader=yaml.Loader) or {}
        if not isinstance(res, dict):
            raise ValueError("Invalid yaml file")
        return res


def dict_to_yaml(path, data):
    """
    Convert Dictionary to Yaml file

    Args:
        path (str): path to yaml file
        data (dic): data of yaml

    Returns:
        path (dic): return path to yaml file

    """
    path = os.path.join(path, "build_config.yaml")
    with open(path, "w+") as outfile:
        yaml.dump(data, outfile, default_flow_style=False)
    return path


ceph_key_out = send_cmd(
    "oc rsh --kubeconfig /opt/cluster/p1/auth/kubeconfig -n openshift-storage $(oc get --kubeconfig "
    "/opt/cluster/p1/auth/kubeconfig pods -o wide -n openshift-storage|grep tool|awk '{print$1}') "
    "cat /etc/ceph/keyring |grep key"
)
ceph_key_ls = ceph_key_out.split(" ")
ceph_key = ceph_key_ls[2].replace("\n", "")

endpoint_out = send_cmd(
    "oc get --kubeconfig /opt/cluster/p1/auth/kubeconfig storagecluster -o yaml"
    " -n openshift-storage|grep -i endpo"
)
endpoint_ls = endpoint_out.split(" ")
endpoint = endpoint_ls[5].replace("\n", "")

build_config = yaml_to_dict("build_config.yaml")
build_config["AUTH"]["external"]["ceph_admin_key"] = ceph_key
build_config["DEPLOYMENT"]["storage_provider_endpoint"] = endpoint
build_config["ENV_DATA"]["provider_name"] = "oviner-pr"
dict_to_yaml(path="/opt/cluster", data=build_config)
