# -*- coding: utf8 -*-
"""
Representation of general Kubernetes/OpenShift objects config file.

This allows one to work with multiple objects of different kind at once, as
explained in `Imperative Management of Kubernetes Objects Using Configuration
Files
<https://kubernetes.io/docs/tasks/manage-kubernetes-objects/imperative-config/>`_.

Usage:
    First you prepare list of dictionaries of k8s objects such as Deployment or
    PVC which describes your workload/project to be deployed in OCP. Then
    create instance of ``ObjectConfFile`` class with the list to be able to
    create the resource in the cluster (to run it), or delete it later when
    it's no longer needed.
"""


import logging
import os
import time
import yaml

from ocs_ci.framework import config
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed, NotFoundError


logger = logging.getLogger(__name__)


def link_spec_volume(spec_dict, volume_name, pvc_name):
    """
    Find volume of given name in given spec dict, and set given pvc name as
    a pvc for the volume in the spec.

    Args:
        spec_dict (dict): dictionary with a container/template spec
        volume_name (str): name of the volume in the spec dict to link
        pvc_name (str): name of the target pvc (for the given volume)

    Raises:
        NotFoundError when given volume is not found in given spec
    """
    is_pvc_linked = False
    for vol in spec_dict["volumes"]:
        if vol["name"] == volume_name:
            vol["persistentVolumeClaim"]["claimName"] = pvc_name
            is_pvc_linked = True
            break
    if not is_pvc_linked:
        raise NotFoundError("volume %s not found in given spec")


class ObjectConfFile:
    """
    This class represents particular k8s object config file which describes
    multiple k8s resources.

    Methods of this class implements `Imperative Management of Kubernetes
    Objects Using Configuration Files
    <https://kubernetes.io/docs/tasks/manage-kubernetes-objects/imperative-config/>`_.
    """

    def __init__(self, name, obj_dict_list, project, tmp_path):
        """
        Args:
            name (str): Name of this object config file
            obj_dict_list (list): List of dictionaries with k8s objects
            project (ocp.OCP): Instance of :class:`ocp.OCP` of ``Project``
                kind, specifying namespace where the object will be deployed.
            tmp_path (pathlib.Path): Directory where a temporary yaml file will
                be created. In test context, use pytest fixture `tmp_path`_.

        .. _`tmp_path`: https://docs.pytest.org/en/latest/tmpdir.html#the-tmp-path-fixture
        """
        self.name = name
        self.project = project
        # dump the job description in yaml format into a temporary file
        self._tmp_path = tmp_path
        self.yaml_file = tmp_path / f"objectconfig.{self.name}.yaml"
        self.yaml_file.write_text(yaml.dump_all(obj_dict_list))

    def _run_command(self, command, namespace, out_yaml_format=False):
        """
        Run given oc command on this object file.

        Args:
            command (str): Either ``create``, ``delete`` or ``get``
            namespace (str): Name of the namespace for oc command
            out_yaml_format (bool): Use oc yaml output format
        """
        if namespace is None:
            namespace = self.project.namespace
        logger.info(
            (
                f"going to run oc {command} "
                f"on {self.name} object config yaml file "
                f"in namespace {namespace}"
            )
        )
        logger.debug(self.yaml_file.read_text())
        oc_cmd = [
            "oc",
            "--kubeconfig",
            config.RUN["kubeconfig"],
            command,
            "-f",
            os.path.join(self._tmp_path, self.yaml_file.name),
            "-n",
            namespace,
        ]
        if out_yaml_format:
            oc_cmd.extend(["-o", "yaml"])
        # assuming run_cmd is logging everything
        out = run_cmd(cmd=oc_cmd, timeout=600)
        return out

    def create(self, namespace=None):
        """
        Run ``oc create`` on in this object file.

        Args:
            namespace (str): Name of the namespace where to deploy, overriding
            self.project.namespace value (in a similar way how you can specify
            any value to ``-n`` option of ``oc create``.
        """
        return self._run_command("create", namespace, out_yaml_format=True)

    def delete(self, namespace=None):
        """
        Run ``oc delete`` on in this object file.

        Args:
            namespace (str): Name of the namespace where to deploy, overriding
            self.project.namespace value (in a similar way how you can specify
            any value to ``-n`` option of ``oc delete``.
        """
        return self._run_command("delete", namespace)

    def apply(self, namespace=None):
        """
        Run ``oc apply`` on in this object file.

        Args:
            namespace (str): Name of the namespace where to deploy, overriding
            self.project.namespace value (in a similar way how you can specify
            any value to ``-n`` option of ``oc apply``.
        """
        return self._run_command("apply", namespace)

    def get(self, namespace=None):
        """
        Run ``oc get`` on in this object file.

        Args:
            namespace (str): Name of the namespace where to deploy, overriding
            self.project.namespace value (in a similar way how you can specify
            any value to ``-n`` option of ``oc get``.
        """
        out = self._run_command("get", namespace, out_yaml_format=True)
        return yaml.safe_load(out)

    def describe(self, namespace=None):
        """
        Run ``oc describe`` on in this object file.

        Args:
            namespace (str): Name of the namespace where to deploy, overriding
            self.project.namespace value (in a similar way how you can specify
            any value to ``-n`` option of ``oc describe``.
        """
        return self._run_command("describe", namespace, out_yaml_format=False)

    def wait_for_delete(self, resource_name="", timeout=60, sleep=3, namespace=None):
        """
        Wait for a resource to be deleted

        Args:
            resource_name (str): The name of the resource to wait
                for (e.g.kube_obj_name)
            timeout (int): Time in seconds to wait
            sleep (int): Sampling time in seconds
            namespace (str): Name of the namespace where to deploy, overriding
                self.project.namespace value (in a similar way how you can specify
                any value to ``-n`` option of ``oc get``.

        Raises:
            CommandFailed: If failed to verify the resource deletion
            TimeoutError: If resource is not deleted within specified timeout

        Returns:
            bool: True in case resource deletion is successful

        """

        start_time = time.time()
        while True:
            try:
                self.get(namespace=namespace)
            except CommandFailed as ex:
                if "NotFound" in str(ex):
                    logger.info(f"{resource_name} got deleted successfully")
                    return True
                else:
                    raise ex

            if timeout < (time.time() - start_time):
                describe_out = self.describe(namespace=namespace)
                msg = (
                    f"Timeout when waiting for {resource_name} to delete. "
                    f"Describe output: {describe_out}"
                )
                raise TimeoutError(msg)
            time.sleep(sleep)
