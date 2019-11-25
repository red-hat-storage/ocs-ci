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

import yaml

from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(name=__file__)


class ObjectConfFile():
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
        logger.info((
            f"going to run oc {command} "
            f"on {self.name} object config yaml file "
            f"in namespace {namespace}"))
        logger.debug(self.yaml_file.read_text())
        oc_cmd = [
            "oc",
            "--kubeconfig",
            os.getenv('KUBECONFIG'),
            command,
            "-f",
            os.path.join(self._tmp_path, self.yaml_file.name),
            "-n",
            namespace]
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
