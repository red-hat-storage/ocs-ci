""" Pod realted functionalities and context info

Each pod in the openshift cluster will have a corresponding pod object.
Few assumptions:
    oc cluster is up and running

"""

import logging

from ocs.pod_exec import Exec, CmdObj

logger = logging.getLogger(__name__)


class Pod(object):
    """Handles per pod related context

        Attributes:
            _name (str):      name of the pod in oc cluster
            _namespace(str):  openshift namespace where this pod lives
            labels (dict):   A dict of oc labels associated with pod
            roles (list):    This could be oc roles like Master, etcd OR
                             ceph roles like mon, osd etc


    """

    def __init__(self, name=None, namespace=None, labels=None, roles=[]):
        """Context detail per pod

            Args:
                name (string):      name of the pod in oc cluster
                namespace (string): namespace in which pod lives
                labels (dict):      dictionary of oc labels associated with pod
                roles (list):       This could be oc roles like Master, etcd OR
                    ceph roles like mon, osd etc.
        """
        self._name = name
        self._namespace = namespace
        self._labels = labels
        self._roles = roles
        # TODO: get backend config !!

    @property
    def name(self):
        return self._name

    @property
    def namespace(self):
        return self._namespace

    @property
    def roles(self):
        return self._roles

    @property
    def labels(self):
        return self._labels

    def set_role(self, role):
        """
        Set a role for this pod
        Args:
            role (str): New role to be assigned for this pod
        """
        self._roles.append(role)

    def exec_command(self, **kw):
        """ Handles execution of a command on a pod
        This function crates an Exec object and runs command through that
        object, Exec object handles all the backend spicific details like
        whether to use rest_apis OR kubernetes client to perform task

        Args:
            kw (dict): Dict of key and value from which command along with
                       options and exec_cmd options as well

            typically args looks like:
                cmd = ['bash', '-c']
                timeout = 60    #timeout for command
                wait = False    #Run command asynchronously at kubernetes API
                                level
                check_ec = True #check error code

        Returns:
            (stdout, stderr, retcode)  #retcode only if check_ec = True

        """
        if kw.get('cmd'):
            cmd = kw['cmd']
            if isinstance(cmd, list):
                cmd = ' '.join(cmd)
        timeout = kw.get('timeout', 60)
        wait = kw.get('wait', True)         # default synchronous execution
        check_ec = kw.get('check_ec', True)
        long_running = kw.get('long_running', False)

        cmd_obj = CmdObj(
            cmd,
            timeout,
            wait,
            check_ec,
            long_running,
        )

        runner = Exec()
        stdout, stderr, err = runner.run(
            self.name,
            self.namespace,
            cmd_obj,
        )

        if check_ec:
            return stdout, stderr, err
        else:
            return stdout, stderr
