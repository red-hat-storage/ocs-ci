""" Dispatcher for proper exec based on api-client config

This class takes care of dispatching a proper api-client instance
to take care of command execution.

WHY this class ?
Execution of command might happen using various means, as of now we are using
kubernetes client but we might also use openshift rest client. To cope with
this we might have to dynamically figure out at run time which backend to use.

This module will have all api client class definitions in this file along with
dispatcher class Exec.

"""


from collections import namedtuple
import logging

from ocs.exceptions import CommandFailed

# Upstream KubernetesClient
from kubernetes import config
from kubernetes.client import Configuration
from kubernetes.client.apis import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream


logger = logging.getLogger(__name__)

""" This dict holds a mapping of stringified class name to class

Used by factory function to dynamically find the class to be instantiated

"""
_clsmap = dict()

"""Packing all elements required for execution """
CmdObj = namedtuple('CmdObj', [
    'cmd',
    'timeout',
    'wait',
    'check_ec',
    'long_running',
])


def register_class(cls):
    """ Decorator for registering a class 'cls' in class map

    Please make sure that api-client name in config should be same as
    class name.

    """
    name = str(cls).split(".")[2].rstrip('\'>')
    _clsmap[name] = cls
    return cls


class Exec(object):
    """ Dispatcher class for proper api client instantiation

    This class has a factory function which returns proper api client instance
    necessary for command execution

    """
    def __init__(self, oc_client='KubClient'):
        self.oc_client = oc_client

    def run(self, podname, namespace, cmd_obj):
        """ actual run happens here
        Command will be fwd to api client object
        and it should return a 3 tuple
        (stdout, stderr, retval)

        """
        # Get api-client specific object
        apiclnt = _clsmap[self.oc_client]()
        logger.info(f"Instantiated api-client {self.oc_client}")
        return apiclnt.run(podname, namespace, cmd_obj)


@register_class
class KubClient(object):
    """ Specific to upstream Kubernetes client library

    """
    def __init__(self):
        """ Api-client environment initialization
        Assumption is KUBERNETES env is set so that client has access to
        oc cluster config.

        """
        config.load_kube_config()
        conf = Configuration()
        conf.assert_hostname = False
        Configuration.set_default(conf)
        self.api = core_v1_api.CoreV1Api()

    def run(self, podname, namespace, cmd_obj):
        resp = None
        stdout = None
        stderr = None
        ret = None

        try:
            resp = self.api.read_namespaced_pod(
                name=podname,
                namespace=namespace
            )
            logger.info(resp)
        except ApiException as ex:
            if ex.status != 404:
                logger.error("Unknown error: %s" % ex)

        # run command in bash
        bash = ['/bin/bash']
        resp = stream(
            self.api.connect_get_namespaced_pod_exec,
            podname,
            namespace,
            command=bash,
            stderr=True,
            stdin=True,
            stdout=True,
            tty=False,
            _preload_content=False
        )
        done = False
        outbuf = ''
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                stdout = resp.read_stdout(timeout=cmd_obj.timeout)
                outbuf = outbuf + stdout
                if cmd_obj.long_running:
                    while resp.peek_stdout(timeout=cmd_obj.timeout):
                        stdout = resp.read_stdout(timeout=cmd_obj.timeout)
                        outbuf = outbuf + stdout
            if resp.peek_stderr():
                stderr = resp.read_stderr(timeout=60)
            if not done:
                resp.write_stdin(cmd_obj.cmd)
                resp.write_stdin('\n')
                done = True
            else:
                break
        """
        Couple of glitches in capturing return value.
        Rest api doesn't return ret value of the command
        hence this workaround.
        we can fix this once we have facility to capture err code
        """
        if cmd_obj.check_ec:
            resp.write_stdin("echo $?\n")
            try:
                ret = int(resp.readline_stdout(timeout=5))
            except (ValueError, TypeError):
                logger.error(
                    f"TimeOut: Command timedout after {cmd_obj.timeout}"
                )
                raise CommandFailed(
                    f"Failed to run \"{cmd_obj.cmd}\""
                )
            finally:
                resp.close()

        if outbuf:
            stdout = outbuf

        return stdout, stderr, ret
