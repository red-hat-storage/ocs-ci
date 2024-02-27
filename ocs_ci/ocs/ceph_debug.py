import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.resources.deployment import get_osd_deployments, get_mon_deployments

logger = logging.getLogger(__name__)


class RookCephPlugin(object):
    """
    This helps you put the Mon/OSD deployments in debug mode without scaling down the rook-operator
    or other steps involved using krew plugin. This will also take care of the plugin installation
    if not already installed. Generally debug pods are used to perform debug ops, but they can also
    be used for maintenance purpose. One can use offline tools like ceph-objectstore-tool, ceph-monstore-tool
    using debug pods.
    e.g: List all PGs: $ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-0 --op --list-pgs
    """

    krew_install_cmd = f"sh {constants.KREW_INSTALL_DIR}/krew_install.sh"
    rookceph_install_cmd = f"sh {constants.KREW_INSTALL_DIR}/rookcephplugin_install.sh"

    def __init__(
        self,
        namespace=config.ENV_DATA["cluster_namespace"],
        operator_namespace=config.ENV_DATA["cluster_namespace"],
        alternate_image=None,
        *args,
        **kwargs,
    ):
        self.namespace = namespace
        self.operator_namespace = operator_namespace
        self.alternate_image = alternate_image
        self.cmd = f"rook-ceph -n {namespace} --operator-namespace {operator_namespace}"
        self.deployment_in_debug = dict()

        if not self.check_krew_installed():
            try:
                self.install_krew()
            except Exception as ex:
                logger.error("[Failed] Krew installation failed!")
                raise ex
            logger.info("Krew installed successfully!")
        else:
            logger.info("Krew is already installed!")

        if not self.check_for_rook_ceph():
            try:
                self.install_rook_ceph_plugin()
            except Exception as ex:
                logger.error("[Failed] rook-ceph plugin installation failed")
                raise ex
            logger.info("Rook-ceph installed successfully!")
        else:
            logger.info("Rook-ceph is already installed!")

    def check_krew_installed(self):
        """
        Checks if krew is installed already

        Returns:
            bool: True if installed, False otherwise

        """
        installed = True
        try:
            exec_cmd(cmd="kubectl krew")
        except Exception as ex:
            if 'unknown command "krew" for "kubectl"' in ex.args[0]:
                installed = False
        return installed

    def check_for_rook_ceph(self):
        """
        Checks if rook-ceph plugin is installed

        Returns:
            bool: True if installed, False otherwise

        """
        installed = True
        try:
            exec_cmd(cmd="kubectl rook-ceph --help")
        except Exception as ex:
            if 'unknown command "rook-ceph" for "kubectl"' in ex.args[0]:
                installed = False
        return installed

    def install_krew(self):
        """
        Install krew
        """
        exec_cmd(cmd=self.krew_install_cmd)
        return True

    def install_rook_ceph_plugin(self):
        """
        Install rook-ceph plugin
        """
        exec_cmd(cmd=self.rookceph_install_cmd)
        return True

    def debug_start(self, deployment_name, alternate_image=None, timeout=800):
        """
        This starts the debug mode for the deployment

        Args:
            deployment_name (str): Name of the deployment that you want
            it to be in debug mode i.e, either Mon or OSD deployments

            alternate_image (str): Alternate image that you want to pass

        Returns:
              True: if debug start is successful

        """

        if deployment_name in self.deployment_in_debug.keys():
            raise Exception(
                f"[Error] Deployment {deployment_name} seems to be already in debug mode!"
            )

        command = self.cmd
        command += f" debug start {deployment_name}"
        if alternate_image:
            self.alternate_image = alternate_image
            command += f" --alternate-image {self.alternate_image}"
        OCP().exec_oc_cmd(command=command, timeout=timeout, out_yaml_format=False)
        logger.info(f"{deployment_name} is successfully in mainetenance mode now!")

        self.deployment_in_debug[deployment_name] = True
        return True

    def debug_stop(self, deployment_name, alternate_image=None, timeout=800):
        """
        This stops the debug mode for the deployment

        Args:
            alternate_image (str): Alternate image that you want to pass

        Returns:
            True: if debug stop is successful

        """
        if deployment_name not in self.deployment_in_debug.keys():
            raise Exception("[Error] Deployment not in debug mode")

        # TODO: Make sure deployment is either mon or osd

        command = self.cmd
        command += f" debug stop {deployment_name}"
        if alternate_image:
            self.alternate_image = alternate_image
            command += f" --alternate-image {self.alternate_image}"
        OCP().exec_oc_cmd(command=command, timeout=timeout, out_yaml_format=False)
        logger.info(
            f"{deployment_name} is successfully removed from mainetenance mode now!"
        )

        self.deployment_in_debug.pop(deployment_name)
        return True


class CephObjectStoreTool(RookCephPlugin):
    """
    This is to perform COT related operations on OSD debug pod
    We can extend this class in future to perform various other ceph-objectstore-tool operations
    """

    def __init__(
        self, deployment_name=None, data_path="/var/lib/ceph/osd/", *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data_path = data_path
        self.cot_cmd = constants.CEPHOBJECTSTORE_TOOL_CMD
        self.deployment_name = deployment_name

    def __validate_deployment(self, deployment_name):
        """
        Validate if the deployment is debug mode
        before performing COT operations

        Args:
            deployment_name(str): Name of the deployment

        """
        if not deployment_name:
            if not self.deployment_name:
                raise Exception(
                    "Need to pass the deployment_name either when initialising "
                    "the CephObjectStoreTool or while running the COT!!"
                )
        if deployment_name not in self.deployment_in_debug.keys():
            raise Exception("Please put the osd deployment in debug mode first!")
        else:
            self.deployment_name = deployment_name

        return True

    def __get_data_path(self):
        """
        Get --data-path for deployment based on osd-id
        """
        data_path = self.data_path
        for deployment in get_osd_deployments():
            if deployment.name == f"{self.deployment_name}-debug":
                osd_id = deployment.pods[0].labels["ceph-osd-id"]
                data_path += f"ceph-{osd_id}"
        return data_path

    def __get_osd_debug_pod(self):
        """
        Get osd pod corresponding to the osd deployment
        """
        debug_pod = " "
        for deployment in get_osd_deployments():
            if deployment.name == f"{self.deployment_name}-debug":
                debug_pod = deployment.pods[0]
        return debug_pod

    def run_cot_list_pgs(self, deployment_name):
        """
        Run COT list PG operation

        Args:
            deployment_name: Name of the original deployment thats in debug

        Returns:
            pgs: List of PGS

        """
        self.__validate_deployment(deployment_name)
        data_path = self.__get_data_path()
        command = self.cot_cmd + f" --data-path {data_path} --op list-pgs"
        pgs = self.__get_osd_debug_pod().exec_cmd_on_pod(command=command).split(" ")
        return pgs


class MonStoreTool(RookCephPlugin):
    """
    This is to perform MonStoreTool related operations on Mon debug pod
    We can extend this class in future to perform various other ceph-monstore-tool operations
    """

    def __init__(
        self, deployment_name=None, store_path="/var/lib/ceph/mon/", *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.store_path = store_path
        self.mot_cmd = constants.CEPHMONSTORE_TOOL_CMD
        self.deployment_name = deployment_name

    def __validate_deployment(self, deployment_name):
        """
        Validate if the deployment is debug mode
        before performing MonStoreTool operations

        Args:
            deployment_name(str): Name of the deployment

        """
        if not deployment_name:
            if not self.deployment_name:
                raise Exception(
                    "Need to pass the deployment_name either when initialising "
                    "the CephObjectStoreTool or while running the COT!!"
                )
        if deployment_name not in self.deployment_in_debug.keys():
            raise Exception("Please put the osd deployment in debug mode first!")
        else:
            self.deployment_name = deployment_name

        return True

    def __get_store_path(self):
        """
        This returns the store-path for the corresponding
        Mon id
        """
        store_path = self.store_path
        for deployment in get_mon_deployments():
            if deployment.name == f"{self.deployment_name}-debug":
                mon_id = deployment.pods[0].labels["ceph_daemon_id"]
                store_path += f"ceph-{mon_id}"
        return store_path

    def __get_mon_debug_pod(self):
        """
        This returns the Mon debug pod for the corresponding
        debug deployment
        """
        debug_pod = " "
        for deployment in get_mon_deployments():
            if deployment.name == f"{self.deployment_name}-debug":
                debug_pod = deployment.pods[0]
        return debug_pod

    def run_mot_get_monmap(self, deployment_name):
        """
        Runs MonStoreTool get monmap operation
        Args:
            deployment_name: deployment name

        Returns:
            str: output for get monmap command

        """
        self.__validate_deployment(deployment_name)
        store_path = self.__get_store_path()
        command = self.mot_cmd + f" {store_path} get monmap -- --out /tmp/monmap"
        out = self.__get_mon_debug_pod().exec_cmd_on_pod(
            command=str(command), ignore_error=True
        )
        return out
