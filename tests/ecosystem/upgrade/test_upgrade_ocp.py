import logging
import json

from ocs_ci.framework import Config
from ocs_ci.ocs import ocp
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CephHealthException

logger = logging.getLogger(__name__)

# TODO: add image type validation (ga to ga , nightly to nightly, newer than current etc.)


class TestUpgradeOCP(ManageTest):
    """
    1. check cluster health
    2. check OCP version
    3. perform OCP upgrade
    4. check all OCP ClusterOperators
    5. check OCP version
    5. monitor cluster health
    """
    ocp_o = ocp.OCP()

    def test_upgrade_ocp(self):
        """

        Tests OCS stability when upgrading OCP

        """
        config = Config()
        ceph_cluster = CephCluster()
        ceph_cluster.enable_health_monitor()

        target_image = config.UPGRADE['ocp_upgrade_version']
        self.cluster_operators = self.get_all_cluster_operators()
        logger.info(f" oc version: {self.get_current_oc_version()}")
        self.get_all_cluster_operators()
        upgrade_operator_timeout = TimeoutSampler(
            timeout=2700,
            sleep=60,
            func=self.check_upgrade_completed,
            target_version=target_image
        )

        # Upgrade OCP

        self.upgrade_ocp(image=target_image)

        # Wait for upgrade
        for ocp_operator in self.cluster_operators:
            logger.info(f"Checking upgrade status of {ocp_operator}:")
            ver = self.get_cluster_operator_version(ocp_operator)
            logger.info(f"current {ocp_operator} version: {ver}")
            upgrade_operator_timeout.wait_for_func_status(result=True)

        # validate cluster status and health
        ceph_cluster.disable_health_monitor()
        if ceph_cluster.health_error_status:
            CephHealthException(
                f"During upgrade hit Ceph HEALTH_ERROR: "
                f"{ceph_cluster.health_error_status}"
            )

    def get_all_cluster_operators(self):
        """
        Get all ClusterOperators names in OCP

        Returns:
            list: cluster-operator names

        """
        ocp_obj = ocp.OCP(kind='ClusterOperator')
        operator_info = ocp_obj.get("-o name", out_yaml_format=False, all_namespaces=True)
        operators_full_names = str(operator_info).split()
        operator_names = list()
        for name in operators_full_names:
            splitted = name.split('/')
            for part in splitted:
                if part == 'clusteroperator.config.openshift.io':
                    splitted.remove(part)
            operator_names.append(splitted[0])

        logger.info(f"ClusterOperators full list: {operator_names}")
        return operator_names

    def get_cluster_operator_version(self, cluster_operator_name):
        """
        Get image version of selected cluster operator

        Args:
            cluster_operator_name (str): ClusterOperator name

        Returns:
            str: cluster operator version: ClusterOperator image version

        """
        ocp_obj = ocp.OCP(kind='ClusterOperator')
        operator_info = ocp_obj.get(cluster_operator_name)
        operator_status = operator_info.get('status')

        version = operator_status.get('versions')[0]['version']
        if version.endswith('_openshift'):
            return version[:-10]
        else:
            return version

    def get_current_oc_version(self):
        """
        Gets Current OCP client version

        Returns:
            str: current COP client version

        """
        oc_json = self.ocp_o.exec_oc_cmd('oc version -o json', out_yaml_format=False)
        oc_dict = json.loads(oc_json)

        return oc_dict.get("openshiftVersion")

    def upgrade_ocp(self, image):
        """
        upgrade OCP version

        Args:
            image (str): image to be installed

        """
        self.ocp_o.exec_oc_cmd(
            f"adm upgrade --to-image=registry.svc.ci.openshift.org/ocp/release:{image} "
            f"--allow-explicit-upgrade --force "
        )
        logger.info(f"Upgrading OCP to version: {image}")

        return 0

    def check_upgrade_completed(self, target_version):
        """
        Check if OCP upgrade process is completed

        Args:
            target_version (str): expected OCP client

        Returns:
            bool: True if success, False if failed

        """
        logger.info(f"current version: {self.get_current_oc_version}")
        if self.get_current_oc_version() == target_version:
            logger.info(f" cluster operator upgrade to build {target_version} completed")
            return True

        logger.debug(f"upgrade not completed")
        return False
