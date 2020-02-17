import logging
import json
import time

from ocs_ci.ocs import ocp
from ocs_ci.utility.utils import run_cmd
from ocs_ci.framework.testlib import ManageTest

logger = logging.getLogger(__name__)


class TestUpgradeOCP(ManageTest):
    """
    1. check cluster health
    2. check OCP version
    3. perform OCP upgrade
    4. check OCP upgrade process by images (registry, logging, monitoring)
    5. check OCP version
    5. check cluster health:
       #TODO - shell not be ERROR. can be WARNING
    """

    def test_upgrade_ocp(self):
        """

        Tests OCS stability when upgrading OCP

        """
        target_image = "4.3.0-0.nightly-2020-02-15-174639"
        self.cluster_operators = self.get_all_cluster_operators()

        logger.info(f" OC VERSION: {self.get_current_oc_version()}")

        self.get_all_cluster_operators()

        # Upgrade OCP

        self.upgrade_ocp(image=target_image)

        # Wait for upgrade
        for ocp_operator in self.cluster_operators:
            ver = self.get_cluster_operator_version(ocp_operator)
            logger.info(f"current {ocp_operator} version: {ver}")
            while ver != target_image:
                ver = self.get_cluster_operator_version(ocp_operator)
                time.sleep(30)

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

        return operator_status.get('versions')[0]['version']

    def get_current_oc_version(self):
        """
        Gets Current OCP client version

        Returns:
            str: current COP client version

        """
        oc_json = run_cmd('oc version -o json')
        oc_dict = json.loads(oc_json)

        return oc_dict.get("openshiftVersion")

    def upgrade_ocp(self, image):
        """
        upgrade OCP version

        Args:
            image (str): image to be installed

        """
        ocp_o = ocp.OCP()
        ocp_o.exec_oc_cmd(
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
        if self.get_current_oc_version() == target_version:
            return True

        return False
