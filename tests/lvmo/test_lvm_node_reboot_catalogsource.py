import logging

from ocs_ci.framework.pytest_customization.marks import tier4a, skipif_lvm_not_installed
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, bugzilla
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.resources.pod import wait_for_lvm_pod_running
from ocs_ci.ocs.exceptions import CommandFailed, CatalogSourceNotFoundAfterReboot
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.deployment.deployment import create_catalog_source
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.node import get_node_objs

logger = logging.getLogger(__name__)


@bugzilla("2122785")
@tier4a
@skipif_lvm_not_installed
@skipif_ocs_version("<4.11")
class TestLvmCatalogSourceNodeReboot(ManageTest):
    """
    Test lvm snapshot bigger than disk

    """

    number_of_reboots = 10

    def test_node_reboot_sno_catalogsource(
        self,
        nodes,
    ):
        """
        test reboot multiple time to check catalogsource existence
        .* Reboot node
        .* Check that catalogsource exists
        .* repeat number_of_reboots

        """
        lvm = LVM()

        image = lvm.image
        catalogsource_not_found_occurrences = 0
        nodes_ocs = get_node_objs()
        nodes_names = []
        for reboot in range(1, (self.number_of_reboots + 1)):
            for node in nodes_ocs:
                nodes_names.append(node.data["metadata"]["name"])
            nodes.restart_nodes(nodes_ocs, force=False, wait=False)

            wait_for_nodes_status(node_names=nodes_names)

            wait_for_lvm_pod_running()
            logger.info(f"++++++++++++++++++ In reboot {reboot} ++++++++++++++++++")
            try:
                LVM()
            except CommandFailed as er:
                logger.error(
                    f"CatalogSource "
                    f"{constants.OPERATOR_CATALOG_SOURCE_NAME} not found in reboot {reboot}. {er}"
                    f" Re-creating and continuing for statistics. Gathering events"
                )
                retrieve = run_cmd(
                    cmd="oc get events --all-namespaces --sort-by='.metadata.creationTimestamp'"
                )
                splitted = retrieve.split("\n")
                for line in splitted:
                    if "redhat-oper" in line and "Normal" not in line:
                        logger.info(f"** {line}")

                create_catalog_source(image=image)
                catalogsource_not_found_occurrences += 1
        if catalogsource_not_found_occurrences > 0:
            raise CatalogSourceNotFoundAfterReboot(
                f"Catalogsource {constants.OPERATOR_CATALOG_SOURCE_NAME} not found"
                f"in {catalogsource_not_found_occurrences} "
                f"out of {self.number_of_reboots} reboots"
            )
