import logging


from ocs_ci.framework.testlib import (
    ocp_upgrade,
    ocs_upgrade,
    pre_upgrade,
    pre_ocp_upgrade,
    pre_ocs_upgrade,
    post_upgrade,
    post_ocp_upgrade,
    post_ocs_upgrade,
)

log = logging.getLogger(__name__)


@ocp_upgrade
def test_ocp_upgrade():
    log.info("ocp_upgrade 30")


@post_ocp_upgrade
def test_post_ocp_upgrade():
    log.info("post_ocp_upgrade 40")


@ocs_upgrade
def test_ocs_upgrade():
    log.info("ocs_upgrade 60")


@post_upgrade
def test_post_upgrade():
    log.info("post_upgrade 80")


@pre_upgrade
def test_pre_upgrade():
    log.info("pre_upgrade 10")


@post_ocs_upgrade
def test_post_ocs_upgrade():
    log.info("post_ocs_upgrade 70")


@pre_ocp_upgrade
def test_pre_ocp_upgrade():
    log.info("pre_ocp_upgrade 20")


@pre_ocs_upgrade
def test_pre_ocs_upgrade():
    log.info("pre_ocs_upgrade 50")


# ORDER_BEFORE_UPGRADE = 10
# ORDER_BEFORE_OCP_UPGRADE = 20
# ORDER_OCP_UPGRADE = 30
# ORDER_AFTER_OCP_UPGRADE = 40
# ORDER_BEFORE_OCS_UPGRADE = 50
# ORDER_OCS_UPGRADE = 60
# ORDER_AFTER_OCS_UPGRADE = 70
# ORDER_AFTER_UPGRADE = 80
