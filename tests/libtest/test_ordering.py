from ocs_ci.framework.pytest_customization.marks import (
    post_ocs_upgrade, post_upgrade, ocs_upgrade, pre_ocp_upgrade, pre_upgrade
)


@post_upgrade
def test_post_upgrade():
    print("Testing post upgrade")


@post_ocs_upgrade
def test_post_ocs_upgrade():
    print("Testing post ocs upgrade")


@ocs_upgrade
def test_ocs_upgrade():
    print("Testing ocs upgrade")


@pre_ocp_upgrade
def test_pre_ocp_upgrade():
    print("Testing pre ocp upgrade")


@pre_upgrade
def test_pre_upgrade():
    print("Testing pre upgrade")

