from ocs_ci.framework.testlib import tier1


@tier1
def test_fail():
    raise Exception
