import logging

from ocs_ci.framework.testlib import tier1

logger = logging.getLogger(__name__)


@tier1
def test_dahorak_pass1():
    logger.info("pass 1")
    assert True


@tier1
def test_dahorak_pass2():
    logger.info("pass 2")
    assert True


@tier1
def test_dahorak_pass3():
    logger.info("pass 3")
    assert True


@tier1
def test_dahorak_pass4():
    logger.info("pass 4")
    assert True


@tier1
def test_dahorak_pass5():
    logger.info("pass 5")
    assert True


@tier1
def test_dahorak_fail1():
    logger.info("fail 1")
    assert False


@tier1
def test_dahorak_fail2():
    logger.info("fail 2")
    assert False


@tier1
def test_dahorak_fail3():
    logger.info("fail 3")
    assert False


@tier1
def test_dahorak_fail4():
    logger.info("fail 4")
    assert False


@tier1
def test_dahorak_fail5():
    logger.info("fail 5")
    assert False
