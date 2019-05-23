# This is just example of test for pytest

# import logging
# import pytest
# from ocsci.config import ENV_DATA
# from ocsci import run_this, EcosystemTest


# logger = logging.getLogger(__name__)


# @run_this
# def test_not_run_me_fail_pass():
#     logger.info("Hey from test which should pass")
#     logger.info(
#         "You can easily access data from ENV_DATA like cluster_name: %s",
#         ENV_DATA['cluster_name']
#     )
#     assert 1 == 1, "This will not reach this message"


# class TestExampleClass(EcosystemTest):
#     def test_example_method(self):
#         logger.info("Hello from test method inside test class")
