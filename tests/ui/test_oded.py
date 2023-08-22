import logging

global_test_logger = logging.getLogger("global_logger")


def test_example():
    global_test_logger.info("This is an INFO message.")
    global_test_logger.error("This is an ERROR message.")
    assert 1 == 1  # Some test assertion
