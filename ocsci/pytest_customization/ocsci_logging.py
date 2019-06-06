import logging
import os
import time
import pytest

from ocsci import config as ocsci_config

FORMATTER = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s.%(funcName)s.%(lineno)d"
    " - %(message)s"
)
ep_time = int(time.time())
log_dir = "/logs_" + str(ep_time)
log_dir_path = ocsci_config.RUN['log_dir'] + log_dir
sym_link = ocsci_config.RUN['log_dir'] + "/logs"


def create_directory_path(path):
    """
    Creates directory if path doesn't exists
    """
    if not os.path.exists(path):
        os.makedirs(path)


@pytest.fixture(scope="session", autouse=True)
def create_symlink():
    """
    Creates symbolic link for current test run logs
    """
    # check whether source exists or not. otherwise it creates stale symlinks
    logging.info(f"All logs are located under {sym_link}")
    create_directory_path(log_dir_path)
    if os.path.lexists(sym_link):
        os.remove(sym_link)
    os.symlink(log_dir_path, sym_link)


def pytest_runtest_setup(item):
    """
    Adding unique log handler for each test
    """
    log_structure = item.nodeid.split("::")[0][:-3]
    test_class_name = item.nodeid.split("::")[1]
    log_path = os.path.join(log_dir_path, log_structure, test_class_name)
    test_log = item.name + ".log"
    log_file = os.path.join(log_path, test_log)

    if not os.path.exists(log_path):
        os.makedirs(log_path)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(FORMATTER)

    logging.getLogger('').addHandler(fh)


def pytest_runtest_teardown():
    """
    Removing log handler which was created in func:`pytest_runtest_setup`
    """
    fh = logging.getLogger().handlers[1]
    fh.close()
    logging.getLogger().removeHandler(fh)
