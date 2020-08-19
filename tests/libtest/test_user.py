import logging
import pytest

from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


def test_bucket_creation(user_factory):
    user = user_factory()
    exec_cmd(['oc', 'login', '-u', user[0], '-p', user[1]])
    exec_cmd(['oc', 'logout'])
