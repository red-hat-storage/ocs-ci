import logging

import pytest

from ocs_ci.utility.lvmo_utils import delete_lvm_cluster

log = logging.getLogger(__name__)


@pytest.fixture()
def remove_lvm_cluster():
    delete_lvm_cluster()
