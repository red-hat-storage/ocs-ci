# -*- coding: utf8 -*-
from unittest.mock import patch

import pytest

from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.ocs.exceptions import (
    ResourceNameNotSpecifiedException,
    ResourceNotFoundError,
)


def test_pm_null():
    """
    Test that creation of PackageManifest object without any constructor
    agruments works (object is created, no exceptions are raised).
    """
    PackageManifest()


def test_pm_null_get_default_channel():
    pm = PackageManifest()
    with pytest.raises(ResourceNameNotSpecifiedException):
        pm.get_default_channel()


def test_no_resource_found_for_packagemanifest():
    """
    Test that when we run into issue #1338, when no PackageManifest object
    found.

    This unit test serves two purposes:
    - to show what exactly happens to PackageManifest during issue #1338
    - demonstrate that PackageManifest API remains unchanged
    """
    # based on value of _data attribute when packagemanifest data are missing
    # as reported in https://github.com/red-hat-storage/ocs-ci/issues/1338
    data_with_no_item = {
        'apiVersion': 'v1',
        'items': [],
        'kind': 'List',
        'metadata': {'resourceVersion': '', 'selfLink': ''}
    }
    with patch("ocs_ci.ocs.ocp.OCP.get", return_value=data_with_no_item):
        pm = PackageManifest(resource_name='foo', selector='bar')
        with pytest.raises(ResourceNotFoundError):
            pm.get_default_channel()
