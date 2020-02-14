# -*- coding: utf8 -*-

import pytest

from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.ocs.exceptions import ResourceNameNotSpecifiedException


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


def test_pm_get_default_channel_missing():
    """
    Test that when we run into issue #1338, PackageManifest object still
    behaves in the same way as before.

    This unit test serves two purposes:
    - to show what exactly happens to PackageManifest during issue #1338
    - demonstrate that PackageManifest API remains unchanged
    """
    pm = PackageManifest(resource_name='foobar')
    # based on value of _data attribute when packagemanifest data are missing
    # as reported in https://github.com/red-hat-storage/ocs-ci/issues/1338
    pm._data = {
        'apiVersion': 'v1',
        'items': [],
        'kind': 'List',
        'metadata': {'resourceVersion': '', 'selfLink': ''}}
    with pytest.raises(KeyError):
        pm.get_default_channel()
