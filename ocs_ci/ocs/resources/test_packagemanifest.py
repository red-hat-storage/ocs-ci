# -*- coding: utf8 -*-

import pytest

from ocs_ci.ocs.resources.packagemanifest import PackageManifest
from ocs_ci.ocs.exceptions import ResourceNameNotSpecifiedException


def test_pm_null():
    pm = PackageManifest()
    assert pm is not None


def test_pm_null_get_default_channel():
    pm = PackageManifest()
    with pytest.raises(ResourceNameNotSpecifiedException):
        pm.get_default_channel()


def test_pm_get_default_channel_missing():
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
