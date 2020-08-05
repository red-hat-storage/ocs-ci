# -*- coding: utf8 -*-

import pytest

from ocs_ci.deployment.deployment import Deployment


def test_deployment_init_fails_without_default_storageclass():
    """
    Test that without DEFAULT_STORAGECLASS value, Deployment class can't be
    instantiated.
    """
    with pytest.raises(NotImplementedError):
        Deployment()
