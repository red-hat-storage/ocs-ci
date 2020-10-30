# -*- coding: utf8 -*-
"""
This unit test tries to just import every module within ocs_ci module, to
detect serious errors (eg. a syntax error) or dependency problems.
"""

import importlib
import pkgutil

import pytest


def list_submodules(module_path, module_prefix):
    """
    For given module, return list of full module path names for all submodules
    recursively.
    """
    return [
        name
        for _, name, _ in pkgutil.walk_packages(
            path=[module_path], prefix=module_prefix
        )
    ]


# parametrize makes this case run for every submodule in ocs_ci module
@pytest.mark.parametrize("module", list_submodules("ocs_ci", "ocs_ci."))
def test_import(module):
    """
    Just try to import given module. In case of any error, test fails on import
    exception.
    """
    importlib.import_module(module)
