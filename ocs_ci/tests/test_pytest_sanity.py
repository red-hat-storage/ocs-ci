# -*- coding: utf8 -*-

import os
import logging

logger = logging.getLogger(__name__)


def test_nontest_code_unexpected_in_tests_dir():
    """
    Make sure that in tests directory, there are only test code (starts with
    ``test_`` prefix), ``conftest.py`` or ``__init__.py`` files - nothing else.
    """
    invalid_files = []
    for root, dirs, files in os.walk("tests"):
        for name in files:
            # we care only about python code
            if not name.endswith(".py"):
                continue
            # valid filenames
            if name in ["__init__.py", "conftest.py"]:
                continue
            # valid prefixes
            if name.startswith("test_"):
                continue
            # anything else in not valid, and should not be in tests directory
            logger.error("file %s should not be present in tests dir", name)
            invalid_files.append(os.path.join(root, name))
    # workaround for https://github.com/red-hat-storage/ocs-ci/issues/200
    invalid_files.remove("tests/fixtures.py")
    assert invalid_files == []
