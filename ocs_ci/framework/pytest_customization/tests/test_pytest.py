# -*- coding: utf-8 -*-

import logging
import textwrap
from pathlib import Path

import git
import pytest
from pytest import fixture

from ocs_ci import framework
from ocs_ci.framework.main import init_ocsci_conf
from tests import conftest as test_conftest

pytest_plugins = [
    "pytester",
]


logger = logging.getLogger(__name__)


@fixture(autouse=True)
def reset_config():
    framework.config.reset()


def test_pytest_works():
    logger.info("This is prove that pytest works")


def test_help_message(testdir):
    """
    Check that ``py.test --help`` output lists custom options of
    ocscilib pytest plugin.
    """
    testdir.makeconftest(
        textwrap.dedent(
            """
        pytest_plugins = ['ocs_ci.framework.pytest_customization.ocscilib']
    """
        )
    )
    result = testdir.runpytest("--help")
    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(
        [
            "*--ocsci-conf*",
            "*--cluster-path*",
            "*--cluster-name*",
        ]
    )


def test_simple(testdir):
    """
    Make sure that  pytest itself is not broken by running very simple test.
    """
    # create a temporary pytest test module
    testdir.makepyfile(
        textwrap.dedent(
            """\
        def test_foo():
            assert 1 == 1
        """
        )
    )
    # run pytest with the following cmd args
    result = testdir.runpytest("-v")
    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(["*::test_foo PASSED*"])
    # make sure that that we get a '0' exit code for the testsuite
    assert result.ret == 0


def test_config_parametrize(testdir, tmpdir):
    """
    Parametrization via config values, use case described in
    https://github.com/red-hat-storage/ocs-ci/pull/61#issuecomment-494866745
    """
    testdir.makeconftest(
        textwrap.dedent(
            """
        pytest_plugins = ['ocs_ci.framework.pytest_customization.ocscilib']
    """
        )
    )
    # create a temporary pytest test module
    testdir.makepyfile(
        textwrap.dedent(
            """\
        import pytest

        from ocs_ci.framework import config as ocsci_config

        @pytest.mark.parametrize("item", ocsci_config.RUN)
        def test_demo_parametrized_config(item):
            assert item is not None
        """
        )
    )
    # create config file
    conf_file = testdir.makefile(
        ".yaml",
        textwrap.dedent(
            """\
        RUN:
          things:
            - 1
            - 2
        """
        ),
    )
    pytest_arguments = [
        "-v",
        f"--ocsci-conf={conf_file}",
        f"--cluster-path={tmpdir}",
        "--cluster-name=fake-cluster",
    ]
    # this is a bit hack which allow us init all the config which we do in
    # runner run_ocsci.py. Without this we won't be able to access config
    init_ocsci_conf(pytest_arguments)
    # run pytest with the following pytest_argumetns
    result = testdir.runpytest(*pytest_arguments)
    # Build a list of lines we expect to see in the output
    run_defaults = framework.config.get_defaults()["RUN"]
    expected_items = list(run_defaults.keys()) + ["things"]
    expected_lines = [f"collecting*collected {len(expected_items)} items"]
    expected_lines.extend(
        [f"*test_demo_parametrized_config?{key}? PASSED*" for key in expected_items]
    )
    # fnmatch_lines does an assertion internally
    result.stdout.fnmatch_lines(expected_lines)
    # make sure that that we get a '0' exit code for the testsuite
    assert result.ret == 0


@fixture
def git_test_repository(tmp_path, monkeypatch):
    """Provide an isolated Git repository for change-detection tests."""
    repo = git.Repo.init(tmp_path)
    with repo.config_writer() as config_writer:
        config_writer.set_value("user", "name", "Test")
        config_writer.set_value("user", "email", "test@example.com")
    (tmp_path / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    polarion_id\n    blue_squad\n"
    )
    monkeypatch.setattr(test_conftest.constants, "TOP_DIR", str(tmp_path))
    return repo, tmp_path


@pytest.mark.parametrize(
    "before,after,expected",
    [
        pytest.param(None, "def test_new(): pass\n", {"test_new"}, id="new-file"),
        pytest.param(
            None,
            "from pytest import raises as test_imported\n",
            {"test_imported"},
            id="imported-test",
        ),
        pytest.param(
            "from pytest import raises as test_imported\n",
            "from pytest import raises as test_imported\ndef test_new(): pass\n",
            {"test_new"},
            id="unchanged-imported-test",
        ),
        pytest.param(
            "from pytest import raises as test_imported\n",
            "from pytest import warns as test_imported\n",
            {"test_imported"},
            id="changed-imported-test",
        ),
        pytest.param(
            "def test_old(): pass\n",
            "def test_old(): pass\ndef test_new(): pass\n",
            {"test_new"},
            id="new-test",
        ),
        pytest.param(
            "def test_legacy(): pass\n",
            "import math\ndef test_legacy(): pass\ndef test_new(): assert math.sqrt(4) == 2\n",
            {"test_new"},
            id="new-import-for-new-test",
        ),
        pytest.param(
            "from math import sqrt\ndef test_legacy(): assert sqrt(4) == 2\n",
            (
                "from math import sqrt, ceil\n"
                "def test_legacy(): assert sqrt(4) == 2\n"
                "def test_new(): assert ceil(1.1) == 2\n"
            ),
            {"test_new"},
            id="extended-import-for-new-test",
        ),
        pytest.param(
            "from pytest import mark\n@mark.blue_squad\ndef test_legacy(): pass\n",
            (
                "from pytest import mark, raises\n"
                "@mark.blue_squad\n"
                "def test_legacy(): pass\n"
                "@mark.blue_squad\n"
                "@mark.polarion_id('OCS-4483')\n"
                "def test_new(): assert callable(raises)\n"
            ),
            {"test_new"},
            id="extended-marker-import-for-new-test",
        ),
        pytest.param(
            "def test_legacy(): pass\n",
            "LIMIT = 2\ndef test_legacy(): pass\ndef test_new(): assert LIMIT == 2\n",
            {"test_new"},
            id="new-constant-for-new-test",
        ),
        pytest.param(
            "def test_legacy(): pass\n",
            "def helper(): return True\ndef test_legacy(): pass\ndef test_new(): assert helper()\n",
            {"test_new"},
            id="new-helper-for-new-test",
        ),
        pytest.param(
            (
                "VALUES = [1]\n"
                "def values(): return VALUES\n"
                "@parametrize('value', values())\n"
                "def test_case(value): pass\n"
            ),
            (
                "VALUES = [1, 2]\n"
                "def values(): return VALUES\n"
                "@parametrize('value', values())\n"
                "def test_case(value): pass\n"
            ),
            {"test_case"},
            id="changed-parameter-provider-data",
        ),
        pytest.param(
            "pytestmark = polarion_id('OCS-4483')\ndef test_case(): pass\n",
            "def test_case(): pass\n",
            {"test_case"},
            id="removed-module-marker",
        ),
        pytest.param(
            "@polarion_id('OCS-4483')\nclass TestCases:\n    def test_case(self): pass\n",
            "class TestCases:\n    def test_case(self): pass\n",
            {"TestCases::test_case"},
            id="removed-class-marker",
        ),
        pytest.param(
            "def test_first(): pass\ndef test_second(): pass\n",
            "def test_first(): assert True\ndef test_second(): pass\n",
            {"test_first"},
            id="changed-body",
        ),
        pytest.param(
            "@polarion_id('OCS-4483')\ndef test_case(): pass\n",
            "def test_case(): pass\n",
            {"test_case"},
            id="removed-marker",
        ),
        pytest.param(
            "@parametrize('value', [1])\ndef test_case(value): pass\n",
            "@parametrize('value', [1, 2])\ndef test_case(value): pass\n",
            {"test_case"},
            id="changed-parameters",
        ),
        pytest.param(
            "class TestCases:\n    def test_first(self): pass\n    def test_second(self): pass\n",
            "class TestCases:\n    def test_first(self): assert True\n    def test_second(self): pass\n",
            {"TestCases::test_first"},
            id="changed-method",
        ),
        pytest.param(
            "@parametrize('value', [1])\nclass TestCases:\n    def test_case(self, value): pass\n",
            "@parametrize('value', [1, 2])\nclass TestCases:\n    def test_case(self, value): pass\n",
            {"TestCases::test_case"},
            id="changed-class-parameters",
        ),
        pytest.param(
            "VALUES = [1]\n@parametrize('value', VALUES)\ndef test_case(value): pass\n",
            "VALUES = [1, 2]\n@parametrize('value', VALUES)\ndef test_case(value): pass\n",
            {"test_case"},
            id="changed-shared-parameters",
        ),
        pytest.param(
            "def test_case():\n    pass\n",
            "# Comment\ndef test_case( ):\n    pass  # Comment\n",
            set(),
            id="comments-and-formatting",
        ),
        pytest.param(
            "def test_old(): pass\ndef test_kept(): pass\n",
            "def test_kept(): pass\n",
            set(),
            id="deleted-test",
        ),
    ],
)
def test_changed_test_nodeids(git_test_repository, testdir, before, after, expected):
    """Select changed definitions without selecting unrelated legacy tests."""
    repo, root = git_test_repository
    test_file = root / "tests" / "test_cases.py"
    test_file.parent.mkdir()
    base_file = test_file if before is not None else root / "README.md"
    prologue = "import pytest\nparametrize = pytest.mark.parametrize\npolarion_id = pytest.mark.polarion_id\n"
    base_file.write_text(prologue + before if before is not None else "Base\n")
    repo.index.add([str(base_file.relative_to(root))])
    base_commit = repo.index.commit("Base tests")
    repo.git.update_ref("refs/remotes/origin/master", base_commit.hexsha)
    test_file.write_text(prologue + after)
    repo.index.add(["tests/test_cases.py"])
    items = testdir.getitems(test_file)
    assert test_conftest.get_changed_test_nodeids(items, "master") == {
        f"tests/test_cases.py::{name}" for name in expected
    }


def test_changed_test_nodeids_uses_merge_base(git_test_repository, testdir):
    repo, root = git_test_repository
    test_file = root / "tests" / "test_cases.py"
    test_file.parent.mkdir()
    test_file.write_text("def test_first(): pass\ndef test_second(): pass\n")
    repo.index.add(["tests/test_cases.py"])
    base_commit = repo.index.commit("Base tests")
    feature = repo.create_head("feature", base_commit)
    feature.checkout()
    test_file.write_text("def test_first(): assert True\ndef test_second(): pass\n")
    repo.index.add(["tests/test_cases.py"])
    repo.index.commit("Change first test")
    target = repo.create_head("target", base_commit)
    target.checkout()
    test_file.write_text("def test_first(): pass\ndef test_second(): assert True\n")
    repo.index.add(["tests/test_cases.py"])
    target_commit = repo.index.commit("Change second test")
    repo.git.update_ref("refs/remotes/origin/master", target_commit.hexsha)
    feature.checkout()
    items = testdir.getitems(test_file)
    assert test_conftest.get_changed_test_nodeids(items, "master") == {
        "tests/test_cases.py::test_first"
    }


@pytest.mark.parametrize("inherited_id", [False, True])
def test_polarion_validation_checks_each_variant(testdir, monkeypatch, inherited_id):
    testdir.makeini("[pytest]\nmarkers =\n    blue_squad\n    polarion_id\n")
    test_file = Path(str(testdir.tmpdir.mkdir("tests"))) / "test_variants.py"
    marker = '@pytest.mark.polarion_id("OCS-4483")\n' if inherited_id else ""
    test_file.write_text(
        "import pytest\n"
        "@pytest.mark.blue_squad\n"
        "@pytest.mark.parametrize('value', [\n"
        "    pytest.param(1, marks=pytest.mark.polarion_id('OCS-4483')),\n"
        "    pytest.param(2),\n"
        "], ids=['with-id', 'without-id'])\n"
        f"{marker}"
        "def test_changed(value): pass\n"
        "@pytest.mark.blue_squad\n"
        "def test_legacy(): pass\n"
    )
    items = testdir.getitems(test_file)
    changed_test = next(item for item in items if item.name.startswith("test_changed"))
    changed_nodeid = changed_test.nodeid.split("[", 1)[0]
    monkeypatch.setattr(test_conftest.constants, "TOP_DIR", str(testdir.tmpdir))
    monkeypatch.setattr(
        test_conftest,
        "get_changed_test_nodeids",
        lambda items, base_branch: {changed_nodeid},
    )
    if inherited_id:
        test_conftest.verify_test_decorators_requirements(items)
        return
    with pytest.raises(test_conftest.MissingDecoratorError) as error:
        test_conftest.verify_test_decorators_requirements(items)
    message = str(error.value)
    assert (
        "test_changed[without-id]" in message
        and "test_changed[with-id]" not in message
        and "test_legacy" not in message
    ), message


def test_changed_test_nodeids_handles_empty_collection():
    assert test_conftest.get_changed_test_nodeids([], "master") == set()


def test_changed_test_nodeids_rejects_empty_base_branch():
    with pytest.raises(ValueError, match="target branch"):
        test_conftest.get_changed_test_nodeids([], "")
