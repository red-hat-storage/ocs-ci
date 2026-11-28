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


@pytest.mark.parametrize(
    "before,after,expected",
    [
        ("", "def test_new(): pass\n", {"test_new"}),
        (
            "def test_first(): pass\ndef test_second(): pass\n",
            "def test_first(): assert True\ndef test_second(): pass\n",
            {"test_first"},
        ),
        (
            "class TestCases:\n    def test_case(self): pass\n",
            "class TestCases:\n    def test_case(self): assert True\n",
            {"TestCases::test_case"},
        ),
        (
            "@polarion_id('OCS-4483')\ndef test_case(): pass\n",
            "def test_case(): pass\n",
            {"test_case"},
        ),
        (
            "@parametrize('value', [1])\ndef test_case(value): pass\n",
            "@parametrize('value', [1, 2])\ndef test_case(value): pass\n",
            {"test_case"},
        ),
        (
            "@polarion_id('OCS-4483')\nclass TestCases:\n    def test_case(self): pass\n",
            "class TestCases:\n    def test_case(self): pass\n",
            {"TestCases::test_case"},
        ),
        (
            "class TestCases:\n    pytestmark = polarion_id('OCS-4483')\n    def test_case(self): pass\n",
            "class TestCases:\n    def test_case(self): pass\n",
            {"TestCases::test_case"},
        ),
        (
            "pytestmark = polarion_id('OCS-4483')\ndef test_case(): pass\n",
            "def test_case(): pass\n",
            {"test_case"},
        ),
        (
            "VALUES = [1]\n@parametrize('value', VALUES)\ndef test_case(value): pass\n",
            "VALUES = [1, 2]\n@parametrize('value', VALUES)\ndef test_case(value): pass\n",
            set(),
        ),
        (
            "@fixture(params=[1])\ndef value(request): return request.param\ndef test_case(value): pass\n",
            "@fixture(params=[1, 2])\ndef value(request): return request.param\ndef test_case(value): pass\n",
            set(),
        ),
        (
            "def test_case(): pass\n",
            "import math\ndef helper(): return 1\ndef test_case(): pass\n",
            set(),
        ),
        (
            "def test_case():\n    pass\n",
            "# Comment\ndef test_case( ):\n    pass  # Comment\n",
            set(),
        ),
        ("def test_removed(): pass\n", "", set()),
    ],
)
def test_test_signatures_compare_direct_definitions(before, after, expected):
    """Shared-data edits and formatting do not select unchanged legacy tests."""
    previous = test_conftest.get_test_signatures(before)
    current = test_conftest.get_test_signatures(after)
    assert {
        name
        for name in current
        if name.rsplit("::", 1)[-1].startswith("test")
        and previous.get(name) != current[name]
    } == expected


@fixture
def git_test_repository(tmp_path, monkeypatch):
    """Provide an isolated Git repository for change-detection tests."""
    repo = git.Repo.init(tmp_path)
    with repo.config_writer() as config_writer:
        config_writer.set_value("user", "name", "Test")
        config_writer.set_value("user", "email", "test@example.com")
    (tmp_path / "pytest.ini").write_text("[pytest]\nmarkers =\n    polarion_id\n")
    monkeypatch.setattr(test_conftest.constants, "TOP_DIR", str(tmp_path))
    return repo, tmp_path


def test_changed_test_nodeids_uses_merge_base(git_test_repository, testdir):
    """Select an edited inherited method, excluding target-branch-only changes."""
    repo, root = git_test_repository
    test_file = root / "test_cases.py"
    test_file.write_text(
        "class Base:\n    def test_first(self): pass\n"
        "class TestCases(Base):\n    def test_second(self): pass\n"
    )
    repo.index.add(["test_cases.py"])
    base_commit = repo.index.commit("Base tests")
    feature = repo.create_head("feature", base_commit)
    feature.checkout()
    test_file.write_text(
        test_file.read_text().replace(
            "test_first(self): pass", "test_first(self): assert True"
        )
    )
    repo.index.add(["test_cases.py"])
    repo.index.commit("Change first test")
    target = repo.create_head("target", base_commit)
    target.checkout()
    test_file.write_text(
        test_file.read_text().replace(
            "test_second(self): pass", "test_second(self): assert True"
        )
    )
    repo.index.add(["test_cases.py"])
    target_commit = repo.index.commit("Change second test")
    repo.git.update_ref("refs/remotes/origin/master", target_commit.hexsha)
    feature.checkout()
    items = testdir.getitems(test_file)
    assert test_conftest.get_changed_test_nodeids(items, "master") == {
        "test_cases.py::TestCases::test_first"
    }


def test_changed_test_nodeids_includes_new_files(git_test_repository, testdir):
    repo, root = git_test_repository
    readme = root / "README.md"
    readme.write_text("Base\n")
    repo.index.add(["README.md"])
    baseline = repo.index.commit("Base repository")
    repo.git.update_ref("refs/remotes/origin/master", baseline.hexsha)
    test_file = root / "test_cases.py"
    test_file.write_text(
        "import pytest\n@pytest.mark.parametrize('value', [1, 2])\n"
        "def test_new(value): pass\n"
    )
    repo.index.add(["test_cases.py"])
    items = testdir.getitems(test_file)
    assert test_conftest.get_changed_test_nodeids(items, "master") == {
        "test_cases.py::test_new"
    }


@pytest.mark.parametrize("encoding", ["utf-8-sig", "latin-1"])
def test_changed_test_nodeids_handles_source_encodings(
    git_test_repository, testdir, encoding
):
    repo, root = git_test_repository
    test_file = root / "test_cases.py"
    source = f"# coding: {encoding}\nLABEL = 'café'\ndef test_case(): pass\n"
    test_file.write_bytes(source.encode(encoding))
    repo.index.add(["test_cases.py"])
    baseline = repo.index.commit("Base tests")
    repo.git.update_ref("refs/remotes/origin/master", baseline.hexsha)
    test_file.write_bytes(source.replace("pass", "assert True").encode(encoding))
    items = testdir.getitems(test_file)
    assert test_conftest.get_changed_test_nodeids(items, "master") == {
        "test_cases.py::test_case"
    }


@pytest.mark.parametrize("marker_scope", [None, "function", "class", "module"])
@pytest.mark.parametrize("fixture_parameters", [False, True])
def test_polarion_validation_checks_each_variant(
    testdir, monkeypatch, marker_scope, fixture_parameters
):
    """Use pytest's effective markers for decorator and fixture variants."""
    testdir.makeini("[pytest]\nmarkers =\n    blue_squad\n    polarion_id\n")
    filename = "test_variants[1].py"
    test_file = Path(str(testdir.tmpdir.mkdir("tests"))) / filename
    source = (
        "import pytest\npytestmark = [pytest.mark.blue_squad]\n"
        "VALUES = [pytest.param(1, marks=pytest.mark.polarion_id('OCS-4483')), 2]\n"
    )
    definition = "def test_changed(value): pass\n"
    if fixture_parameters:
        source += (
            "@pytest.fixture(params=VALUES, ids=['with-id', 'without-id'])\n"
            "def value(request): return request.param\n"
        )
    else:
        definition = (
            "@pytest.mark.parametrize('value', VALUES, ids=['with-id', 'without-id'])\n"
            + definition
        )
    marker = "@pytest.mark.polarion_id('OCS-4483')\n"
    name = "test_changed"
    if marker_scope == "function":
        definition = marker + definition
    elif marker_scope == "class":
        definition = (
            marker
            + "class TestCases:\n"
            + textwrap.indent(definition.replace("(value)", "(self, value)"), "    ")
        )
        name = "TestCases::test_changed"
    elif marker_scope == "module":
        source += "pytestmark.append(pytest.mark.polarion_id('OCS-4483'))\n"
    test_file.write_text(source + definition + "def test_legacy(): pass\n")
    items = testdir.getitems(test_file)
    monkeypatch.setattr(test_conftest.constants, "TOP_DIR", str(testdir.tmpdir))
    monkeypatch.setattr(
        test_conftest,
        "get_changed_test_nodeids",
        lambda items, base_branch: {f"tests/{filename}::{name}"},
    )
    if marker_scope:
        test_conftest.verify_test_decorators_requirements(items)
        return
    with pytest.raises(test_conftest.MissingDecoratorError) as error:
        test_conftest.verify_test_decorators_requirements(items)
    message = str(error.value)
    assert (
        "test_changed[without-id]" in message
        and "test_changed[with-id]" not in message
        and "test_legacy" not in message
    )


def test_changed_test_nodeids_handles_empty_collection():
    assert test_conftest.get_changed_test_nodeids([], "master") == set()


def test_changed_test_nodeids_rejects_empty_base_branch():
    with pytest.raises(ValueError, match="target branch"):
        test_conftest.get_changed_test_nodeids([], "")
