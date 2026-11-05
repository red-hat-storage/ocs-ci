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
            (
                "if True:\n"
                "    VALUES = [1]\n"
                "else:\n"
                "    VALUES = [2]\n"
                "@parametrize('value', VALUES)\n"
                "def test_case(value): pass\n"
                "def test_legacy(): pass\n"
            ),
            (
                "if True:\n"
                "    VALUES = [1, 3]\n"
                "else:\n"
                "    VALUES = [2]\n"
                "@parametrize('value', VALUES)\n"
                "def test_case(value): pass\n"
                "def test_legacy(): pass\n"
            ),
            {"test_case"},
            id="changed-conditional-parameters",
        ),
        pytest.param(
            (
                "VALUES = [1]\nif True:\n    def helper(): VALUES = [2]\n"
                "@parametrize('value', VALUES)\ndef test_legacy(value): pass\n"
            ),
            (
                "VALUES = [1]\nif True:\n    def helper(): VALUES = [3]\n"
                "@parametrize('value', VALUES)\ndef test_legacy(value): pass\n"
            ),
            set(),
            id="unrelated-conditional-helper",
        ),
        pytest.param(
            (
                "VALUES = [1]\n"
                "VALUES.append(pytest.param(2, marks=polarion_id('OCS-4483')))\n"
                "@parametrize('value', VALUES)\n"
                "def test_case(value): pass\n"
                "@pytest.mark.blue_squad\ndef test_legacy(): pass\n"
            ),
            (
                "VALUES = [1]\n"
                "VALUES.append(2)\n"
                "@parametrize('value', VALUES)\n"
                "def test_case(value): pass\n"
                "@pytest.mark.blue_squad\ndef test_legacy(): pass\n"
            ),
            {"test_case"},
            id="changed-mutated-parameters",
        ),
        pytest.param(
            (
                "VALUES = [1]\nif True:\n"
                "    VALUES.append(pytest.param(2, marks=polarion_id('OCS-4483')))\n"
                "@parametrize('value', VALUES)\n"
                "def test_case(value): pass\n"
                "@pytest.mark.blue_squad\ndef test_legacy(): pass\n"
            ),
            (
                "VALUES = [1]\nif True:\n"
                "    VALUES.append(2)\n"
                "@parametrize('value', VALUES)\n"
                "def test_case(value): pass\n"
                "@pytest.mark.blue_squad\ndef test_legacy(): pass\n"
            ),
            {"test_case"},
            id="changed-conditional-mutation",
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


@pytest.mark.parametrize(
    "changed_value,expected",
    [("[1]", "test_first"), ("[2]", "test_second"), ("[3]", "test_first")],
)
def test_changed_test_nodeids_uses_definition_bindings(
    git_test_repository, testdir, changed_value, expected
):
    """Use the parameter binding active when each decorator is evaluated."""
    repo, root = git_test_repository
    test_file = root / "tests" / "test_cases.py"
    test_file.parent.mkdir()
    source = (
        "import pytest\n"
        "VALUES = [1]\n"
        "@pytest.mark.parametrize('value', VALUES)\n"
        "def test_first(value): pass\n"
        "VALUES.extend([3])\n"
        "VALUES = [2]\n"
        "@pytest.mark.parametrize('value', VALUES)\n"
        "def test_second(value): pass\n"
    )
    test_file.write_text(source)
    repo.index.add(["tests/test_cases.py"])
    base_commit = repo.index.commit("Base tests")
    repo.git.update_ref("refs/remotes/origin/master", base_commit.hexsha)
    test_file.write_text(source.replace(changed_value, "[4]"))
    items = testdir.getitems(test_file)
    assert test_conftest.get_changed_test_nodeids(items, "master") == {
        f"tests/test_cases.py::{expected}"
    }


@pytest.mark.parametrize(
    "before_encoding,after_encoding",
    [
        ("utf-8-sig", "utf-8"),
        ("utf-8", "utf-8-sig"),
        ("latin-1", "utf-8"),
        ("utf-8", "latin-1"),
    ],
)
def test_changed_test_nodeids_handles_source_encodings(
    git_test_repository, testdir, before_encoding, after_encoding
):
    """Read Python source encodings from both Git and the working tree."""
    repo, root = git_test_repository
    test_file = root / "tests" / "test_cases.py"
    test_file.parent.mkdir()
    source = "# coding: {}\nLABEL = 'café'\ndef test_case(): {}\n"
    test_file.write_bytes(
        source.format(before_encoding, "pass").encode(before_encoding)
    )
    repo.index.add(["tests/test_cases.py"])
    base_commit = repo.index.commit("Base tests")
    repo.git.update_ref("refs/remotes/origin/master", base_commit.hexsha)
    test_file.write_bytes(
        source.format(after_encoding, "assert True").encode(after_encoding)
    )
    items = testdir.getitems(test_file)
    assert test_conftest.get_changed_test_nodeids(items, "master") == {
        "tests/test_cases.py::test_case"
    }


@pytest.mark.parametrize("filename", ["test_variants.py", "test_variants[1].py"])
@pytest.mark.parametrize("inherited_id", [False, True])
def test_polarion_validation_checks_each_variant(
    testdir, monkeypatch, inherited_id, filename
):
    testdir.makeini("[pytest]\nmarkers =\n    blue_squad\n    polarion_id\n")
    test_file = Path(str(testdir.tmpdir.mkdir("tests"))) / filename
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
    changed_nodeid = f"tests/{filename}::test_changed"
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


@pytest.mark.parametrize(
    "before,after,changed",
    [
        ("marks=pytest.mark.polarion_id('OCS-4483')", "marks=[]", True),
        ("return request.param", "return request.param + 1", False),
        ("def value(request):", "def value(request, other):", True),
    ],
)
def test_fixture_signatures_exclude_bodies(before, after, changed):
    """Compare fixture headers without comparing fixture bodies."""
    source = (
        "import pytest\n"
        "@pytest.fixture(params=[\n"
        "    pytest.param(1, marks=pytest.mark.polarion_id('OCS-4483')),\n"
        "])\n"
        "def value(request): return request.param\n"
        "def test_case(value): pass\n"
    )
    previous = test_conftest.get_test_signatures(source, {"value"})
    current = test_conftest.get_test_signatures(
        source.replace(before, after), {"value"}
    )
    assert (previous["value"] != current["value"]) is changed


@pytest.mark.parametrize("mode", ["direct", "transitive", "autouse", "conftest"])
@pytest.mark.parametrize(
    "before,after,affected_modes",
    [
        (
            "marks=pytest.mark.polarion_id('OCS-4483')",
            "marks=[]",
            {"direct", "transitive", "autouse", "conftest"},
        ),
        (
            "def value(request): return request.param",
            "def value(request): return 2",
            set(),
        ),
        ("marks=pytest.mark.polarion_id('OCS-9999')", "marks=[]", set()),
        (
            "def bridge(value): return value",
            "def bridge(): return 1",
            {"transitive", "autouse"},
        ),
    ],
)
def test_changed_test_nodeids_tracks_fixture_parameters(
    git_test_repository, testdir, mode, before, after, affected_modes
):
    """Select consumers of changed fixture metadata, including shared fixtures."""
    repo, root = git_test_repository
    test_file = root / "tests" / "test_cases.py"
    test_file.parent.mkdir()
    providers = (
        "import pytest\n"
        "@pytest.fixture(params=[\n"
        "    pytest.param(1, marks=pytest.mark.polarion_id('OCS-4483')),\n"
        "])\n"
        "def value(request): return request.param\n"
        "@pytest.fixture(params=[\n"
        "    pytest.param(3, marks=pytest.mark.polarion_id('OCS-9999')),\n"
        "])\n"
        "def unused(request): return request.param\n"
        f"@pytest.fixture(autouse={mode == 'autouse'})\n"
        "def bridge(value): return value\n"
    )
    argument = "bridge" if mode == "transitive" else "value"
    tests = (
        "import pytest\n"
        "@pytest.mark.blue_squad\n"
        f"def test_case({argument}): pass\n"
        "@pytest.mark.blue_squad\n"
        "def test_legacy(): pass\n"
    )
    provider_file = test_file
    if mode == "conftest":
        provider_file = test_file.parent / "conftest.py"
        provider_file.write_text(providers)
        test_file.write_text(tests)
    else:
        test_file.write_text(providers + tests)
    repo.index.add(["tests"])
    base_commit = repo.index.commit("Base tests")
    repo.git.update_ref("refs/remotes/origin/master", base_commit.hexsha)
    provider_file.write_text(provider_file.read_text().replace(before, after))
    items = testdir.getitems(test_file)
    expected = set()
    if mode in affected_modes:
        expected.add("tests/test_cases.py::test_case")
        if mode == "autouse":
            expected.add("tests/test_cases.py::test_legacy")
    assert test_conftest.get_changed_test_nodeids(items, "master") == expected


@pytest.mark.parametrize(
    "before,after,changed_names",
    [
        ("return 1", "return 2", set()),
        ("'OCS-4483'", "'OCS-4484'", {"Base"}),
        ("pass", "assert True", {"Base::test_case"}),
    ],
)
def test_test_signatures_separate_class_metadata(before, after, changed_names):
    """Keep inherited class metadata separate from helper and test bodies."""
    source = (
        "import pytest\n"
        "class Base:\n"
        "    pytestmark = pytest.mark.polarion_id('OCS-4483')\n"
        "    def helper(self): return 1\n"
        "    def test_case(self): pass\n"
        "class TestCases(Base):\n"
        "    def test_own(self): assert True\n"
    )
    previous = test_conftest.get_test_signatures(source, include_inherited=False)
    current = test_conftest.get_test_signatures(
        source.replace(before, after), include_inherited=False
    )
    assert {
        name for name in current if current[name] != previous[name]
    } == changed_names


@pytest.mark.parametrize(
    "before,after,expected",
    [
        ("import pytest", "import pytest\nimport math", set()),
        ("return 1", "return 2", set()),
        (
            "def test_inherited(self): pass",
            "def test_inherited(self): assert True",
            {"test_inherited"},
        ),
        ("'OCS-4483'", "'OCS-4484'", {"test_inherited", "test_own"}),
        (
            "import pytest",
            "import pytest\npytestmark = pytest.mark.polarion_id('OCS-4484')",
            {"test_inherited", "test_own"},
        ),
    ],
)
def test_changed_test_nodeids_tracks_inherited_definitions(
    git_test_repository, testdir, before, after, expected
):
    """Compare inherited tests and class metadata without comparing unused helpers."""
    repo, root = git_test_repository
    test_file = root / "tests" / "test_cases.py"
    test_file.parent.mkdir()
    source = (
        "import pytest\n"
        "class Base:\n"
        "    pytestmark = pytest.mark.polarion_id('OCS-4483')\n"
        "    def helper(self): return 1\n"
        "    def test_inherited(self): pass\n"
        "@pytest.mark.blue_squad\n"
        "class TestCases(Base):\n"
        "    def test_own(self): assert True\n"
    )
    test_file.write_text(source)
    repo.index.add(["tests"])
    baseline = repo.index.commit("Base tests")
    repo.git.update_ref("refs/remotes/origin/master", baseline.hexsha)
    test_file.write_text(source.replace(before, after))
    items = testdir.getitems(test_file)
    assert test_conftest.get_changed_test_nodeids(items, "master") == {
        f"tests/test_cases.py::TestCases::{name}" for name in expected
    }


@pytest.mark.parametrize(
    "mode",
    [
        "from",
        "module",
        "qualified",
        "relative",
        "relative_module",
        "reexport",
        "provider",
        "cyclic",
        "diamond",
    ],
)
@pytest.mark.parametrize("change", ["parameters", "unrelated"])
def test_changed_test_nodeids_tracks_imported_parameters(
    git_test_repository, testdir, mode, change
):
    """Follow imported parameter data without selecting tests for unused edits."""
    repo, root = git_test_repository
    directory = root / "tests" / "parameter_cases"
    directory.mkdir(parents=True)
    (directory / "__init__.py").write_text("")
    provider = directory / "case_data.py"
    source = (
        "import pytest\n"
        "VALUES = [pytest.param(1, marks=pytest.mark.polarion_id('OCS-4483'))]\n"
        "def unused(): return 1\n"
    )
    source += "def values(): return VALUES\n"
    if mode == "cyclic":
        source = (
            source.replace(
                "def values(): return VALUES",
                "def values(): return VALUES if True else get_forwarded()",
            )
            + "from .exports import get_forwarded\n"
        )
    provider.write_text(source)
    if mode == "from":
        imported = "from parameter_cases.case_data import VALUES\n"
        values = "VALUES"
    elif mode == "module":
        imported = "import parameter_cases.case_data as case_data\n"
        values = "case_data.VALUES"
    elif mode == "qualified":
        imported = "import parameter_cases.case_data\n"
        values = "parameter_cases.case_data.VALUES"
    elif mode == "provider":
        imported = "from parameter_cases.case_data import values\n"
        values = "values()"
    elif mode == "relative_module":
        imported = "from . import case_data\n"
        values = "case_data.VALUES"
    elif mode == "relative":
        imported = "from .case_data import VALUES\n"
        values = "VALUES"
    elif mode == "cyclic":
        (directory / "exports.py").write_text(
            "from .case_data import values\ndef get_forwarded(): return values()\n"
        )
        imported = "from parameter_cases.case_data import values\n"
        values = "values()"
    elif mode == "diamond":
        (directory / "left.py").write_text("from .case_data import VALUES\n")
        (directory / "right.py").write_text("from .case_data import VALUES\n")
        (directory / "exports.py").write_text(
            "from .left import VALUES as LEFT\n"
            "from .right import VALUES as RIGHT\n"
            "def values(): return LEFT if True else RIGHT\n"
        )
        imported = "from parameter_cases.exports import values\n"
        values = "values()"
    else:
        (directory / "exports.py").write_text("from .case_data import VALUES\n")
        imported = "from parameter_cases.exports import VALUES\n"
        values = "VALUES"
    test_file = directory / "test_cases.py"
    test_file.write_text(
        "import pytest\n"
        + imported
        + "@pytest.mark.blue_squad\n"
        + f"@pytest.mark.parametrize('value', {values})\n"
        + "def test_case(value): pass\n"
        + "@pytest.mark.blue_squad\ndef test_legacy(): pass\n"
    )
    repo.index.add(["tests"])
    baseline = repo.index.commit("Base tests")
    repo.git.update_ref("refs/remotes/origin/master", baseline.hexsha)
    if change == "parameters":
        provider.write_text(source.replace("VALUES = [", "VALUES = [2, "))
    else:
        provider.write_text(source.replace("return 1", "return 2"))
    items = testdir.getitems(test_file)
    expected = (
        {"tests/parameter_cases/test_cases.py::test_case"}
        if change == "parameters"
        else set()
    )
    assert test_conftest.get_changed_test_nodeids(items, "master") == expected


@pytest.mark.parametrize("kind", ["test", "fixture"])
@pytest.mark.parametrize("change", ["unrelated", "definition"])
def test_changed_test_nodeids_unwraps_decorated_functions(
    git_test_repository, testdir, kind, change
):
    """Locate decorated tests and fixtures in their original source modules."""
    repo, root = git_test_repository
    directory = root / "tests" / "wrapped_cases"
    directory.mkdir(parents=True)
    (directory / "__init__.py").write_text("")
    (directory / "wrappers.py").write_text(
        "from functools import wraps\n"
        "def wrapped(function):\n"
        "    @wraps(function)\n"
        "    def wrapper(*args, **kwargs): return function(*args, **kwargs)\n"
        "    return wrapper\n"
    )
    source = "import pytest\nfrom wrapped_cases.wrappers import wrapped\n"
    if kind == "fixture":
        source += (
            "@pytest.fixture(params=[pytest.param(1, marks=pytest.mark.polarion_id('OCS-4483'))])\n"
            "@wrapped\ndef value(request): return request.param\n"
            "@pytest.mark.blue_squad\ndef test_case(value): pass\n"
        )
        before, after = "marks=pytest.mark.polarion_id('OCS-4483')", "marks=[]"
    else:
        source += "@pytest.mark.blue_squad\n@wrapped\ndef test_case(): pass\n"
        before, after = "pass", "assert True"
    test_file = directory / "test_cases.py"
    test_file.write_text(source)
    repo.index.add(["tests"])
    baseline = repo.index.commit("Base tests")
    repo.git.update_ref("refs/remotes/origin/master", baseline.hexsha)
    test_file.write_text(
        source + "UNRELATED = 1\n"
        if change == "unrelated"
        else source.replace(before, after)
    )
    items = testdir.getitems(test_file)
    expected = (
        {"tests/wrapped_cases/test_cases.py::test_case"}
        if change == "definition"
        else set()
    )
    assert test_conftest.get_changed_test_nodeids(items, "master") == expected
