"""Configuration for the OCS-CI run agent."""

from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
AGENTS_DIR = MODULE_DIR.parent
REPO_ROOT = AGENTS_DIR.parent.parent

# Parameters safe to override when retriggering test runs
TEST_RUN_OVERRIDE_KEYS: frozenset[str] = frozenset(
    {
        "TEST_PATH",
        "TEST_MARK_EXPRESSION",
        "TEST_NAME_EXPRESSION",
        "RUN_INSTALL_OCP",
        "RUN_INSTALL_OCS",
        "RUN_TEST",
        "RUN_TEARDOWN",
        "ADDITIONAL_PYTEST_PARAMS",
    }
)

TEST_RUN_DEFAULT_OVERRIDES: dict[str, str | bool] = {
    "RUN_INSTALL_OCP": False,
    "RUN_INSTALL_OCS": False,
    "RUN_TEST": True,
    "RUN_TEARDOWN": False,
    "TEST_MARK_EXPRESSION": "",
}

WAIT_POLL_SEC_DEFAULT = 60
WAIT_TIMEOUT_SEC_DEFAULT = 14400

AUTH_YAML_CANDIDATES: tuple[Path, ...] = (
    REPO_ROOT / "data" / "auth.yaml",
    REPO_ROOT / "data" / "auth.yml",
)
