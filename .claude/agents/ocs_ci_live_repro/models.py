"""Constants and enums for live cluster verification."""

STAGE_LIVE_CLUSTER_VERIFICATION = "live_cluster_verification"

VERIFIER_DRY_RUN = "dry_run"
VERIFIER_LIVE = "claude_agent_sdk"
VERIFIER_LIVE_CLI = "claude_code_cli"

VERDICT_DRY_RUN = "dry_run"
VERDICT_SKIPPED = "skipped"
VERDICT_INCONCLUSIVE = "inconclusive"
VERDICT_FIXED = "fixed"
VERDICT_NOT_FIXED = "not_fixed"

SKIP_ENV_MISMATCH = "env_mismatch"
SKIP_MISSING_REPRO = "missing_repro_steps"
SKIP_NO_CLUSTER = "no_cluster"
