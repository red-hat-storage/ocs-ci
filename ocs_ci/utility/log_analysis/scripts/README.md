# Cache Backfill Script

Backfills old cache files with fields added after the initial analysis: `bug_details`, `suggested_fix`, `run_metadata`, `traceback`, `status`, and `polarion_id`.

## Why

Cache files created before these features were added contain only the core analysis (`category`, `root_cause_summary`, `evidence`, `recommended_action`). The backfill script enriches them without re-running the full agentic analysis pipeline.

## What Gets Backfilled

| Field | Source | AI Cost |
|-------|--------|---------|
| `run_metadata` | Scanner `.log` files, report HTMLs | Free |
| `traceback` | Scanner `.log` files, report HTMLs | Free |
| `status`, `polarion_id` | Scanner `.log` files, report HTMLs | Free |
| `test_class` | Scanner `.log` files | Free |
| Broken `unknown` recovery | Recorded session files (re-parse with brace-depth JSON extractor) | Free |
| `bug_details` | Lightweight single-turn AI call using existing analysis | ~$0.02/call |
| `suggested_fix` | Lightweight single-turn AI call with source code from release branch | ~$0.03/call |

## Usage

```bash
# Dry run — scan and report candidates
python -m ocs_ci.utility.log_analysis.scripts.backfill_cache \
  --cache-dir ~/.ocs-ci/analysis_cache \
  --dry-run

# Metadata only (free, instant, no risk)
python -m ocs_ci.utility.log_analysis.scripts.backfill_cache \
  --cache-dir ~/.ocs-ci/analysis_cache \
  --mode metadata-only

# Re-parse broken sessions (free, recovers misclassified unknown entries)
python -m ocs_ci.utility.log_analysis.scripts.backfill_cache \
  --cache-dir ~/.ocs-ci/analysis_cache \
  --sessions-dir /mnt/ocsci-jenkins/log_analysis/sessions_dir \
  --mode reparse-only

# Generate bug_details for product_bug entries (AI, small batch first)
python -m ocs_ci.utility.log_analysis.scripts.backfill_cache \
  --cache-dir ~/.ocs-ci/analysis_cache \
  --mode bug-details-only \
  --limit 5

# Generate suggested_fix for test_bug entries (AI, uses release branch source)
python -m ocs_ci.utility.log_analysis.scripts.backfill_cache \
  --cache-dir ~/.ocs-ci/analysis_cache \
  --upstream-repo ~/.ocs-ci/upstream-repo/ocs-ci \
  --mode fix-only \
  --limit 5

# Full backfill
python -m ocs_ci.utility.log_analysis.scripts.backfill_cache \
  --cache-dir ~/.ocs-ci/analysis_cache \
  --mode all \
  --delay 2.0
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--cache-dir` | `~/.ocs-ci/analysis_cache` | Cache directory to process |
| `--upstream-repo` | `~/.ocs-ci/upstream-repo/ocs-ci` | Bare git repo for reading test source |
| `--sessions-dir` | `/mnt/ocsci-jenkins/log_analysis/sessions_dir` | Recorded session transcripts |
| `--model` | `sonnet` | AI model for generation calls |
| `--dry-run` | off | Scan only, don't modify files |
| `--limit N` | unlimited | Max entries to process per category |
| `--delay` | `1.0` | Seconds between AI calls (rate limiting) |
| `--mode` | `all` | What to backfill (see below) |
| `-v` | off | Debug logging |

### Modes

- **`metadata-only`** — Fill `run_metadata`, `traceback`, `status`, `polarion_id`, `test_class` from scanner logs. No AI, instant.
- **`reparse-only`** — Re-extract JSON from recorded session files for broken `unknown` entries using brace-depth counting. No AI.
- **`bug-details-only`** — Generate `bug_details` for `product_bug` entries. Single-turn AI call per entry.
- **`fix-only`** — Generate `suggested_fix` for `test_bug` entries. Reads source from correct release branch, single-turn AI call.
- **`all`** — Run all of the above in order.

## Recommended Execution Order

1. `--dry-run` — see candidate counts
2. `--mode metadata-only` — free, no risk
3. `--mode reparse-only` — free, recovers broken parses
4. `--mode bug-details-only --limit 5` — verify quality on small batch
5. `--mode fix-only --limit 5` — verify quality on small batch
6. Full run with `--mode all`

## How It Works

### Metadata Backfill
Searches scanner subprocess `.log` files for lines like `Cached analysis for <hash>` to find which run produced each cache file. Extracts `run_metadata`, `traceback`, and `status` from the same log or associated report HTML.

### Session Re-parsing
Old cache entries classified as `unknown` with empty `root_cause_summary` may be victims of the regex-based JSON extractor that broke on nested code fences inside `bug_details.description`. The script finds the corresponding recorded session file, re-extracts the JSON using brace-depth counting (same fix applied to `ClaudeCodeBackend._extract_json`), and updates the cache with the correct classification.

### Bug Details Generation
For `product_bug` entries, sends a single-turn prompt with:
- Existing `root_cause_summary` and `evidence`
- `traceback` (if available)
- `run_metadata` (platform, OCP/ODF versions)

Returns a structured DFBUGS form with `bug_subject`, `description`, `steps_to_reproduce`, etc.

### Suggested Fix Generation
For `test_bug` entries:
1. Derives the release branch from `run_metadata.ocs_version` (e.g., `4.21.1-2.konflux` → `release-4.21`)
2. Derives the file path from `test_class` (e.g., `tests.functional.pv.test_clone.TestClone` → `tests/functional/pv/test_clone.py`)
3. Reads the source with `git show release-4.21:<path>` from the upstream bare repo
4. Sends a single-turn prompt with the analysis + actual source code

Returns a `suggested_fix` with `file`, `function`, `line`, `description`, `code_snippet`, and `diff`.

## Safety

- **Idempotent** — skips entries that already have the target field
- **Non-destructive** — only adds new fields, never removes existing data
- **Rate-limited** — configurable delay between AI calls (default 1s)
- **Dry-run first** — always preview before modifying

## Cost Estimate

For a typical cache with ~200 product_bug and ~150 test_bug entries:
- Metadata + session re-parse: **$0**
- bug_details: ~$0.02/call × ~200 = **~$4**
- suggested_fix: ~$0.03/call × ~150 = **~$4.50**
- **Total: ~$8–10**
