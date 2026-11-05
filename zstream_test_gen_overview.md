# Z-Stream Test Generator

**AI-Driven Verification Test Generation for ODF Bug Fixes**

## Problem Statement

Every ODF z-stream release ships 15-25 bug fixes, each requiring a hand-written
pytest verification test that follows strict ocs-ci conventions. The same fix is
backported across 3-5 ODF versions, multiplying the tracking burden. Today, many
fixes are verified only manually and produce no lasting automation artifact,
meaning regressions can silently reappear in future releases.

Writing these tests is time-consuming but largely mechanical: read the bug, study
the upstream fix, write a test with the right base class, markers, fixtures, and
cleanup pattern. This overhead competes with higher-value engineering work.

## Solution

An end-to-end pipeline that reads Jira bugs, understands the upstream fix, and
generates production-ready ocs-ci pytest tests — complete with helper functions,
draft PRs, and backport branches.

```
python -m ocs_ci.utility.zstream_test_gen --fix-version odf-4.22.6
```

### Pipeline Stages

| Stage | Name | Description |
|-------|------|-------------|
| 0a | Fix reviews | Address CodeRabbit comments on open PRs automatically |
| 0b | Collect feedback | Learn from reviewer corrections on merged PRs |
| 1 | Collect | Query Jira for all bugs in the fix version |
| 2 | Enrich | Fetch upstream PR diffs + AI root cause analysis |
| 3 | Classify | Automatable / DR / Manual-only / Already covered |
| 3.5 | Deduplicate | Group backported clones, keep richest context |
| 3.7 | Skip open PRs | Skip bugs that already have open PRs from previous runs |
| 3.8 | Score | Confidence scoring (7 signals, 0-100%) |
| 4 | Generate | AI test generation with similar-test examples |
| 5 | Extract helpers | Second AI pass: refactor inline logic into helpers |
| 6 | Validate | Syntax, imports, patterns, flake8, secrets scan, pytest collect |
| 7 | Publish | Draft PRs + backports + Jira labels |

### Automated Feedback Loop

The tool learns from every interaction:

- **CodeRabbit review fixes** — On each run, the tool checks open PRs for
  CodeRabbit review comments. It reads the comments + current code, generates
  fixes via Claude, and pushes a fix commit directly to the PR branch.

- **Merged PR learning** — For PRs that were merged after reviewer corrections,
  the tool compares the original generated code vs the final merged code,
  extracts reusable correction rules, and stores them in
  `~/.zstream_test_gen_corrections.yaml`. These rules are automatically injected
  into the system prompt for all future generations.

- **No human intervention required** — The feedback loop runs automatically at
  the start of every pipeline run. It can also be triggered standalone via
  `--maintain`, `--fix-reviews`, or `--learn-only`.

### Secrets Protection

Two layers of scanning prevent sensitive data from reaching GitHub:

1. **Validation stage** — Scans generated test code for hardcoded credentials,
   private keys, tokens, pull secrets, and AWS keys before any fix-up attempts.

2. **Publisher gate** — Final scan of all PR content (test files, helper files,
   PR body, commit message) right before pushing to GitHub. If anything matches,
   the PR is blocked.

### Open PR Detection

When run against a release for the second time, the tool checks GitHub for
existing open PRs before regenerating tests. Bugs with open PRs are skipped
entirely, saving Claude API costs and avoiding duplicate work.

## CLI Reference

| Command | Description |
|---------|-------------|
| `--fix-version odf-4.22.6` | Full pipeline: maintain + generate + publish |
| `--bug DFBUGS-10065` | Process a single bug |
| `--maintain` | Fix reviews + collect feedback only (no generation) |
| `--fix-reviews` | Fix CodeRabbit comments on open PRs only |
| `--learn-only` | Collect feedback from merged/closed PRs only |
| `--init-config` | Create sample config file |
| `--dry-run` | Generate tests but don't create PRs |
| `--no-pr` | Generate and save locally, no PRs |
| `--no-backport` | Skip backport PRs to release branches |
| `--confidence high` | Only publish PRs at or above this confidence level |
| `-v` | Verbose/debug logging |

## Benefits

- **Reduce QE overhead** — Engineers review and refine instead of writing
  boilerplate verification tests from scratch for every z-stream fix.

- **Increase automation coverage** — Deduplication across backports + automatic
  release branch PRs ensure every version gets coverage without extra effort.

- **Permanent regression tests** — Every verified fix becomes part of the
  regression suite, catching silent regressions automatically.

- **Self-improving** — The tool learns from CodeRabbit reviews and human
  reviewer corrections, reducing the same mistakes in future generations.

- **Retroactive coverage** — Process past fix versions to generate tests for
  bugs that were only verified manually.

- **Consistent conventions** — Generated tests follow ocs-ci patterns by design:
  base classes, markers, fixtures, logging, cleanup.

- **Faster qualification** — Verification tests are ready for review as soon as
  fixes are merged, compressing release timelines.

- **Safe by default** — Two-layer secrets scanning prevents sensitive data from
  being pushed to GitHub.

## Architecture

```
ocs_ci/utility/zstream_test_gen/
├── cli.py            CLI entry point, argument parsing
├── config.py         YAML + env var configuration loading
├── pipeline.py       Orchestrates all stages end-to-end
├── jira_client.py    Jira queries, parsing, clone chain walking
├── github_client.py  PR diffs, fork-based PR creation, CodeRabbit comment fetching
├── generator.py      Claude AI calls (analysis, generation, helper extraction)
├── classifier.py     Bug classification + confidence scoring
├── validator.py      Syntax, import, pattern, flake8, secrets validation
├── publisher.py      PR publishing, backports, Jira labeling, secrets gate
├── feedback.py       Automated feedback loop (review fixing + learning)
├── prompts.py        All AI prompt templates
└── models.py         Data models (BugInfo, GeneratedTest, HelperSpec, etc.)
```

## Configuration

```bash
python -m ocs_ci.utility.zstream_test_gen --init-config
# Edit ~/.zstream_test_gen.yaml with your credentials
```

Required credentials:
- **Jira**: API token for redhat.atlassian.net
- **GitHub**: Fine-grained PAT for your fork + `gh` CLI auth for upstream PRs
- **Claude**: Vertex AI project or Anthropic API key

## Getting Started

1. **Configure** — Run `--init-config`, add Jira/GitHub/Claude credentials
2. **Fork** — Fork `red-hat-storage/ocs-ci`, set `fork_repo` in config
3. **Generate** — Run with `--fix-version` and `--confidence` level
4. **Review** — Check draft PRs, merge helpers into target modules
5. **Maintain** — Run `--maintain` periodically to fix reviews and learn
