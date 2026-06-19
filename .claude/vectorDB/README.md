# OCS-CI Code Vector Database

Shared semantic search over ocs-ci code and test metadata using Qdrant. Lives at `.claude/vectorDB/` — not tied to any single agent.

Run all commands from the **ocs-ci repository root**.

## Indexed directories

Only these top-level directories (and their subdirs) are indexed:

| Directory | Notes |
|-----------|-------|
| `conf/` | Configuration files |
| `Docker_files/` | Docker assets |
| `docs/` | Documentation |
| `examples/` | Example scripts |
| `external/` | External integrations |
| `ocs_ci/` | Main library (`ocs-ci/` in config maps here) |
| `scripts/` | Utility scripts |
| `src/` | Additional source packages |
| `template_test/` | Test templates |
| `terraform/` | Terraform configs |
| `tests/` | Pytest tests |

Nothing outside this list is indexed (no `.claude/`, repo root files, etc.).

## Pipeline

```text
OCS-CI repo dirs (above)
        |
        v
   Code Parser          ← code_parser.py
        |
        v
  Chunks + Metadata    ← test functions (test_*.py) or whole files
        |
        v
  Qdrant Vector DB
        |
        v
  Retrieval API        ← retrieval.py / vector_db_cli.py search
```

## Prerequisites

Install deps **with the same `python` you use to run the CLI**:

```bash
python .claude/vectorDB/vector_db_cli.py install-deps
```

`install-deps` bootstraps `pip` via `ensurepip` if your venv is missing it.

Or manually:

```bash
python -m ensurepip --upgrade   # only if pip is missing
python -m pip install -r .claude/vectorDB/requirements.txt
```

**Note:** Installing into the ocs-ci `.venv` may upgrade packages (e.g. `numpy`) that conflict with `ocs-ci` pins. If that matters for pytest runs, use a separate venv for the vector DB only.

## Quick start

### Create the database (full index)

```bash
python .claude/vectorDB/vector_db_cli.py create
```

### Update incrementally

```bash
python .claude/vectorDB/vector_db_cli.py update
```

### Index a single directory only

```bash
python .claude/vectorDB/vector_db_cli.py create --index-dir tests
python .claude/vectorDB/vector_db_cli.py create --index-dir ocs_ci
```

### Search tests (default)

```bash
python .claude/vectorDB/vector_db_cli.py search "noobaa bucket replication"
```

### Search all indexed content

```bash
python .claude/vectorDB/vector_db_cli.py search "storagecluster upgrade" --all-content
```

### Other commands

```bash
python .claude/vectorDB/vector_db_cli.py status
python .claude/vectorDB/vector_db_cli.py cleanup --all
```

## Chunk types

- **`test`** — one chunk per `test_*` function (docstring, jira, coverage areas, pytest command)
- **`file`** — one chunk per other indexable file (`.py`, `.md`, `.yaml`, `.tf`, `.sh`, …)

## CLI reference

| Command | Description |
|---------|-------------|
| `create` | Full rebuild of configured directories |
| `update` | Incremental update by file content hash |
| `status` | Collection stats and manifest summary |
| `search QUERY` | Similarity search (tests only by default) |
| `search-issue` | Search using a z-stream run-record issue |
| `cleanup` | Delete collection; `--all` wipes `data/` |

| Flag | Description |
|------|-------------|
| `--index-dir DIR` | Limit to one allowed top-level directory |
| `--all-content` | Search tests + files (not just tests) |
| `--collection NAME` | Qdrant collection (default: `ocs_ci_code`) |
| `--max-files N` | Cap files scanned (debug) |

## Module layout

| File | Purpose |
|------|---------|
| `vector_db_cli.py` | CLI entry point |
| `config.py` | `INDEX_DIR_NAMES`, paths, defaults |
| `code_parser.py` | Walk allowed dirs, extract chunks |
| `index_manager.py` | create / update / cleanup / status |
| `embedder.py` | Sentence-transformers embeddings |
| `qdrant_store.py` | Qdrant CRUD and search |
| `retrieval.py` | `find_similar_tests()` API |

## Storage

```text
.claude/vectorDB/data/
  qdrant/
  manifest.json
```

Local data is gitignored.
