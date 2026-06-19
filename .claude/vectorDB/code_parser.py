"""
Parse ocs-ci repository files for vector indexing.

Indexes only directories listed in config.INDEX_DIR_NAMES:
  - test_*.py → per-test-function chunks (via z-stream test_matcher)
  - other supported files → per-file chunks
"""

import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator

from config import (
    AGENTS_DIR,
    INDEX_DIR_ALIASES,
    INDEX_DIR_NAMES,
    INDEX_EXTENSIONS,
    INDEX_FILENAMES,
    MAX_EMBED_CHARS,
    MAX_FILE_BYTES,
    REPO_ROOT,
    SKIP_DIR_NAMES,
    resolve_index_dirs,
)

log = logging.getLogger(__name__)

_ZSTREAM_DIR = AGENTS_DIR / "zstream"
if str(_ZSTREAM_DIR) not in sys.path:
    sys.path.insert(0, str(_ZSTREAM_DIR))

from test_matcher import TestCandidate, _parse_test_file  # noqa: E402


@dataclass
class IndexChunk:
    """A single embeddable unit: test function or source file."""

    chunk_id: str
    file_path: str
    chunk_type: str
    name: str
    source_dir: str
    docstring: str = ""
    content_excerpt: str = ""
    jira_ids: list[str] = field(default_factory=list)
    polarion_ids: list[str] = field(default_factory=list)
    coverage_areas: list[str] = field(default_factory=list)
    class_name: str | None = None
    pytest_command: str | None = None

    @property
    def node_id(self) -> str:
        return self.chunk_id


def _source_dir_name(file_path: str) -> str:
    return file_path.split("/", 1)[0]


def _should_index_file(path: Path) -> bool:
    if path.name.startswith("."):
        return False
    if any(part in SKIP_DIR_NAMES or part.endswith(".egg-info") for part in path.parts):
        return False
    if path.suffix.lower() in INDEX_EXTENSIONS:
        return True
    return path.name.lower() in INDEX_FILENAMES


def iter_index_files(
    index_roots: list[Path] | None = None,
    *,
    limit_root: Path | None = None,
    max_files: int | None = None,
) -> Iterator[Path]:
    """Walk configured index directories and yield indexable files."""
    roots = (
        [limit_root.resolve()] if limit_root else (index_roots or resolve_index_dirs())
    )
    count = 0

    for root in roots:
        if not root.is_dir():
            log.warning("Index directory not found: %s", root)
            continue

        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [
                d
                for d in dirnames
                if d not in SKIP_DIR_NAMES
                and not d.endswith(".egg-info")
                and not d.startswith(".")
            ]
            for filename in filenames:
                path = Path(dirpath) / filename
                if not _should_index_file(path):
                    continue
                try:
                    if path.stat().st_size > MAX_FILE_BYTES:
                        log.debug("Skipping large file: %s", path)
                        continue
                except OSError:
                    continue
                yield path
                count += 1
                if max_files and count >= max_files:
                    return


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _py_outline(content: str) -> str:
    """Extract module docstring, class names, and def names from Python source."""
    parts: list[str] = []
    mod_doc = re.match(r'^(\s*"""[\s\S]*?"""|\s*\'\'\'[\s\S]*?\'\'\')', content)
    if mod_doc:
        parts.append(mod_doc.group(1).strip("\"' \n"))
    classes = re.findall(r"^class\s+(\w+)", content, re.MULTILINE)
    functions = re.findall(r"^def\s+(\w+)", content, re.MULTILINE)
    if classes:
        parts.append("classes: " + " ".join(classes[:40]))
    if functions:
        parts.append("functions: " + " ".join(functions[:60]))
    return " ".join(parts)


def _file_excerpt(path: Path, content: str) -> str:
    if path.suffix.lower() == ".py":
        outline = _py_outline(content)
        body = content[: MAX_EMBED_CHARS - len(outline) - 1]
        return f"{outline}\n{body}".strip()
    return content[:MAX_EMBED_CHARS]


def _test_candidate_to_chunk(candidate: TestCandidate) -> IndexChunk:
    return IndexChunk(
        chunk_id=candidate.node_id,
        file_path=candidate.file_path,
        chunk_type="test",
        name=candidate.test_name,
        source_dir=_source_dir_name(candidate.file_path),
        docstring=candidate.docstring,
        content_excerpt=candidate.search_text,
        jira_ids=candidate.jira_ids,
        polarion_ids=candidate.polarion_ids,
        coverage_areas=candidate.coverage_areas,
        class_name=candidate.class_name,
        pytest_command=f"pytest {candidate.node_id}",
    )


def _file_to_chunk(path: Path) -> IndexChunk | None:
    rel_path = str(path.relative_to(REPO_ROOT))
    try:
        content = _read_text(path)
    except OSError as exc:
        log.debug("Skipping unreadable file %s: %s", path, exc)
        return None

    excerpt = _file_excerpt(path, content)
    return IndexChunk(
        chunk_id=rel_path,
        file_path=rel_path,
        chunk_type="file",
        name=path.name,
        source_dir=_source_dir_name(rel_path),
        docstring=excerpt[:500],
        content_excerpt=excerpt,
    )


def parse_index_chunks(
    *,
    limit_root: Path | None = None,
    max_files: int | None = None,
) -> list[IndexChunk]:
    """
    Parse all indexable content under configured INDEX_DIR_NAMES.

    Args:
        limit_root (Path | None): Restrict to one allowed top-level directory
        max_files (int | None): Cap files scanned (debug)

    Returns:
        list[IndexChunk]: Chunks ready for embedding

    """
    if limit_root:
        resolved = limit_root.resolve()
        rel = resolved.relative_to(REPO_ROOT)
        allowed_tops = {INDEX_DIR_ALIASES.get(n, n) for n in INDEX_DIR_NAMES}
        if rel.parts[0] not in allowed_tops:
            raise ValueError(
                f"--index-dir must be one of: {', '.join(INDEX_DIR_NAMES)}"
            )

    chunks: list[IndexChunk] = []
    files_seen = 0

    for path in iter_index_files(limit_root=limit_root, max_files=max_files):
        files_seen += 1
        rel = str(path.relative_to(REPO_ROOT))

        if path.name.startswith("test_") and path.suffix == ".py":
            for candidate in _parse_test_file(path):
                chunks.append(_test_candidate_to_chunk(candidate))
            continue

        chunk = _file_to_chunk(path)
        if chunk:
            chunks.append(chunk)

    log.info("Parsed %d chunks from %d files", len(chunks), files_seen)
    return chunks


def embedding_text(chunk: IndexChunk) -> str:
    """Build rich text for embedding."""
    parts = [
        chunk.file_path,
        chunk.chunk_type,
        chunk.name,
        chunk.class_name or "",
        chunk.source_dir,
        chunk.docstring,
        " ".join(chunk.jira_ids),
        " ".join(chunk.polarion_ids),
        " ".join(chunk.coverage_areas),
        chunk.content_excerpt,
    ]
    return " ".join(filter(None, parts)).strip()[:MAX_EMBED_CHARS]


def chunk_to_payload(chunk: IndexChunk) -> dict[str, Any]:
    """Serialize an IndexChunk to a Qdrant payload dict."""
    return {
        "node_id": chunk.chunk_id,
        "file_path": chunk.file_path,
        "chunk_type": chunk.chunk_type,
        "name": chunk.name,
        "source_dir": chunk.source_dir,
        "class_name": chunk.class_name,
        "docstring": chunk.docstring[:2000],
        "jira_ids": chunk.jira_ids,
        "polarion_ids": chunk.polarion_ids,
        "coverage_areas": chunk.coverage_areas,
        "pytest_command": chunk.pytest_command,
        "embedding_text": embedding_text(chunk)[:MAX_EMBED_CHARS],
    }


def stable_point_id(chunk_id: str) -> str:
    """Deterministic UUID from chunk id."""
    digest = hashlib.sha256(chunk_id.encode("utf-8")).hexdigest()
    return (
        f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"
    )


def file_content_hash(path: Path) -> str:
    """SHA-256 of file bytes for change detection."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def group_chunks_by_file(chunks: list[IndexChunk]) -> dict[str, list[IndexChunk]]:
    """Group chunks by source file path."""
    grouped: dict[str, list[IndexChunk]] = {}
    for chunk in chunks:
        grouped.setdefault(chunk.file_path, []).append(chunk)
    return grouped


def load_manifest(path: Path) -> dict[str, Any]:
    """Load the index manifest (file hashes and indexed chunk ids)."""
    if not path.is_file():
        return {"files": {}, "collection": None, "embedding_model": None}
    return json.loads(path.read_text(encoding="utf-8"))


def save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    """Persist the index manifest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def chunk_dict(chunk: IndexChunk) -> dict[str, Any]:
    """Convert IndexChunk to a plain dict."""
    return asdict(chunk)


# Backward-compatible aliases
candidate_to_payload = chunk_to_payload
group_candidates_by_file = group_chunks_by_file
parse_tests = parse_index_chunks
