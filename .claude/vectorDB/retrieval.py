"""LLM retrieval API — find similar ocs-ci code and tests via vector search."""

import logging
import sys
from pathlib import Path
from typing import Any

from code_parser import (
    chunk_to_payload,
    embedding_text,
    group_chunks_by_file,
    load_manifest,
    parse_index_chunks,
    save_manifest,
    stable_point_id,
)
from config import (
    AGENTS_DIR,
    DEFAULT_BATCH_SIZE,
    DEFAULT_COLLECTION,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_TOP_K,
    MANIFEST_PATH,
    QDRANT_PATH,
    REPO_ROOT,
    resolve_index_dirs,
)
from embedder import embed_texts
from qdrant_store import create_collection, get_client, search_similar, upsert_points

log = logging.getLogger(__name__)

_TEST_MATCH_DIR = AGENTS_DIR / "ocs_ci_test_match"
if str(_TEST_MATCH_DIR) not in sys.path:
    sys.path.insert(0, str(_TEST_MATCH_DIR))


def build_query_text(
    query: str,
    *,
    components: list[str] | None = None,
    reproduction_steps: list[str] | None = None,
    verification_steps: list[str] | None = None,
) -> str:
    """Combine query parts into embedding text."""
    parts = [
        query,
        " ".join(components or []),
        " ".join(reproduction_steps or []),
        " ".join(verification_steps or []),
    ]
    return " ".join(filter(None, parts)).strip()


def find_similar_tests(
    query: str,
    *,
    top_k: int = DEFAULT_TOP_K,
    collection: str = DEFAULT_COLLECTION,
    embedding_model: str | None = None,
    qdrant_url: str | None = None,
    coverage_areas: list[str] | None = None,
    score_threshold: float | None = None,
    chunk_type: str | None = "test",
    components: list[str] | None = None,
    reproduction_steps: list[str] | None = None,
    verification_steps: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Find ocs-ci content semantically similar to a natural-language query.

    By default filters to chunk_type=test; pass chunk_type=None to search all content.
    """
    manifest = load_manifest(MANIFEST_PATH)
    model = (
        embedding_model or manifest.get("embedding_model") or DEFAULT_EMBEDDING_MODEL
    )

    query_text = build_query_text(
        query,
        components=components,
        reproduction_steps=reproduction_steps,
        verification_steps=verification_steps,
    )
    if not query_text:
        raise ValueError("Query text is empty")

    client = get_client(qdrant_url=qdrant_url)
    query_vector = embed_texts([query_text], model_name=model, show_progress=False)[0]

    return search_similar(
        client,
        collection,
        query_vector,
        top_k=top_k,
        score_threshold=score_threshold,
        coverage_areas=coverage_areas,
        chunk_type=chunk_type,
    )


def find_similar_tests_for_issue(
    issue: dict[str, Any],
    *,
    top_k: int = DEFAULT_TOP_K,
    collection: str = DEFAULT_COLLECTION,
    embedding_model: str | None = None,
    qdrant_url: str | None = None,
) -> list[dict[str, Any]]:
    """Find tests similar to a z-stream run-record issue dict."""
    from coverage_mapper import infer_issue_coverage_areas  # noqa: E402

    repro = issue.get("stages", {}).get("repro_steps", {}).get("data", {})
    issue_coverage = infer_issue_coverage_areas(issue)

    query_parts = [
        issue.get("key", ""),
        issue.get("summary", ""),
        issue.get("description", ""),
        repro.get("issue_summary", ""),
        repro.get("expected_result", ""),
    ]
    query = " ".join(filter(None, query_parts))

    return find_similar_tests(
        query,
        top_k=top_k,
        collection=collection,
        embedding_model=embedding_model,
        qdrant_url=qdrant_url,
        coverage_areas=issue_coverage.get("coverage_areas"),
        components=issue.get("components"),
        reproduction_steps=repro.get("reproduction_steps"),
        verification_steps=repro.get("verification_steps"),
        chunk_type="test",
    )


def index_all_chunks(
    *,
    index_dir: Path | None = None,
    collection: str = DEFAULT_COLLECTION,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
    batch_size: int = DEFAULT_BATCH_SIZE,
    qdrant_url: str | None = None,
    recreate: bool = False,
    max_files: int | None = None,
) -> dict[str, Any]:
    """Full index build: parse configured dirs, embed, upsert into Qdrant."""
    from code_parser import file_content_hash

    chunks = parse_index_chunks(limit_root=index_dir, max_files=max_files)
    if not chunks:
        return {"indexed": 0, "files": 0, "message": "No indexable content found"}

    client = get_client(qdrant_url=qdrant_url)
    vector_size = len(
        embed_texts(["probe"], model_name=embedding_model, show_progress=False)[0]
    )
    create_collection(client, collection, vector_size=vector_size, recreate=recreate)

    texts = [embedding_text(c) for c in chunks]
    vectors = embed_texts(texts, model_name=embedding_model, batch_size=batch_size)
    point_ids = [stable_point_id(c.chunk_id) for c in chunks]
    payloads = [chunk_to_payload(c) for c in chunks]

    total = 0
    for start in range(0, len(point_ids), batch_size):
        end = start + batch_size
        total += upsert_points(
            client,
            collection,
            point_ids[start:end],
            vectors[start:end],
            payloads[start:end],
        )

    grouped = group_chunks_by_file(chunks)
    files_manifest: dict[str, Any] = {}
    for file_path, file_chunks in grouped.items():
        abs_path = REPO_ROOT / file_path
        if abs_path.is_file():
            files_manifest[file_path] = {
                "content_hash": file_content_hash(abs_path),
                "chunk_ids": [c.chunk_id for c in file_chunks],
            }

    index_roots = [str(p.relative_to(REPO_ROOT)) for p in resolve_index_dirs()]
    save_manifest(
        MANIFEST_PATH,
        {
            "collection": collection,
            "embedding_model": embedding_model,
            "vector_size": vector_size,
            "qdrant_path": str(QDRANT_PATH) if not qdrant_url else None,
            "qdrant_url": qdrant_url,
            "index_dirs": index_roots,
            "files": files_manifest,
            "total_chunks": len(chunks),
        },
    )

    log.info("Indexed %d chunks from %d files", len(chunks), len(files_manifest))
    return {
        "indexed": total,
        "files": len(files_manifest),
        "chunks": len(chunks),
        "index_dirs": index_roots,
        "collection": collection,
        "embedding_model": embedding_model,
    }


# Backward-compatible alias
index_all_tests = index_all_chunks
