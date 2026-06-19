"""Index lifecycle: create, update, and cleanup the ocs-ci code vector DB."""

import logging
import shutil
from pathlib import Path
from typing import Any

from code_parser import (
    chunk_to_payload,
    embedding_text,
    file_content_hash,
    group_chunks_by_file,
    load_manifest,
    parse_index_chunks,
    save_manifest,
    stable_point_id,
)
from config import (
    DATA_DIR,
    DEFAULT_BATCH_SIZE,
    DEFAULT_COLLECTION,
    DEFAULT_EMBEDDING_MODEL,
    MANIFEST_PATH,
    QDRANT_PATH,
    REPO_ROOT,
)
from embedder import embed_texts
from qdrant_store import (
    collection_info,
    delete_collection,
    delete_points,
    get_client,
    upsert_points,
)

log = logging.getLogger(__name__)


def create_db(
    *,
    index_dir: Path | None = None,
    collection: str = DEFAULT_COLLECTION,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
    batch_size: int = DEFAULT_BATCH_SIZE,
    qdrant_url: str | None = None,
    max_files: int | None = None,
) -> dict[str, Any]:
    """Create a fresh vector DB from configured ocs-ci directories."""
    from retrieval import index_all_chunks

    return index_all_chunks(
        index_dir=index_dir,
        collection=collection,
        embedding_model=embedding_model,
        batch_size=batch_size,
        qdrant_url=qdrant_url,
        recreate=True,
        max_files=max_files,
    )


def update_db(
    *,
    index_dir: Path | None = None,
    collection: str = DEFAULT_COLLECTION,
    embedding_model: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    qdrant_url: str | None = None,
    max_files: int | None = None,
) -> dict[str, Any]:
    """
    Incrementally update the vector DB for changed files.

    Compares file content hashes in the manifest; re-indexes only changed files.
    """
    manifest = load_manifest(MANIFEST_PATH)
    model = (
        embedding_model or manifest.get("embedding_model") or DEFAULT_EMBEDDING_MODEL
    )

    client = get_client(qdrant_url=qdrant_url)
    if not client.collection_exists(collection):
        log.info("Collection %s not found; running full create", collection)
        return create_db(
            index_dir=index_dir,
            collection=collection,
            embedding_model=model,
            batch_size=batch_size,
            qdrant_url=qdrant_url,
            max_files=max_files,
        )

    all_chunks = parse_index_chunks(limit_root=index_dir, max_files=max_files)
    grouped = group_chunks_by_file(all_chunks)
    known_files: dict[str, Any] = manifest.get("files", {})

    to_reindex: list[str] = []
    to_delete_chunk_ids: list[str] = []

    for file_path, file_chunks in grouped.items():
        abs_path = REPO_ROOT / file_path
        if not abs_path.is_file():
            continue
        current_hash = file_content_hash(abs_path)
        prev = known_files.get(file_path)
        if not prev or prev.get("content_hash") != current_hash:
            to_reindex.append(file_path)
            if prev:
                to_delete_chunk_ids.extend(
                    prev.get("chunk_ids", prev.get("node_ids", []))
                )

    removed_files = set(known_files) - set(grouped)
    for file_path in removed_files:
        to_delete_chunk_ids.extend(
            known_files[file_path].get(
                "chunk_ids", known_files[file_path].get("node_ids", [])
            )
        )
        del known_files[file_path]

    if not to_reindex and not to_delete_chunk_ids:
        total = manifest.get("total_chunks", manifest.get("total_tests", 0))
        log.info("Vector DB is up to date (%d chunks)", total)
        return {"updated": 0, "deleted": 0, "unchanged": True}

    delete_ids = [stable_point_id(cid) for cid in to_delete_chunk_ids]
    deleted = delete_points(client, collection, delete_ids)

    upsert_chunks = []
    for file_path in to_reindex:
        upsert_chunks.extend(grouped[file_path])

    upserted = 0
    if upsert_chunks:
        texts = [embedding_text(c) for c in upsert_chunks]
        vectors = embed_texts(texts, model_name=model, batch_size=batch_size)
        point_ids = [stable_point_id(c.chunk_id) for c in upsert_chunks]
        payloads = [chunk_to_payload(c) for c in upsert_chunks]

        for start in range(0, len(point_ids), batch_size):
            end = start + batch_size
            upserted += upsert_points(
                client,
                collection,
                point_ids[start:end],
                vectors[start:end],
                payloads[start:end],
            )

        for file_path in to_reindex:
            abs_path = REPO_ROOT / file_path
            file_chunks = grouped[file_path]
            known_files[file_path] = {
                "content_hash": file_content_hash(abs_path),
                "chunk_ids": [c.chunk_id for c in file_chunks],
            }

    save_manifest(
        MANIFEST_PATH,
        {
            **manifest,
            "collection": collection,
            "embedding_model": model,
            "files": known_files,
            "total_chunks": len(all_chunks),
        },
    )

    log.info(
        "Updated %d chunks in %d files; deleted %d stale points",
        upserted,
        len(to_reindex),
        deleted,
    )
    return {
        "updated": upserted,
        "deleted": deleted,
        "files_reindexed": len(to_reindex),
        "total_chunks": len(all_chunks),
    }


def cleanup_db(
    *,
    collection: str = DEFAULT_COLLECTION,
    qdrant_url: str | None = None,
    remove_data: bool = False,
) -> dict[str, Any]:
    """Remove the Qdrant collection and optionally wipe local data directory."""
    client = get_client(qdrant_url=qdrant_url)
    existed = client.collection_exists(collection)
    if existed:
        delete_collection(client, collection)

    if MANIFEST_PATH.is_file():
        MANIFEST_PATH.unlink()

    removed_path = False
    if remove_data and not qdrant_url and DATA_DIR.is_dir():
        shutil.rmtree(DATA_DIR, ignore_errors=True)
        removed_path = True

    return {
        "collection_deleted": existed,
        "data_dir_removed": removed_path,
        "collection": collection,
    }


def db_status(
    *,
    collection: str = DEFAULT_COLLECTION,
    qdrant_url: str | None = None,
) -> dict[str, Any]:
    """Return vector DB status and manifest summary."""
    client = get_client(qdrant_url=qdrant_url)
    info = collection_info(client, collection)
    manifest = load_manifest(MANIFEST_PATH)

    return {
        "collection": collection,
        "qdrant": {
            "url": qdrant_url,
            "local_path": str(QDRANT_PATH) if not qdrant_url else None,
            **info,
        },
        "manifest": {
            "exists": MANIFEST_PATH.is_file(),
            "embedding_model": manifest.get("embedding_model"),
            "total_chunks": manifest.get("total_chunks", manifest.get("total_tests")),
            "indexed_files": len(manifest.get("files", {})),
            "index_dirs": manifest.get("index_dirs"),
        },
    }
