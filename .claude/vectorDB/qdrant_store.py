"""Qdrant vector store operations for ocs-ci test metadata."""

import logging
from pathlib import Path
from typing import Any, Sequence

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels

from config import DEFAULT_VECTOR_SIZE, QDRANT_PATH

log = logging.getLogger(__name__)


def get_client(
    *, qdrant_url: str | None = None, qdrant_path: Path | None = None
) -> QdrantClient:
    """
    Create a Qdrant client (embedded local or remote).

    Args:
        qdrant_url (str | None): Remote Qdrant URL (e.g. http://localhost:6333)
        qdrant_path (Path | None): Local embedded storage path

    Returns:
        QdrantClient: Connected client

    """
    if qdrant_url:
        return QdrantClient(url=qdrant_url)
    path = qdrant_path or QDRANT_PATH
    path.mkdir(parents=True, exist_ok=True)
    return QdrantClient(path=str(path))


def collection_exists(client: QdrantClient, collection: str) -> bool:
    """Return True if the collection exists."""
    return client.collection_exists(collection)


def create_collection(
    client: QdrantClient,
    collection: str,
    *,
    vector_size: int = DEFAULT_VECTOR_SIZE,
    recreate: bool = False,
) -> None:
    """
    Create (or recreate) the Qdrant collection.

    Args:
        client (QdrantClient): Qdrant client
        collection (str): Collection name
        vector_size (int): Embedding dimension
        recreate (bool): Drop existing collection first

    """
    if recreate and collection_exists(client, collection):
        log.info("Deleting existing collection: %s", collection)
        client.delete_collection(collection)

    if collection_exists(client, collection):
        log.info("Collection already exists: %s", collection)
        return

    log.info("Creating collection: %s (dim=%d)", collection, vector_size)
    client.create_collection(
        collection_name=collection,
        vectors_config=qmodels.VectorParams(
            size=vector_size,
            distance=qmodels.Distance.COSINE,
        ),
    )


def upsert_points(
    client: QdrantClient,
    collection: str,
    point_ids: Sequence[str],
    vectors: Sequence[list[float]],
    payloads: Sequence[dict[str, Any]],
) -> int:
    """
    Upsert vectors into the collection.

    Returns:
        int: Number of points upserted

    """
    if not point_ids:
        return 0

    points = [
        qmodels.PointStruct(id=pid, vector=vector, payload=payload)
        for pid, vector, payload in zip(point_ids, vectors, payloads, strict=True)
    ]
    client.upsert(collection_name=collection, points=points, wait=True)
    return len(points)


def delete_points(
    client: QdrantClient, collection: str, point_ids: Sequence[str]
) -> int:
    """Delete points by id."""
    if not point_ids:
        return 0
    client.delete(
        collection_name=collection,
        points_selector=qmodels.PointIdsList(points=list(point_ids)),
        wait=True,
    )
    return len(point_ids)


def delete_collection(client: QdrantClient, collection: str) -> None:
    """Delete the entire collection."""
    if collection_exists(client, collection):
        client.delete_collection(collection)
        log.info("Deleted collection: %s", collection)


def collection_info(client: QdrantClient, collection: str) -> dict[str, Any]:
    """Return collection statistics."""
    if not collection_exists(client, collection):
        return {"exists": False, "points_count": 0}

    info = client.get_collection(collection)
    return {
        "exists": True,
        "points_count": info.points_count,
        "status": str(info.status),
        "vector_size": info.config.params.vectors.size,
    }


def search_similar(
    client: QdrantClient,
    collection: str,
    query_vector: list[float],
    *,
    top_k: int = 10,
    score_threshold: float | None = None,
    coverage_areas: list[str] | None = None,
    chunk_type: str | None = None,
    source_dir: str | None = None,
) -> list[dict[str, Any]]:
    """
    Search for similar code/tests by embedding vector.

    Args:
        chunk_type (str | None): Filter by payload chunk_type (e.g. "test", "file")
        source_dir (str | None): Filter by top-level index directory

    Returns:
        list[dict]: Ranked matches with score and payload

    """
    if not collection_exists(client, collection):
        return []

    must: list[qmodels.FieldCondition] = []
    if coverage_areas:
        must.append(
            qmodels.FieldCondition(
                key="coverage_areas",
                match=qmodels.MatchAny(any=coverage_areas),
            )
        )
    if chunk_type:
        must.append(
            qmodels.FieldCondition(
                key="chunk_type",
                match=qmodels.MatchValue(value=chunk_type),
            )
        )
    if source_dir:
        must.append(
            qmodels.FieldCondition(
                key="source_dir",
                match=qmodels.MatchValue(value=source_dir),
            )
        )

    query_filter = qmodels.Filter(must=must) if must else None

    results = client.query_points(
        collection_name=collection,
        query=query_vector,
        limit=top_k,
        score_threshold=score_threshold,
        query_filter=query_filter,
        with_payload=True,
    )

    matches = []
    for point in results.points:
        payload = point.payload or {}
        matches.append(
            {
                "score": round(point.score, 4),
                "node_id": payload.get("node_id"),
                "file_path": payload.get("file_path"),
                "chunk_type": payload.get("chunk_type"),
                "name": payload.get("name"),
                "source_dir": payload.get("source_dir"),
                "test_name": (
                    payload.get("name") if payload.get("chunk_type") == "test" else None
                ),
                "class_name": payload.get("class_name"),
                "docstring": payload.get("docstring", "")[:300],
                "jira_ids": payload.get("jira_ids", []),
                "coverage_areas": payload.get("coverage_areas", []),
                "pytest_command": payload.get("pytest_command"),
                "match_reasons": [f"semantic similarity: {point.score:.3f}"],
            }
        )
    return matches
