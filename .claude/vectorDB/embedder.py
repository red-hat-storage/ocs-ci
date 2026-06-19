"""Generate text embeddings for ocs-ci test metadata."""

import logging
from typing import Sequence

log = logging.getLogger(__name__)

_model = None
_model_name: str | None = None


def get_embedder(model_name: str):
    """
    Lazy-load a sentence-transformers model.

    Args:
        model_name (str): HuggingFace model id

    Returns:
        SentenceTransformer: Loaded model

    """
    global _model, _model_name
    if _model is not None and _model_name == model_name:
        return _model

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise ImportError(
            "sentence-transformers is required. Install with:\n"
            "  pip install -r .claude/vectorDB/requirements.txt"
        ) from exc

    log.info("Loading embedding model: %s", model_name)
    _model = SentenceTransformer(model_name)
    _model_name = model_name
    return _model


def embed_texts(
    texts: Sequence[str],
    *,
    model_name: str,
    batch_size: int = 64,
    show_progress: bool = True,
) -> list[list[float]]:
    """
    Embed a list of texts.

    Args:
        texts (Sequence[str]): Texts to embed
        model_name (str): HuggingFace model id
        batch_size (int): Encoding batch size
        show_progress (bool): Show tqdm progress bar

    Returns:
        list[list[float]]: Embedding vectors

    """
    if not texts:
        return []

    model = get_embedder(model_name)
    vectors = model.encode(
        list(texts),
        batch_size=batch_size,
        show_progress_bar=show_progress,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    return [vector.tolist() for vector in vectors]
