from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Sequence

import numpy as np

# Optional deps
_HAS_ST = False
_HAS_TFIDF = False
try:  # sentence-transformers (preferred)
    from sentence_transformers import SentenceTransformer
    _HAS_ST = True
except Exception:  # pragma: no cover
    pass

try:  # scikit-learn TF-IDF fallback
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    _HAS_TFIDF = True
except Exception:  # pragma: no cover
    pass


@dataclass
class _IndexMeta:
    json_mtime: float
    backend: Literal["st", "tfidf"]


class SemanticSearch:
    """Semantic search over a companies JSON with two interchangeable backends.

    - Preferred: SentenceTransformer (all-MiniLM-L6-v2) with cosine similarity.
    - Fallback: scikit-learn TF-IDF with cosine similarity.

    Features:
      * Lazy, thread-safe model encoding (lock around ST encode).
      * Embedding/vector cache that invalidates when the JSON file changes.
      * Deterministic top-k with stable tie-breaking.
      * Helpful errors when neither backend is available.
    """

    def __init__(
        self,
        companies_json_path: str,
        *,
        backend: Optional[Literal["auto", "st", "tfidf"]] = "auto",
        st_model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    ) -> None:
        self.companies_json_path = companies_json_path
        self.st_model_name = st_model_name
        self._encode_lock = threading.Lock()

        if not os.path.exists(companies_json_path):
            raise FileNotFoundError(f"companies.json not found at {companies_json_path}")

        # Choose backend
        self._mode: Optional[Literal["st", "tfidf"]]
        if backend == "st":
            if not _HAS_ST:
                raise RuntimeError("sentence-transformers is not installed")
            self._mode = "st"
        elif backend == "tfidf":
            if not _HAS_TFIDF:
                raise RuntimeError("scikit-learn is not installed")
            self._mode = "tfidf"
        else:  # auto
            if _HAS_ST:
                self._mode = "st"
            elif _HAS_TFIDF:
                self._mode = "tfidf"
            else:
                raise RuntimeError("Install either sentence-transformers or scikit-learn for search.")

        # Lazy init; build index now
        self._meta: Optional[_IndexMeta] = None
        self._data: List[Dict] = []
        self._texts: List[str] = []

        # Backend-specific members
        self._st_model: Optional[SentenceTransformer] = None if not _HAS_ST else None
        self._st_embeddings: Optional[np.ndarray] = None
        self._tfidf_vectorizer: Optional[TfidfVectorizer] = None if not _HAS_TFIDF else None
        self._tfidf_matrix = None

        self._build_index(force=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def list_companies(self) -> List[Dict]:
        self._maybe_reindex()
        return self._data

    def search(self, query: str, top_k: int = 10) -> List[Dict]:
        if not isinstance(query, str) or not query.strip():
            return []
        top_k = max(1, min(int(top_k), len(self._data) or 1))

        self._maybe_reindex()
        q = query.strip()

        if self._mode == "st":
            assert _HAS_ST
            with self._encode_lock:  # SentenceTransformer is not guaranteed thread-safe
                q_emb = self._st_model.encode([q], normalize_embeddings=True)
            # (N, D) @ (D, 1) -> (N,)
            scores = (self._st_embeddings @ q_emb.T).squeeze(axis=1)
            # stable argsort: highest first, then original index to tie-break
            idx = np.lexsort((np.arange(scores.size), -scores))[:top_k]
        else:
            assert _HAS_TFIDF
            q_vec = self._tfidf_vectorizer.transform([q])
            sims = cosine_similarity(self._tfidf_matrix, q_vec).ravel()
            idx = np.lexsort((np.arange(sims.size), -sims))[:top_k]

        return [self._data[i] for i in idx.tolist()]

    # ------------------------------------------------------------------
    # Indexing
    # ------------------------------------------------------------------
    def _build_index(self, *, force: bool = False) -> None:
        mtime = os.path.getmtime(self.companies_json_path)
        if not force and self._meta and self._meta.json_mtime == mtime:
            return

        with open(self.companies_json_path, "r", encoding="utf-8") as f:
            data: List[Dict] = json.load(f)

        texts = [self._format_row(d) for d in data]

        if self._mode == "st":
            assert _HAS_ST
            if self._st_model is None:
                self._st_model = SentenceTransformer(self.st_model_name)
            # normalize embeddings at encode-time for cosine via dot
            with self._encode_lock:
                emb = self._st_model.encode(texts, normalize_embeddings=True)
            # ensure (N, D) float32
            self._st_embeddings = np.asarray(emb, dtype=np.float32)
        else:
            assert _HAS_TFIDF
            self._tfidf_vectorizer = TfidfVectorizer(stop_words="english")
            self._tfidf_matrix = self._tfidf_vectorizer.fit_transform(texts)

        self._data = data
        self._texts = texts
        self._meta = _IndexMeta(json_mtime=mtime, backend=self._mode)  # type: ignore[arg-type]

    def _maybe_reindex(self) -> None:
        try:
            current = os.path.getmtime(self.companies_json_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"companies.json not found at {self.companies_json_path}")
        if not self._meta or current != self._meta.json_mtime:
            self._build_index(force=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _format_row(d: Dict) -> str:
        name = str(d.get("name", "")).strip()
        ticker = str(d.get("ticker", "")).strip().upper()
        sector = str(d.get("sector", "")).strip()
        # Include more fields to help recall without hurting precision too much
        pieces: List[str] = [name]
        if ticker:
            pieces.append(f"({ticker})")
        if sector:
            pieces.append(f"— {sector}")
        return " ".join(p for p in pieces if p)

    # Convenience for tests/tools
    def reindex_now(self) -> None:
        self._build_index(force=True)

    def backend(self) -> str:
        return self._mode or "unknown"