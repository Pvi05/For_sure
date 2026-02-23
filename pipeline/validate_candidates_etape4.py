"""
validate_candidates_etape4.py — Étape 4 de la pipeline
=======================================================
Validation sémantique des posts candidats via Gemini 2.5 Flash.

Pour chaque candidat, Gemini évalue si le post candidat et le post de référence
rapportent le MÊME FAIT ou la MÊME AFFIRMATION principale.

  SCORE FINAL = llm_score retourné par Gemini (0.0 → 1.0)
  Retour      : list[ValidationResult] filtrés à ≥ FINAL_THRESHOLD,
                triés par score décroissant.

Dépendances :
    pip install google-genai

Variables d'environnement :
    GOOGLE_API_KEY   — clé API Gemini (obligatoire)
    VALIDATE_MODEL   — modèle Gemini (défaut : gemini-2.5-flash)
    VALIDATE_WORKERS — nombre de threads parallèles (défaut : 4)
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

import numpy as np
from sentence_transformers import SentenceTransformer

import json
import logging
import os
import re

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

_embedder: SentenceTransformer | None = None
EMBED_MODEL = os.getenv(
    "EMBED_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
PEARSON_THRESHOLD = 0.3

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Résultat de validation (public — utilisé par Provenance_graph_etape6.py)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ValidationResult:
    """Résultat de validation pour un post candidat."""
    post: object              # BlueskyPost
    similarity_score: float   # Score final 0.0–1.0
    same_topic: bool          # Le LLM considère-t-il que c'est le même fait ?
    reasoning: str            # Justification textuelle du LLM

# ─────────────────────────────────────────────────────────────────────────────
# Passe 1 — Pré-filtre par corrélation de Pearson sur embeddings
# ─────────────────────────────────────────────────────────────────────────────


def _get_embedder() -> SentenceTransformer:
    global _embedder
    if _embedder is None:
        _embedder = SentenceTransformer(EMBED_MODEL)
    return _embedder


def _pearson(a: np.ndarray, b: np.ndarray) -> float:
    """Corrélation de Pearson = similarité cosinus sur vecteurs centrés."""
    a_c = a - a.mean()
    b_c = b - b.mean()
    norm_a = np.linalg.norm(a_c)
    norm_b = np.linalg.norm(b_c)
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return float(np.dot(a_c, b_c) / (norm_a * norm_b))


def _pearson_prefilter(
    ref_text: str,
    candidates: list,
    threshold: float = PEARSON_THRESHOLD,
) -> tuple[list, list[float]]:
    """
    Pré-filtre les candidats par corrélation de Pearson entre embeddings.
    N'utilise que numpy — pas de scipy.

    Args:
        ref_text  : Texte du post de référence.
        candidates: Liste de BlueskyPost candidats.
        threshold : Corrélation de Pearson minimale pour retenir un candidat.

    Returns:
        (survivors, scores) — candidats retenus et leur score Pearson.
    """
    embedder = _get_embedder()
    texts = [ref_text] + [c.text for c in candidates]
    embeddings = embedder.encode(texts, convert_to_numpy=True)

    ref_vec = embeddings[0]
    survivors, scores = [], []

    for i, candidate in enumerate(candidates):
        corr = _pearson(ref_vec, embeddings[i + 1])
        status = "✓ passe" if corr >= threshold else "✗ filtré"
        print(
            f"   [Passe 1 — Pearson] [{i}] r={corr:.3f}  "
            f"@{getattr(candidate, 'author_handle', '?')} → {status}"
        )
        if corr >= threshold:
            survivors.append(candidate)
            scores.append(corr)

    print(
        f"[Passe 1] ✅ {len(survivors)}/{len(candidates)} candidat(s) "
        f"retenus (seuil Pearson={threshold})"
    )
    return survivors, scores

# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale — API publique
# ─────────────────────────────────────────────────────────────────────────────


def validate_candidates(
    ref_post,
    candidates: list,
    pearson_threshold: float = PEARSON_THRESHOLD,
) -> list[ValidationResult]:
    """
    Étape 4 — Pré-filtre sémantique par corrélation de Pearson sur embeddings.

    Args:
        ref_post          : BlueskyPost de référence.
        candidates        : Liste de BlueskyPost candidats (étape 3).
        pearson_threshold : Seuil de corrélation de Pearson (défaut : 0.5).

    Returns:
        list[ValidationResult] triée par similarity_score décroissant.
    """
    if not candidates:
        print("[Étape 4] Aucun candidat → retour vide.")
        return []

    print(f"\n[Étape 4] ── Pré-filtre Pearson (seuil={pearson_threshold})")
    survivors, scores = _pearson_prefilter(
        ref_post.text, candidates, threshold=pearson_threshold)

    validated = [
        ValidationResult(
            post=post,
            similarity_score=round(score, 4),
            same_topic=True,
            reasoning="embedding_prefilter",
        )
        for post, score in zip(survivors, scores)
    ]
    validated.sort(key=lambda r: r.similarity_score, reverse=True)

    print(
        f"[Étape 4] ✅ {len(validated)}/{len(candidates)} candidat(s) retenus")
    return validated

# ─────────────────────────────────────────────────────────────────────────────
# Smoke test (sans dépendance externe sauf GOOGLE_API_KEY)
# ─────────────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    """
    Smoke test — valide un appel Gemini réel.
    Lance : python validate_candidates_etape4.py
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    class _FakePost:
        def __init__(self, text: str, handle: str = "user"):
            self.text = text
            self.author_handle = handle
            self.uri = f"at://fake/{handle}"
            self.date = "2025-01-01T00:00:00Z"

    ref = _FakePost(
        "Des manifestants arborant des croix gammées ont été photographiés lors "
        "du rassemblement de Lyon selon le journaliste de BFM présent sur place.",
        handle="ref_post",
    )
    candidates = [
        _FakePost(
            # Très similaire — doit être validé
            "Le correspondant de BFM TV à Lyon confirme avoir vu des symboles nazis "
            "parmi les participants au rassemblement.",
            handle="similar_post",
        ),
        _FakePost(
            # Sujet différent — doit être rejeté
            "Ma dernière course strava à lyon, je suis allé à droite du parc de la tête d'or",
            handle="unrelated_post"
        ),
        _FakePost(
            # Modérément similaire
            "Les organisateurs du rassemblement lyonnais démentent toute présence "
            "d'éléments d'extrême droite lors de l'événement.",
            handle="moderate_post"
        ),
    ]

    print("\n" + "="*60)
    print(f"SMOKE TEST — validate_candidates_etape4.py")
    print("="*60 + "\n")

    results = validate_candidates(ref, candidates)
    print(f"\n→ {len(results)} candidat(s) validé(s) :\n")
    for r in results:
        print(f"  [{r.similarity_score:.4f}]  @{r.post.author_handle}")
        print(f"    same_topic={r.same_topic}  reasoning: {r.reasoning[:80]}")
