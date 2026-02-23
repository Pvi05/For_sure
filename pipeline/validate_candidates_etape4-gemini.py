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

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from google import genai
from google.genai import types as genai_types

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Seuil de validation finale. Conforme à MIN_SIMILARITY de provenance_graph.py.
FINAL_THRESHOLD = 0.65

# Parallélisme des appels LLM (ThreadPoolExecutor)
DEFAULT_WORKERS = int(os.getenv("VALIDATE_WORKERS", "4"))

# Modèle Gemini
GEMINI_MODEL = os.getenv("VALIDATE_MODEL", "gemini-2.5-flash")

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
# Client Gemini (singleton)
# ─────────────────────────────────────────────────────────────────────────────

_gemini_client: Optional[genai.Client] = None


def _get_gemini_client() -> genai.Client:
    """Charge le client Gemini de façon paresseuse (lazy singleton)."""
    global _gemini_client
    if _gemini_client is None:
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "GOOGLE_API_KEY non définie. "
                "La passe LLM (étape 4) nécessite une clé Gemini valide."
            )
        _gemini_client = genai.Client(api_key=api_key)
    return _gemini_client


# ─────────────────────────────────────────────────────────────────────────────
# Prompts
# ─────────────────────────────────────────────────────────────────────────────

_LLM_SYSTEM_PROMPT = """Tu es un expert en analyse de désinformation et de propagation de l'information sur les réseaux sociaux.

Ta tâche est de comparer deux posts Bluesky et de déterminer s'ils parlent du MÊME FAIT ou de la MÊME AFFIRMATION principale.

Règles d'évaluation :
- Concentre-toi sur l'AFFIRMATION FACTUELLE centrale, pas sur la forme ou le style.
- Deux posts très différents dans la forme mais rapportant le même fait → score élevé.
- Une reformulation biaisée du même fait doit recevoir un score élevé (c'est précisément ce qu'on cherche à détecter).
- Des posts sur des sujets superficiellement similaires mais avec des faits différents → score bas.

Réponds UNIQUEMENT en JSON valide, sans texte autour :
{
  "same_topic": true,
  "similarity_score": 0.0,
  "reasoning": "explication concise en français (max 80 mots)",
  "shared_entities": ["entité1", "entité2"],
  "shared_claims": ["affirmation1"]
}"""

_USER_TEMPLATE = """POST DE RÉFÉRENCE (plus récent) :
\"\"\"{ref_text}\"\"\"

POST CANDIDAT (antérieur) :
\"\"\"{candidate_text}\"\"\"

Ces deux posts rapportent-ils le même fait ou la même affirmation principale ?"""


# ─────────────────────────────────────────────────────────────────────────────
# Parsing de la réponse LLM
# ─────────────────────────────────────────────────────────────────────────────

def _parse_llm_response(raw: str) -> tuple[float, bool, str]:
    json_match = re.search(r'\{.*\}', raw, re.DOTALL)
    if not json_match:
        logger.warning(
            "LLM: aucun JSON trouvé dans la réponse : %s", raw)
        return -1.0, False, "parse_error"

    try:
        data = json.loads(json_match.group())
    except json.JSONDecodeError as exc:
        logger.warning("LLM: JSON invalide (%s) : %s", exc, raw[:200])
        return -1.0, False, "json_decode_error"

    # ← extract all fields before returning
    score_raw = data.get("similarity_score", 0.0)
    try:
        score = max(0.0, min(1.0, float(score_raw)))
    except (TypeError, ValueError):
        logger.warning("LLM: similarity_score invalide : %s", score_raw)
        return -1.0, False, "invalid_score"

    # ← key aligns with prompt JSON
    same_claim = bool(data.get("same_topic", False))
    reasoning = str(data.get("reasoning", ""))
    return score, same_claim, reasoning


# ─────────────────────────────────────────────────────────────────────────────
# Appel LLM — Gemini
# ─────────────────────────────────────────────────────────────────────────────

def _call_gemini_single(
    client: genai.Client,
    ref_text: str,
    candidate_text: str,
    candidate_idx: int,
) -> tuple[int, float, bool, str]:
    """
    Appel Gemini pour une seule paire (ref, candidat).
    Retourne (candidate_idx, similarity_score, same_claim, reasoning).
    similarity_score == -1.0 en cas d'échec.
    """
    prompt = _USER_TEMPLATE.format(
        ref_text=ref_text.strip(),
        candidate_text=candidate_text.strip(),  # ← was candidate.text.strip()
    )
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=_LLM_SYSTEM_PROMPT,
                temperature=0.0,
                max_output_tokens=2000,
            ),
        )
        raw = response.text.strip() if response.text else ""
        score, same_claim, reasoning = _parse_llm_response(raw)
        logger.debug(
            "LLM[%d]: score=%.2f  same_claim=%s  reasoning=%s",
            candidate_idx, score, same_claim, reasoning[:60]
        )
        return candidate_idx, score, same_claim, reasoning

    except Exception as exc:
        logger.warning("LLM[%d]: appel échoué (%s)", candidate_idx, exc)
        print(f"   ⚠️  Gemini[{candidate_idx}] erreur : {exc}")
        return candidate_idx, -1.0, False, f"api_error: {exc}"


def _pass_gemini_parallel(
    ref_text: str,
    candidates: list[tuple[int, str]],   # [(original_idx, text), ...]
    workers: int = DEFAULT_WORKERS,
) -> dict[int, tuple[float, bool, str]]:
    """
    Lance les appels Gemini en parallèle pour tous les candidats.
    Retourne un dict {original_idx: (score, same_claim, reasoning)}.
    """
    client = _get_gemini_client()
    results: dict[int, tuple[float, bool, str]] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _call_gemini_single, client, ref_text, text, orig_idx
            ): orig_idx
            for orig_idx, text in candidates
        }

        for future in as_completed(futures):
            orig_idx = futures[future]
            try:
                _, score, same_claim, reasoning = future.result()
            except Exception as exc:
                logger.error("Future[%d] inattendue : %s", orig_idx, exc)
                score, same_claim, reasoning = -1.0, False, "unexpected_error"

            results[orig_idx] = (score, same_claim, reasoning)
            logger.info(
                "  Candidat[%d] → score=%.2f  same_claim=%s  (%s)",
                orig_idx, score, same_claim, reasoning[:50]
            )

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale — API publique
# ─────────────────────────────────────────────────────────────────────────────
def validate_candidates(
    ref_post,
    candidates: list,
    final_threshold: float = FINAL_THRESHOLD,
    workers: int = DEFAULT_WORKERS,
) -> list[ValidationResult]:
    """
    Étape 4 — Validation sémantique des posts candidats via Gemini 2.5 Flash.

    Args:
        ref_post   : BlueskyPost de référence (le post dont on cherche les antécédents).
                     Doit exposer un attribut `.text` (str).
        candidates : Liste de BlueskyPost candidats (issus de l'étape 3).
                     Chaque objet doit exposer `.text`.
        final_threshold :
                     Seuil de validation (score Gemini).
                     Défaut : 0.65, conforme à MIN_SIMILARITY du graphe.
        workers    : Nombre de threads parallèles pour les appels LLM.

    Returns:
        list[ValidationResult] pour les candidats validés,
        triée par similarity_score décroissant.
        Seuls les candidats avec similarity_score >= final_threshold sont inclus.

    Raises:
        EnvironmentError : si GOOGLE_API_KEY n'est pas définie.
    """
    if not candidates:
        print("[Étape 4] Aucun candidat → retour vide.")
        return []

    ref_text = ref_post.text
    candidate_texts = [c.text for c in candidates]  # ← was missing

    print(
        f"[Étape 4] 🧠 Validation Gemini sur {len(candidates)} candidat(s) "
        f"(workers={workers}, model={GEMINI_MODEL})…"
    )

    indexed_candidates = list(enumerate(candidate_texts))

    llm_results = _pass_gemini_parallel(
        ref_text, indexed_candidates, workers=workers)

    validated: list[ValidationResult] = []

    for idx, (post, text) in enumerate(zip(candidates, candidate_texts)):
        score, same_claim, reasoning = llm_results.get(
            idx, (-1.0, False, "not_called"))

        final_score = score if score >= 0.0 else 0.0

        status = "✓ VALIDÉ" if final_score >= final_threshold else "✗ rejeté"
        print(
            f"   [{idx}] score={final_score:.2f}  same_claim={same_claim}"
            f"  @{getattr(post, 'author_handle', '?')} → {status}"
        )

        if final_score >= final_threshold:
            validated.append(ValidationResult(
                post=post,
                similarity_score=round(final_score, 4),
                same_topic=same_claim,
                reasoning=reasoning,
            ))

    validated.sort(key=lambda r: r.similarity_score, reverse=True)

    print(
        f"[Étape 4] ✅ {len(validated)}/{len(candidates)} candidat(s) validé(s) "
        f"(seuil={final_threshold})"
    )
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
            "Recette de quiche lorraine maison : 200g de lardons, 3 œufs, 20cl de crème.",
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
    print(f"Modèle : {GEMINI_MODEL}")
    print("="*60 + "\n")

    results = validate_candidates(ref, candidates)
    print(f"\n→ {len(results)} candidat(s) validé(s) :\n")
    for r in results:
        print(f"  [{r.similarity_score:.4f}]  @{r.post.author_handle}")
        print(f"    same_topic={r.same_topic}  reasoning: {r.reasoning[:80]}")
