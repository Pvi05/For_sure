"""
keywords.py — Étape 2 de la pipeline TrustGraph
================================================
Extraction de mots-clés, entités et affirmations depuis le texte d'un post
Bluesky via Gemini 2.5 Flash (Google).

Place dans la pipeline :
    fetcher.py  →  keywords.py  →  searcher.py  →  validator.py  →  ...

Retourne :
    { "keywords": [...], "entities": [...], "claims": [...] }

Dépendances :
    pip install google-genai

Variables d'environnement :
    GOOGLE_API_KEY  — clé API Gemini (obligatoire)
    KEYWORDS_MODEL  — modèle Gemini à utiliser (défaut : gemini-2.5-flash)
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field

from google import genai
from google.genai import types as genai_types

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

KEYWORDS_MODEL = os.getenv("KEYWORDS_MODEL", "gemini-2.5-flash")

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Prompts
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """Tu es un expert en analyse de contenu et en extraction d'information.
Ta tâche est d'analyser le texte d'un post Bluesky et d'en extraire les éléments clés
qui permettront de retrouver des posts antérieurs traitant du même sujet.

Réponds UNIQUEMENT en JSON valide, sans texte autour, avec exactement ces clés :
{
  "keywords": ["<terme générique>", "..."],
  "entities": ["<entité nommée>", "..."],
  "claims": ["<affirmation factuelle précise>", "..."]
}"""

_USER_TEMPLATE = """Analyse ce post Bluesky et extrais les éléments suivants :

POST :
\"\"\"{text}\"\"\"

Règles :
- keywords : 3 à 6 termes génériques utiles pour une recherche textuelle, sans stop-words.
- entities  : uniquement les entités nommées identifiables (personnes, orgs, lieux, publications…).
- claims    : 1 à 3 affirmations factuelles centrales, formulées comme des phrases courtes
              et précises (ex : "le café réduit de 12% le risque de diabète de type 2").
- Si une catégorie est vide, retourne une liste vide [].
- Ne jamais inventer d'information absente du texte."""


# ─────────────────────────────────────────────────────────────────────────────
# Modèle de données
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class KeywordResult:
    """Résultat structuré de l'extraction de l'étape 2."""
    keywords: list[str] = field(
        default_factory=list)   # Termes génériques importants
    entities: list[str] = field(default_factory=list)   # Entités nommées
    # Affirmations factuelles précises
    claims:   list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "keywords": self.keywords,
            "entities": self.entities,
            "claims":   self.claims,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Client Gemini
# ─────────────────────────────────────────────────────────────────────────────

def _get_client() -> genai.Client:
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GOOGLE_API_KEY non définie. "
            "Exporter la variable avant de lancer le script."
        )
    return genai.Client(api_key=api_key)


# ─────────────────────────────────────────────────────────────────────────────
# Parsing de la réponse LLM
# ─────────────────────────────────────────────────────────────────────────────

def _parse_response(raw: str) -> KeywordResult:
    """
    Parse la réponse JSON de Gemini.
    Retourne un KeywordResult avec des listes vides en cas d'erreur.
    """
    json_match = re.search(r'\{.*\}', raw, re.DOTALL)
    if not json_match:
        logger.warning("Gemini: aucun JSON trouvé dans : %s", raw[:200])
        return KeywordResult()

    try:
        data = json.loads(json_match.group())
    except json.JSONDecodeError as exc:
        logger.warning("Gemini: JSON invalide (%s)", exc)
        return KeywordResult()

    return KeywordResult(
        keywords=[str(k) for k in data.get("keywords", []) if k],
        entities=[str(e) for e in data.get("entities", []) if e],
        claims=[str(c) for c in data.get("claims",   []) if c],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale — Étape 2
# ─────────────────────────────────────────────────────────────────────────────

def extract_keywords(post_text: str, model: str = KEYWORDS_MODEL) -> KeywordResult:
    """
    Extrait les mots-clés, entités et affirmations d'un post Bluesky via Gemini.

    Args:
        post_text : Texte brut du post à analyser.
        model     : Identifiant du modèle Gemini (défaut : gemini-2.5-flash).

    Returns:
        KeywordResult avec les listes keywords, entities, claims.
    """
    logger.info("Étape 2 — extraction keywords via %s…", model)

    client = _get_client()
    prompt = _USER_TEMPLATE.format(text=post_text.strip())

    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=_SYSTEM_PROMPT,
                temperature=0.0,
                max_output_tokens=2048,
                thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
            ),
        )
        raw = (response.text or "").strip()
        # Nettoyage des backticks éventuels
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = _parse_response(raw)
        logger.info(
            "Étape 2 — %d keyword(s), %d entité(s), %d affirmation(s)",
            len(result.keywords), len(result.entities), len(result.claims),
        )
        return result

    except Exception as exc:
        logger.warning("Étape 2 — appel Gemini échoué (%s)", exc)
        return KeywordResult()


# ─────────────────────────────────────────────────────────────────────────────
# Mode mock (pour la démo sans API)
# ─────────────────────────────────────────────────────────────────────────────

_MOCK_RESULTS: dict[str, KeywordResult] = {
    "café": KeywordResult(
        keywords=["café", "diabète", "étude", "corrélation", "risque"],
        entities=["Nature", "diabète de type 2"],
        claims=[
            "boire 3 tasses de café par jour réduit de 12% le risque de diabète de type 2",
            "la corrélation ne prouve pas la causalité",
        ],
    ),
}

_MOCK_DEFAULT = KeywordResult(
    keywords=["information", "étude", "résultat"],
    entities=["source inconnue"],
    claims=["affirmation principale du post"],
)


def extract_keywords_mock(post_text: str) -> KeywordResult:
    """
    Version mock d'extract_keywords.
    Retourne des données statiques selon le contenu du texte.
    À utiliser pour la démo sans accès à l'API Google.
    """
    text_lower = post_text.lower()
    for key, result in _MOCK_RESULTS.items():
        if key in text_lower:
            logger.info("Étape 2 MOCK — résultat '%s' retourné", key)
            return result

    logger.info("Étape 2 MOCK — résultat par défaut retourné")
    return _MOCK_DEFAULT


# ─────────────────────────────────────────────────────────────────────────────
# Smoke test local
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    sample = "Bravo au journaliste de bfm sur place, qui répète à chaque fois qu'il prend la parole que les participants au rassemblement de Lyon essaient de masquer leur appartenance mais qu'il voit des croix gammées etc et que c'est des néonazis"

    print("\n" + "="*60)
    print(f"SMOKE TEST keywords.py  —  modèle : {KEYWORDS_MODEL}")
    print("="*60 + "\n")

    if os.environ.get("GOOGLE_API_KEY"):
        print("\n=== Test mode réel (Gemini) ===")
        result_real = extract_keywords(sample)
        print(json.dumps(result_real.to_dict(), ensure_ascii=False, indent=2))

        print(result_real.to_dict())
    else:
        print("\n[Info] GOOGLE_API_KEY non définie — test réel ignoré.")
