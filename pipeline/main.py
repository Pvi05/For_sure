"""
main.py — Point d'entrée de la pipeline TrustGraph
====================================================
Exécute la pipeline complète depuis une URL ou un URI Bluesky :

    Étape 1  fetch_post           → BlueskyPost
    Étape 2  extract_keywords     → KeywordResult
    Étape 3  search_prior_posts   → candidats bruts
    Étape 4  validate_candidates  → candidats filtrés (embedding + LLM)
    Étape 5  analyze_branch       → BranchAnalysis par arête
    Étape 6  ProvenanceGraph      → graphe NetworkX
    ────────────────────────────────────────────────
    Étape 9  synthesize_chain     → SynthesisResult (narrative LLM)

Sorties produites dans ./output/<timestamp>/ :
    provenance.json   — graphe complet (compatible D3.js)
    provenance.graphml— graphe exportable Gephi / Cytoscape
    synthesis.json    — analyse narrative du parcours de l'information

Usage :
    python main.py <url_ou_uri> [max_depth] [window_days]

Exemples :
    python main.py https://bsky.app/profile/alice.bsky.social/post/3abc
    python main.py at://did:plc:xxx/app.bsky.feed.post/yyy 3 14

Variables d'environnement requises :
    GOOGLE_API_KEY   — pour Gemini (étapes 2 et 9)

Variables d'environnement optionnelles :
    BSKY_HANDLE      — handle Bluesky pour l'authentification (ex: user.bsky.social)
    BSKY_PASSWORD    — mot de passe d'application Bluesky
    SYNTHESIS_MODEL  — modèle Gemini pour la synthèse (défaut: gemini-2.5-flash)
    KEYWORDS_MODEL   — modèle Gemini pour les keywords (défaut: gemini-2.5-flash)
    OPENAI_MODEL     — modèle OpenAI pour la validation (défaut: gpt-4o-mini)
"""

from __future__ import annotations
import logging
from pathlib import Path
from datetime import datetime
import sys
import os
logging.getLogger("httpx").setLevel(logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
# Logging — configuré en premier pour capturer tous les imports
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("trustgraph.main")


# ─────────────────────────────────────────────────────────────────────────────
# Imports de la pipeline
# ─────────────────────────────────────────────────────────────────────────────

try:
    from Provenance_graph_etape6 import ProvenanceGraph
except ImportError as e:
    logger.error("Impossible d'importer ProvenanceGraph : %s", e)
    sys.exit(1)

try:
    from graphetoanswer import synthesize
except ImportError as e:
    logger.error("Impossible d'importer synthesizer : %s", e)
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_MAX_DEPTH = 4
DEFAULT_WINDOW_DAYS = 2
OUTPUT_DIR = Path("output")


# ─────────────────────────────────────────────────────────────────────────────
# Création du répertoire de sortie
# ─────────────────────────────────────────────────────────────────────────────

def _make_output_dir() -> Path:
    """Crée ./output/<timestamp>/ et retourne le Path."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = OUTPUT_DIR / ts
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Répertoire de sortie : %s", out_dir.resolve())
    return out_dir


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────

def run(
    target_url:  str,
    max_depth:   int = DEFAULT_MAX_DEPTH,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> None:
    """
    Exécute la pipeline complète depuis une URL ou URI Bluesky.

    Args:
        target_url    : URL Bluesky (https://bsky.app/...) ou URI AT Protocol.
        max_depth     : Profondeur maximale de remontée dans le graphe.
        window_days   : Fenêtre temporelle de recherche des posts antérieurs (jours).
    """
    out_dir = _make_output_dir()

    # ── Étapes 1–6 : construction du graphe ───────────────────────────────
    print(f"\n🚀  TrustGraph — démarrage de l'analyse")
    print(f"    URL cible    : {target_url}")
    print(f"    Profondeur   : {max_depth}")
    print(f"    Fenêtre      : -{window_days} jours\n")

    graph = ProvenanceGraph(max_depth=max_depth, window_days=window_days)

    try:
        graph.build(target_url)
    except Exception as e:
        logger.error("Échec de la construction du graphe : %s", e)
        raise

    graph.print_summary()

    # ── Sauvegarde du graphe ───────────────────────────────────────────────
    graph_json_path = out_dir / "provenance.json"
    graph_graphml_path = out_dir / "provenance.graphml"

    graph.save_json(str(graph_json_path))
    graph.save_graphml(str(graph_graphml_path))

    # ── Étape 9 : synthèse narrative ──────────────────────────────────────
    print("\n🧠  [Étape 9] Synthèse narrative du parcours…")

    try:
        result = synthesize(graph)
    except EnvironmentError as e:
        logger.error("Étape 9 échouée : %s", e)
        raise
    except Exception as e:
        logger.error("Étape 9 échouée : %s", e)
        raise

    # ── Sauvegarde et affichage de la synthèse ────────────────────────────
    result.save_json(str(out_dir / "synthesis.json"))
    result.print_summary()

    print(f"✅  Analyse terminée. Fichiers dans : {out_dir.resolve()}/")
    print(f"    ├── provenance.json")
    print(f"    ├── provenance.graphml")
    print(f"    └── synthesis.json\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    target_url = args[0] if len(
        args) >= 1 else "https://bsky.app/profile/mathieuhourdin.bsky.social/post/3mfetst7zbc2z"
    max_depth = int(args[1]) if len(args) > 1 else DEFAULT_MAX_DEPTH
    window_days = int(args[2]) if len(args) > 2 else DEFAULT_WINDOW_DAYS

    run(
        target_url=target_url,
        max_depth=max_depth,
        window_days=window_days,
    )
