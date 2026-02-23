"""
pipeline/analyzer.py — Étape 5
Information Provenance Comparator
Analyse la déformation d'une information entre deux dates (ti-1 → ti)
Utilise uniquement Gemini 2.5 Flash (Google).

Supporte le fetch automatique de posts Bluesky via l'API AT Protocol (sans auth).
"""

import json
import os
import re
import sys
import urllib.request
import urllib.parse
from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime

from google import genai
from google.genai import types as genai_types


# ─────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────

@dataclass
class InfoNode:
    """Représente une information à un instant t"""
    content: str            # Texte ou résumé du contenu
    date: str               # ISO 8601 : "2024-01-15"
    source_url: str         # URL de la source
    source_name: str        # Nom lisible de la source (ex: "Le Monde")
    raw_text: Optional[str] = None      # Texte brut complet si dispo
    platform: Optional[str] = None     # "bluesky", "twitter", "web", ...
    author_handle: Optional[str] = None  # @handle de l'auteur si applicable


@dataclass
class DeformationAnalysis:
    """Résultat complet de l'analyse de déformation entre ti-1 et ti"""
    # Métadonnées
    node_older: InfoNode    # ti-1
    node_newer: InfoNode    # ti
    analyzed_at: str
    model_used: str

    # Analyse LLM
    info_lost: list[str]            # Faits présents dans ti-1, absents de ti
    info_added: list[str]           # Éléments ajoutés dans ti absents de ti-1
    # 0.0 (identique) → 1.0 (complètement déformé)
    distortion_score: float
    # "neutral" | "more_alarmist" | "more_partisan" | "minimizes"
    tone_shift: str
    is_fact_check: bool             # Le post corrige-t-il explicitement une erreur ?
    # ["omission", "exagération", "changement_causal", ...]
    deformation_type: list[str]
    deformation_summary: str        # Explication narrative

    # Sources externes de ti-1
    external_sources: list[dict]    # [{"name": ..., "url": ..., "type": ...}]
    primary_source_hypothesis: str  # Quelle est probablement la source primaire ?

    # Score global
    fidelity_score: float           # 1 - distortion_score (pour le graphe)


# ─────────────────────────────────────────────
# Bluesky Fetcher — AT Protocol (pas d'auth requise)
# ─────────────────────────────────────────────

BSKY_PUBLIC_API = "https://public.api.bsky.app/xrpc"


def _parse_bsky_url(url: str) -> tuple[str, str]:
    """
    Parse une URL Bluesky et retourne (handle_or_did, rkey).

    Formats supportés :
      https://bsky.app/profile/alice.bsky.social/post/3lc4xyzabc
      https://bsky.app/profile/did:plc:xxxxx/post/3lc4xyzabc
    """
    pattern = r"bsky\.app/profile/([^/]+)/post/([^/?#]+)"
    m = re.search(pattern, url)
    if not m:
        raise ValueError(
            f"URL Bluesky invalide : {url}\n"
            "Format attendu : https://bsky.app/profile/<handle>/post/<rkey>"
        )
    return m.group(1), m.group(2)


def _resolve_handle_to_did(handle: str) -> str:
    """Résout un handle Bluesky en DID via l'API publique."""
    if handle.startswith("did:"):
        return handle
    params = urllib.parse.urlencode({"handle": handle})
    req = urllib.request.Request(
        f"{BSKY_PUBLIC_API}/com.atproto.identity.resolveHandle?{params}",
        headers={"Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    return data["did"]


def _fetch_bsky_thread(did: str, rkey: str) -> dict:
    """Fetch le thread AT Protocol via getPostThread."""
    uri = f"at://{did}/app.bsky.feed.post/{rkey}"
    params = urllib.parse.urlencode({"uri": uri, "depth": 0})
    req = urllib.request.Request(
        f"{BSKY_PUBLIC_API}/app.bsky.feed.getPostThread?{params}",
        headers={"Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def fetch_bluesky_post(url: str) -> InfoNode:
    """
    Fetche un post Bluesky depuis son URL et retourne un InfoNode.

    Args:
        url: URL complète du post Bluesky
             ex: https://bsky.app/profile/alice.bsky.social/post/3lc4xyzabc

    Returns:
        InfoNode prêt à l'emploi pour compare_information()
    """
    handle_or_did, rkey = _parse_bsky_url(url)

    print(f"  → Résolution de l'identité : {handle_or_did}")
    did = _resolve_handle_to_did(handle_or_did)

    print(f"  → Fetch du post (rkey={rkey})")
    thread_data = _fetch_bsky_thread(did, rkey)

    post = thread_data["thread"]["post"]
    record = post["record"]
    author = post["author"]

    # Texte principal
    text = record.get("text", "")

    # Liens embed (carte externe)
    embed = record.get("embed", {})
    if embed.get("$type") == "app.bsky.embed.external":
        ext = embed.get("external", {})
        title = ext.get("title", "")
        uri = ext.get("uri", "")
        if title or uri:
            text += f"\n[Lien : {title} — {uri}]"

    # Date
    created_at = record.get("createdAt", "")
    date_str = created_at[:10] if created_at else datetime.now().strftime(
        "%Y-%m-%d")

    # Auteur
    handle = author.get("handle", "")
    display_name = author.get("displayName") or handle or "inconnu"
    source_name = f"Bluesky @{handle}" if handle else f"Bluesky {display_name}"

    return InfoNode(
        content=text,
        date=date_str,
        source_url=url,
        source_name=source_name,
        raw_text=json.dumps(record, ensure_ascii=False),
        platform="bluesky",
        author_handle=handle,
    )


def fetch_bluesky_posts(url_older: str, url_newer: str) -> tuple[InfoNode, InfoNode]:
    """
    Fetche deux posts Bluesky et les retourne triés chronologiquement (older, newer).

    Args:
        url_older: URL du post supposé plus ancien (ti-1)
        url_newer: URL du post supposé plus récent (ti)

    Returns:
        Tuple (InfoNode_ti_minus_1, InfoNode_ti)
    """
    print("📡 Fetch du post ti-1...")
    node_a = fetch_bluesky_post(url_older)
    print(f"   ✓ {node_a.source_name} — {node_a.date}")

    print("📡 Fetch du post ti...")
    node_b = fetch_bluesky_post(url_newer)
    print(f"   ✓ {node_b.source_name} — {node_b.date}\n")

    # Tri chronologique automatique
    if node_a.date <= node_b.date:
        return node_a, node_b
    else:
        print("⚠️  Dates inversées — réordonnancement automatique.\n")
        return node_b, node_a


# ─────────────────────────────────────────────
# Prompt Engineering
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """Tu es un expert en fact-checking et en analyse de la provenance de l'information.
Ta mission : comparer deux versions d'une même information à deux instants différents
et produire une analyse structurée et précise.

Réponds UNIQUEMENT en JSON valide, sans markdown, sans commentaires."""


def build_comparison_prompt(older: InfoNode, newer: InfoNode) -> str:
    platform_older = f" [{older.platform}]" if older.platform else ""
    platform_newer = f" [{newer.platform}]" if newer.platform else ""
    return f"""
Compare ces deux versions d'une information et analyse leur déformation.

═══════════════════════════════════════
INFORMATION ANCIENNE (ti-1){platform_older}
Date : {older.date}
Source : {older.source_name} ({older.source_url})
Contenu :
{older.content}
═══════════════════════════════════════
INFORMATION RÉCENTE (ti){platform_newer}
Date : {newer.date}
Source : {newer.source_name} ({newer.source_url})
Contenu :
{newer.content}
═══════════════════════════════════════

Produis un JSON avec EXACTEMENT cette structure :

{{
  "info_lost": [
    "Fait ou nuance présent dans ti-1 mais absent de ti",
    "..."
  ],
  "info_added": [
    "Élément présent dans ti mais absent de ti-1",
    "..."
  ],
  "distortion_score": 0.0,
  "tone_shift": "neutral | more_alarmist | more_partisan | minimizes",
  "is_fact_check": false,
  "deformation_types": [
    "omission_de_contexte | exagération | minimisation | changement_causal | erreur_factuelle | sensationnalisme | changement_de_sujet"
  ],
  "deformation_summary": "Explication narrative concise de la déformation globale (2-4 phrases)",
  "external_sources": [
    {{
      "name": "Nom de la source citée ou implicite dans ti-1",
      "url": "URL si mentionnée, sinon null",
      "type": "étude_scientifique | communiqué_officiel | agence_presse | institution | autre",
      "confidence": 0.0
    }}
  ],
  "primary_source_hypothesis": "Description de ce qui semble être la source primaire de cette information",
  "fidelity_score": 0.0
}}

Règles de scoring :
- distortion_score : 0.0 = aucune déformation, 1.0 = information totalement méconnaissable
- fidelity_score = 1.0 - distortion_score
- tone_shift : choisir exactement une valeur parmi "neutral", "more_alarmist", "more_partisan", "minimizes"
- is_fact_check : true si ti corrige explicitement une erreur factuelle de ti-1, false sinon
- external_sources.confidence : probabilité que cette source soit réelle (0-1)
"""


# ─────────────────────────────────────────────
# Gemini Backend
# ─────────────────────────────────────────────

ANALYZER_MODEL = os.getenv("ANALYZER_MODEL", "gemini-2.5-flash")


def _get_gemini_client() -> genai.Client:
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GOOGLE_API_KEY non définie. "
            "Exporter la variable avant de lancer le script."
        )
    return genai.Client(api_key=api_key)


def compare_with_gemini(
    older: InfoNode,
    newer: InfoNode,
    model: str = ANALYZER_MODEL,
) -> DeformationAnalysis:
    """Analyse la déformation entre deux InfoNodes via Gemini 2.5 Flash."""
    client = _get_gemini_client()
    prompt = build_comparison_prompt(older, newer)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=genai_types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=0.0,
            max_output_tokens=4096,
            thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
        ),
    )
    raw = (response.text or "").strip()

    # Nettoyer éventuels backticks markdown
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    # Extraction robuste du JSON
    json_match = re.search(r'\{.*\}', raw, re.DOTALL)
    if json_match:
        raw = json_match.group()

    data = json.loads(raw)

    return DeformationAnalysis(
        node_older=older,
        node_newer=newer,
        analyzed_at=datetime.now().isoformat(),
        model_used=f"gemini/{model}",
        info_lost=data.get("info_lost", []),
        info_added=data.get("info_added", []),
        distortion_score=float(data.get("distortion_score", 0.0)),
        tone_shift=data.get("tone_shift", "neutral"),
        is_fact_check=bool(data.get("is_fact_check", False)),
        deformation_type=data.get("deformation_types", []),
        deformation_summary=data.get("deformation_summary", ""),
        external_sources=data.get("external_sources", []),
        primary_source_hypothesis=data.get("primary_source_hypothesis", ""),
        fidelity_score=float(data.get("fidelity_score", 1.0)),
    )


# ─────────────────────────────────────────────
# Interface principale
# ─────────────────────────────────────────────

def compare_information(
    older: InfoNode,
    newer: InfoNode,
    model: Optional[str] = None,
    # Paramètre 'backend' conservé pour compatibilité rétrograde (ignoré)
    backend: str = "gemini",
) -> DeformationAnalysis:
    """
    Compare deux InfoNodes et retourne l'analyse de déformation via Gemini 2.5 Flash.

    Usage:
        result = compare_information(node_ti_minus_1, node_ti)
        print(result.distortion_score)
        print(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    """
    return compare_with_gemini(older, newer, model or ANALYZER_MODEL)


def compare_bluesky_posts(
    url_older: str,
    url_newer: str,
) -> DeformationAnalysis:
    """
    Raccourci tout-en-un : fetch deux posts Bluesky + analyse de déformation.

    Args:
        url_older: URL du post Bluesky ti-1
        url_newer: URL du post Bluesky ti

    Returns:
        DeformationAnalysis complet

    Usage:
        result = compare_bluesky_posts(
            "https://bsky.app/profile/alice.bsky.social/post/abc123",
            "https://bsky.app/profile/bob.bsky.social/post/xyz789",
        )
    """
    older, newer = fetch_bluesky_posts(url_older, url_newer)
    print("🤖 Analyse avec Gemini 2.5 Flash...\n")
    return compare_with_gemini(older, newer)


# ─────────────────────────────────────────────
# Graph Node Builder
# ─────────────────────────────────────────────

def to_graph_edge(analysis: DeformationAnalysis) -> dict:
    """
    Convertit une DeformationAnalysis en arête pour le graphe de provenance.
    Format compatible avec NetworkX, Neo4j, ou D3.js.
    """
    return {
        "source": {
            "id": f"{analysis.node_older.source_name}_{analysis.node_older.date}",
            "label": analysis.node_older.source_name,
            "date": analysis.node_older.date,
            "url": analysis.node_older.source_url,
            "platform": analysis.node_older.platform,
        },
        "target": {
            "id": f"{analysis.node_newer.source_name}_{analysis.node_newer.date}",
            "label": analysis.node_newer.source_name,
            "date": analysis.node_newer.date,
            "url": analysis.node_newer.source_url,
            "platform": analysis.node_newer.platform,
        },
        "edge": {
            "fidelity_score": analysis.fidelity_score,
            "distortion_score": analysis.distortion_score,
            "tone_shift": analysis.tone_shift,
            "is_fact_check": analysis.is_fact_check,
            "info_lost": analysis.info_lost,
            "info_added": analysis.info_added,
            "deformation_types": analysis.deformation_type,
            "weight": 1 - analysis.distortion_score,
        }
    }


def _print_result(result: DeformationAnalysis) -> None:
    """Affichage formaté d'un résultat d'analyse."""
    print(f"{'─'*50}")
    print(f"📊 Distortion score    : {result.distortion_score:.2f}")
    print(f"✅ Score de fidélité   : {result.fidelity_score:.2f}")
    print(f"🎭 Tone shift          : {result.tone_shift}")
    print(
        f"🔍 Fact-check          : {'Oui' if result.is_fact_check else 'Non'}")
    print(f"🔴 Types               : {', '.join(result.deformation_type)}")
    print(f"\n📝 Résumé :\n{result.deformation_summary}")
    print(f"\n❌ info_lost :")
    for item in result.info_lost:
        print(f"  - {item}")
    print(f"\n➕ info_added :")
    for item in result.info_added:
        print(f"  - {item}")
    print(f"\n🔗 Sources externes identifiées :")
    for src in result.external_sources:
        print(
            f"  - {src['name']} ({src['type']}) — confiance: {src['confidence']:.0%}")
    print(
        f"\n🌱 Hypothèse source primaire :\n{result.primary_source_hypothesis}")
    edge = to_graph_edge(result)
    print(
        f"\n📈 Arête graphe :\n{json.dumps(edge, indent=2, ensure_ascii=False)}")


# ─────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # ── Mode 1 : deux URLs Bluesky en arguments ───────────────────────────
    # python analyzer_etape5.py <url_old> <url_new>
    if len(sys.argv) >= 3:
        url_old = sys.argv[1]
        url_new = sys.argv[2]

        print("🔍 Comparaison Bluesky — Gemini 2.5 Flash\n")
        result = compare_bluesky_posts(url_old, url_new)
        _print_result(result)

    # ── Mode 2 : démo statique ────────────────────────────────────────────
    else:
        print("ℹ️  Aucune URL fournie — démo statique\n")
        print("Usage réel :")
        print(
            "  python info_comparator.py <url_bsky_old> <url_bsky_new> [claude|gemini]\n")

        original = InfoNode(
            content="""Une étude publiée dans Nature Medicine portant sur 12 000 participants
sur 5 ans montre que la consommation de café (2-3 tasses/jour) est associée
à une réduction de 12% du risque de diabète de type 2, après ajustement
pour l'âge, le poids et l'activité physique. Les auteurs précisent que
la corrélation ne prouve pas la causalité.""",
            date="2024-01-10",
            source_url="https://bsky.app/profile/sciencejournal.bsky.social/post/example1",
            source_name="Bluesky @sciencejournal",
            platform="bluesky",
            author_handle="sciencejournal.bsky.social",
        )

        viral = InfoNode(
            content="🚨 Le café GUÉRIT le diabète ! Une étude prouve que 3 tasses/jour font disparaître le diabète de type 2. Partagez avant censure ☕🔥",
            date="2024-01-14",
            source_url="https://bsky.app/profile/healthbro.bsky.social/post/example2",
            source_name="Bluesky @healthbro",
            platform="bluesky",
            author_handle="healthbro.bsky.social",
        )

        print("🔍 Analyse avec Gemini 2.5 Flash (démo statique)...\n")
        result = compare_information(original, viral)
        _print_result(result)
