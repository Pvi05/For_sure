"""
synthesizer.py — Étape 9 de la pipeline TrustGraph
====================================================
Synthèse narrative du parcours de l'information à partir d'un
ProvenanceGraph (étape 6) déjà construit.

Extrait TOUS les chemins (source primaire → post racine) depuis le DiGraph
NetworkX, puis appelle Gemini pour produire une analyse structurée couvrant
l'ensemble du mouvement d'information — pas seulement une branche.

Place dans la pipeline :
    provenance_graph.py  →  synthesizer.py  →  [visualisation / export]

Usage minimal :
    from provenance_graph import ProvenanceGraph
    from synthesizer import synthesize

    graph = ProvenanceGraph()
    graph.build("https://bsky.app/profile/alice.bsky.social/post/3abc")
    result = synthesize(graph)
    print(result.narrative)
    result.save_json("synthesis.json")

Retourne un SynthesisResult contenant :
    original_fact    — le fait tel qu'exprimé par la source primaire
    final_version    — le fait tel qu'il apparaît dans le post racine
    distortions      — déformations step-by-step entre chaque paire de nœuds
    verdict          — évaluation globale de la véracité du post racine
    veracity_score   — score global 0-1 estimé par le LLM
    narrative        — paragraphe de synthèse lisible par un humain
    top_amplifiers   — handles ayant le plus contribué à la désinformation

Dépendances :
    pip install google-genai networkx

Variables d'environnement :
    GOOGLE_API_KEY    — clé API Gemini (obligatoire)
    SYNTHESIS_MODEL   — modèle Gemini à utiliser (défaut : gemini-2.5-flash)
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import networkx as nx

from pipeline.Provenance_graph_etape6 import ProvenanceGraph

from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

SYNTHESIS_MODEL = os.getenv("SYNTHESIS_MODEL", "gemini-2.5-flash")

_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2.0

# Sélection des chemins pour le prompt LLM
# Un chemin = un couple (source_primaire → racine)
# On limite le nombre de chemins envoyés à Gemini pour éviter le bruit et le
# gonflement du prompt. Les chemins écartés sont les moins fiables ET/OU
# redondants par rapport aux chemins déjà sélectionnés.
_MAX_PATHS_IN_PROMPT    = 4     # Nombre max de chemins envoyés à Gemini
_LOW_FIABILITY_THRESH   = 0.35  # En dessous : source peu fiable
_HIGH_VIRALITY_THRESH   = 0.55  # Au dessus  : source virale (risque de bruit)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Prompts
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """Tu es un expert en fact-checking et en analyse de désinformation sur les réseaux sociaux.

Ta tâche est d'analyser l'ensemble des chemins de propagation d'une information sur Bluesky \
(de chaque source primaire vers le post analysé) et de produire une synthèse rigoureuse et objective \
de la manière dont l'information a évolué, été déformée ou amplifiée au fil de sa propagation.

Si plusieurs sources primaires existent, tu dois les prendre TOUTES en compte pour évaluer \
la véracité globale du post analysé, en pondérant selon leur fiabilité.

Principes directeurs :
- Reste strictement factuel et neutre. N'invente rien d'absent des données fournies.
- Appuie-toi sur les scores d'engagement fournis pour identifier les vecteurs de propagation.
- Si l'information n'a PAS été déformée, dis-le explicitement.
- Identifie précisément QUI a introduit quelle déformation et COMMENT.
- Le champ "narrative" doit être compréhensible par un journaliste non-technique.
- Un nœud marqué "SOURCE DE CONFIANCE" doit être traité comme une référence fiable.
- Un nœud marqué "SOURCE PRIMAIRE" est l'origine ; ses claims sont la vérité de référence.
- Les scores de distorsion sur les arêtes (0 = fidèle, 1 = très déformé) sont calculés \
  objectivement par la pipeline — appuie-toi dessus pour étayer ton analyse.

Réponds UNIQUEMENT en JSON valide, sans texte autour, avec exactement ces clés :
{
  "original_fact": "<le fait tel qu'exprimé par la/les source(s) primaire(s), en une phrase>",
  "final_version": "<le fait tel qu'il apparaît dans le post analysé, en une phrase>",
  "distortions": [
    {
      "step": <entier, 1 = premier relais après la source>,
      "from_author": "<handle du post amont>",
      "to_author":   "<handle du post aval>",
      "what_changed": "<description précise de la déformation, max 100 mots>",
      "info_lost":    ["<information omise>"],
      "info_added":   ["<information ajoutée ou inventée>"],
      "tone_shift":   "<neutral|more_alarmist|more_partisan|minimizes|inverts>",
      "distortion_score": <float 0.0-1.0>
    }
  ],
  "verdict": "<évaluation globale en 1-2 phrases : le post est-il fidèle à la source ?>",
  "veracity_score": <float 0.0-1.0, véracité du post racine par rapport à la/les source(s)>,
  "narrative": "<paragraphe de 100-180 mots rédigé pour un lecteur non-technique>",
  "top_amplifiers": ["<handle du compte ayant le plus propagé une version déformée>"]
}

Règles strictes :
- "distortions" = [] si l'information n'a pas été déformée.
- "top_amplifiers" = [] si aucun compte n'a propagé de désinformation.
- Tous les floats doivent être dans [0.0, 1.0].
- Ne jamais inventer d'informations absentes de la chaîne fournie."""


_USER_TEMPLATE = """Voici l'ensemble des chemins de propagation d'une information sur Bluesky, \
depuis chaque source primaire identifiée jusqu'au post analysé.

{all_paths_text}

{graph_structure}

Analyse l'intégralité de ces chemins. Si plusieurs sources primaires existent, \
prends-les TOUTES en compte pour évaluer la véracité globale du post analysé. \
Produis la synthèse JSON demandée en couvrant le mouvement d'information \
dans sa totalité, pas seulement une branche."""


# ─────────────────────────────────────────────────────────────────────────────
# Modèles de données — Sortie
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Distortion:
    """Déformation de l'information entre deux nœuds consécutifs de la chaîne."""
    step:             int
    from_author:      str
    to_author:        str
    what_changed:     str
    info_lost:        list[str] = field(default_factory=list)
    info_added:       list[str] = field(default_factory=list)
    tone_shift:       str = "neutral"
    distortion_score: float = 0.0

    def to_dict(self) -> dict:
        return {
            "step":             self.step,
            "from_author":      self.from_author,
            "to_author":        self.to_author,
            "what_changed":     self.what_changed,
            "info_lost":        self.info_lost,
            "info_added":       self.info_added,
            "tone_shift":       self.tone_shift,
            "distortion_score": self.distortion_score,
        }


@dataclass
class SynthesisResult:
    """Résultat complet de l'étape 9."""
    original_fact:  str
    final_version:  str
    distortions:    list[Distortion] = field(default_factory=list)
    verdict:        str = ""
    veracity_score: float = 0.5
    narrative:      str = ""
    top_amplifiers: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "original_fact":  self.original_fact,
            "final_version":  self.final_version,
            "distortions":    [d.to_dict() for d in self.distortions],
            "verdict":        self.verdict,
            "veracity_score": self.veracity_score,
            "narrative":      self.narrative,
            "top_amplifiers": self.top_amplifiers,
        }

    def save_json(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
        print(f"💾 Synthèse sauvegardée : {path}")

    def print_summary(self) -> None:
        """Affiche un résumé lisible dans le terminal."""
        bar = "═" * 60
        score = self.veracity_score
        icon = "🟢" if score >= 0.75 else ("🟡" if score >= 0.45 else "🔴")

        print(f"\n{bar}")
        print("📋  SYNTHÈSE — PARCOURS DE L'INFORMATION")
        print(bar)
        print(f"\n{icon}  Score de véracité global : {score:.2f} / 1.00")
        print(f"\n📌  Fait original  : {self.original_fact}")
        print(f"📌  Version finale : {self.final_version}")

        if self.distortions:
            print(f"\n⚠️   Déformations ({len(self.distortions)}) :")
            for d in self.distortions:
                print(f"\n  [{d.step}] @{d.from_author} → @{d.to_author}"
                      f"  distortion={d.distortion_score:.2f}  tone={d.tone_shift}")
                print(f"      {d.what_changed}")
                if d.info_lost:
                    print(f"      Perdu  : {', '.join(d.info_lost)}")
                if d.info_added:
                    print(f"      Ajouté : {', '.join(d.info_added)}")
        else:
            print("\n✅  Aucune déformation significative détectée.")

        if self.top_amplifiers:
            print(f"\n🔊  Principaux amplificateurs : "
                  f"{', '.join('@' + h for h in self.top_amplifiers)}")

        print(f"\n💬  Verdict : {self.verdict}")
        print(f"\n📖  Narrative :\n    {self.narrative}")
        print(f"\n{bar}\n")


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
# Extraction de TOUS les chemins depuis le ProvenanceGraph
# ─────────────────────────────────────────────────────────────────────────────

def _extract_all_paths(graph) -> list[tuple[list[dict], list[dict]]]:
    """
    Extrait TOUS les chemins [source_primaire → post_racine] depuis le DiGraph
    du ProvenanceGraph, un chemin par couple (primaire, racine) distinct.

    Contrairement à l'ancienne approche (remontée d'une seule branche via
    G.predecessors), cette fonction utilise nx.shortest_path pour trouver
    chaque chemin existant dans le graphe, donnant au LLM une vue complète
    du mouvement d'information à travers toutes les branches.

    Returns:
        Liste de (chain_nodes, chain_edges) dans l'ordre [source → racine]
        pour chaque chemin distinct. Toujours non vide.

    Raises:
        ValueError : graphe vide.
    """
    G = graph.G
    if isinstance(G, (nx.MultiDiGraph, nx.MultiGraph)):
        G = nx.DiGraph(G)

    if G.number_of_nodes() == 0:
        raise ValueError("Le graphe est vide — impossible d'extraire des chemins.")

    # ── Racines (is_root=True ou depth minimal) ───────────────────────────
    roots = [nid for nid, d in G.nodes(data=True) if d.get("is_root")]
    if not roots:
        roots = [min(G.nodes(data=True), key=lambda x: x[1].get("depth", 0))[0]]
        logger.warning("Aucun nœud is_root=True — fallback sur depth minimal : %s", roots[0])

    # ── Sources primaires (is_primary=True ou sans prédécesseur) ─────────
    primaries = [nid for nid, d in G.nodes(data=True) if d.get("is_primary")]
    if not primaries:
        primaries = [nid for nid in G.nodes() if not list(G.predecessors(nid))]
    if not primaries:
        primaries = roots  # dernier recours

    # ── Extraction des chemins ────────────────────────────────────────────
    all_paths: list[tuple[list[dict], list[dict]]] = []
    seen: set[tuple] = set()

    for primary in primaries:
        for root in roots:
            try:
                path_nids = nx.shortest_path(G, source=primary, target=root)
            except nx.NetworkXNoPath:
                continue

            path_key = tuple(path_nids)
            if path_key in seen:
                continue
            seen.add(path_key)

            chain_nodes: list[dict] = []
            for nid in path_nids:
                data = dict(G.nodes[nid])
                data["uri"] = data.get("uri", nid)
                chain_nodes.append(data)

            chain_edges: list[dict] = []
            for i in range(len(path_nids) - 1):
                src, tgt = path_nids[i], path_nids[i + 1]
                if G.has_edge(src, tgt):
                    edge = dict(G.edges[src, tgt])
                else:
                    edge = {
                        "distortion_score": None,
                        "tone_shift":       "unknown",
                        "info_lost":        [],
                        "info_added":       [],
                        "is_fact_check":    False,
                    }
                    logger.warning("Arête manquante %s → %s dans le graphe.", src, tgt)
                edge["from_nid"] = src
                edge["to_nid"] = tgt
                chain_edges.append(edge)

            all_paths.append((chain_nodes, chain_edges))
            logger.info(
                "Chemin extrait : %d nœud(s)  [@%s → … → @%s]",
                len(chain_nodes),
                chain_nodes[0].get("author_handle", "?"),
                chain_nodes[-1].get("author_handle", "?"),
            )

    # ── Fallback : aucun chemin primaire→racine trouvé ────────────────────
    if not all_paths:
        root_nid = roots[0]
        data = dict(G.nodes[root_nid])
        data["uri"] = data.get("uri", root_nid)
        all_paths = [([data], [])]
        logger.warning("Aucun chemin primaire→racine — graphe réduit à la racine seule.")

    logger.info("Total : %d chemin(s) extrait(s)", len(all_paths))
    return all_paths


# ─────────────────────────────────────────────────────────────────────────────
# Sérialisation pour le prompt
# ─────────────────────────────────────────────────────────────────────────────

def _virality_score(node: dict) -> float:
    """Score de viralité normalisé [0, 1] via échelle log10 (plafond 10 000 interactions)."""
    interactions = node.get("likes", 0) + \
        node.get("reposts", 0) + node.get("replies", 0)
    if interactions <= 0:
        return 0.0
    return round(min(1.0, math.log10(interactions + 1) / math.log10(10_001)), 3)


def _serialize_nodes(chain_nodes: list[dict]) -> str:
    """Sérialise les nœuds d'un chemin en texte structuré pour Gemini."""
    lines: list[str] = []

    for i, node in enumerate(chain_nodes):
        if i == 0:
            role = "SOURCE PRIMAIRE"
        elif i == len(chain_nodes) - 1:
            role = "POST ANALYSÉ (racine)"
        else:
            role = f"RELAIS #{i}"

        flags: list[str] = []
        if node.get("is_trusted_source"):
            flags.append("SOURCE DE CONFIANCE")
        if node.get("is_primary"):
            flags.append("AUCUN ANTÉCÉDENT TROUVÉ")
        flag_str = "  [" + " | ".join(flags) + "]" if flags else ""

        lines.append(f"── Nœud {i + 1}/{len(chain_nodes)} : {role}{flag_str}")
        lines.append(f"   Auteur      : @{node.get('author_handle', '?')}")
        lines.append(f"   Date        : {node.get('date', '?')}")
        lines.append(
            f"   Engagement  : {node.get('likes', 0)} likes  "
            f"{node.get('reposts', 0)} reposts  "
            f"{node.get('replies', 0)} réponses  "
            f"(viralité normalisée : {_virality_score(node):.2f}/1.00)"
        )
        fiability = node.get("fiability_score")
        if fiability is not None:
            lines.append(f"   Fiabilité   : {fiability:.2f}/1.00")

        claims = node.get("claims", [])
        if claims:
            lines.append("   Affirmation(s) :")
            for claim in claims:
                lines.append(f"      • {claim}")

        entities = node.get("entities", [])
        if entities:
            lines.append(f"   Entités     : {', '.join(entities)}")

        lines.append(f"   Texte       : \"{node.get('text_preview', '')}\"")
        lines.append("")

    return "\n".join(lines)


def _serialize_edges(chain_nodes: list[dict], chain_edges: list[dict]) -> str:
    """Sérialise les arêtes (données étape 5) en texte structuré pour Gemini."""
    if not chain_edges:
        return "Aucune arête — post isolé.\n"

    lines: list[str] = []
    for i, edge in enumerate(chain_edges):
        src_handle = chain_nodes[i].get("author_handle", "?")
        tgt_handle = chain_nodes[i + 1].get("author_handle", "?")
        ds = edge.get("distortion_score")
        ds_str = f"{ds:.2f}" if ds is not None else "N/A (analyse échouée)"
        fc = "  ⚑ FACT-CHECK IDENTIFIÉ" if edge.get("is_fact_check") else ""

        lines.append(f"  Arête {i + 1} : @{src_handle} → @{tgt_handle}{fc}")
        lines.append(
            f"    distortion_score : {ds_str}  (0=fidèle, 1=très déformé)")
        lines.append(
            f"    tone_shift       : {edge.get('tone_shift', 'unknown')}")

        info_lost = edge.get("info_lost",  [])
        info_added = edge.get("info_added", [])
        if info_lost:
            lines.append(
                f"    info_lost        : {json.dumps(info_lost,  ensure_ascii=False)}")
        if info_added:
            lines.append(
                f"    info_added       : {json.dumps(info_added, ensure_ascii=False)}")
        lines.append("")

    return "\n".join(lines)


def _serialize_all_paths(all_paths: list[tuple[list[dict], list[dict]]]) -> str:
    """
    Sérialise l'ensemble des chemins de propagation pour le prompt Gemini.

    Si plusieurs chemins existent (plusieurs sources primaires ou plusieurs
    branches), chacun est clairement délimité avec son en-tête.
    """
    if not all_paths:
        return "Aucun chemin trouvé dans le graphe.\n"

    parts: list[str] = []
    for idx, (chain_nodes, chain_edges) in enumerate(all_paths, start=1):
        if len(all_paths) > 1:
            src = chain_nodes[0].get("author_handle", "?")
            tgt = chain_nodes[-1].get("author_handle", "?")
            parts.append(
                f"══════ CHEMIN {idx}/{len(all_paths)} : "
                f"@{src} → @{tgt}  ({len(chain_nodes)} nœud(s)) ══════\n"
            )
        parts.append(_serialize_nodes(chain_nodes))
        parts.append("DÉFORMATIONS SUR CE CHEMIN :")
        parts.append(_serialize_edges(chain_nodes, chain_edges))

    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Parsing de la réponse LLM
# ─────────────────────────────────────────────────────────────────────────────

def _parse_distortion(raw: dict, fallback_step: int) -> Distortion:
    try:
        score = max(0.0, min(1.0, float(raw.get("distortion_score", 0.0))))
    except (TypeError, ValueError):
        score = 0.0

    return Distortion(
        step=int(raw.get("step", fallback_step)),
        from_author=str(raw.get("from_author", "inconnu")),
        to_author=str(raw.get("to_author", "inconnu")),
        what_changed=str(raw.get("what_changed", "")),
        info_lost=[str(x) for x in raw.get("info_lost", []) if x],
        info_added=[str(x) for x in raw.get("info_added", []) if x],
        tone_shift=str(raw.get("tone_shift", "neutral")),
        distortion_score=score,
    )


def _parse_response(raw: str) -> Optional[SynthesisResult]:
    """
    Parse la réponse JSON de Gemini.
    Retourne None si le parsing est irrécupérable.
    """
    fence_match = re.match(
        r'^\s*```(?:json)?\s*\n?(.*?)\n?\s*```\s*$', raw, re.DOTALL)
    if fence_match:
        raw = fence_match.group(1)

    json_match = re.search(r'\{.*\}', raw, re.DOTALL)
    if not json_match:
        logger.warning("Gemini: aucun JSON trouvé dans : %s", raw[:300])
        return None

    try:
        data = json.loads(json_match.group())
    except json.JSONDecodeError as exc:
        logger.warning("Gemini: JSON invalide (%s) : %s", exc, raw[:300])
        return None

    distortions: list[Distortion] = []
    for i, d_raw in enumerate(data.get("distortions", []), start=1):
        try:
            distortions.append(_parse_distortion(d_raw, fallback_step=i))
        except Exception as exc:
            logger.warning("Distortion[%d] invalide (%s) — ignorée.", i, exc)

    try:
        veracity = max(0.0, min(1.0, float(data.get("veracity_score", 0.5))))
    except (TypeError, ValueError):
        veracity = 0.5

    return SynthesisResult(
        original_fact=str(data.get("original_fact", "")),
        final_version=str(data.get("final_version", "")),
        distortions=distortions,
        verdict=str(data.get("verdict", "")),
        veracity_score=veracity,
        narrative=str(data.get("narrative", "")),
        top_amplifiers=[str(h) for h in data.get("top_amplifiers", []) if h],
    )


def _fallback_result(all_paths: list[tuple[list[dict], list[dict]]]) -> SynthesisResult:
    """Résultat de secours quand Gemini échoue ou renvoie un JSON invalide."""
    if not all_paths:
        return SynthesisResult(
            original_fact="Indisponible",
            final_version="Indisponible",
            verdict="Analyse impossible — graphe vide.",
            veracity_score=0.5,
            narrative="L'analyse automatique n'a pas pu être effectuée.",
        )

    chain_nodes, _ = all_paths[0]
    source = chain_nodes[0] if chain_nodes else {}
    root   = chain_nodes[-1] if chain_nodes else {}

    source_claims = source.get("claims", [])
    root_claims   = root.get("claims", [])
    original = source_claims[0] if source_claims else source.get("text_preview", "")
    final    = root_claims[0]   if root_claims   else root.get("text_preview", "")

    return SynthesisResult(
        original_fact=original,
        final_version=final,
        verdict="Analyse automatique indisponible — résultat de secours.",
        veracity_score=0.5,
        narrative=(
            f"L'analyse narrative n'a pas pu être produite automatiquement. "
            f"Le graphe contient {len(all_paths)} chemin(s) de propagation. "
            f"Source primaire : @{source.get('author_handle', '?')}. "
            f"Post analysé : @{root.get('author_handle', '?')}."
        ),
        distortions=[],
        top_amplifiers=[],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Score de véracité structurel — données déjà présentes dans le graphe
# ─────────────────────────────────────────────────────────────────────────────

def _structural_veracity_from_graph(graph) -> float:
    """
    Score de véracité structurel calculé à partir des données déjà stockées
    dans le ProvenanceGraph — sans les recalculer.

    Composantes :
      best_source_quality : fiability_score maximal parmi toutes les sources
                            primaires. Ce score est déjà calculé par
                            Post_Heuristic_etape7.evaluate_node() lors de
                            la construction du graphe (étape 6) — inutile
                            de le reconstruire ici.
      global_distortion   : distorsion cumulée sur le chemin le plus distordu,
                            déjà calculée par ProvenanceGraph._global_distortion().

    Formule : best_source_quality × (1 − global_distortion)

    Returns:
        float in [0.0, 1.0].
    """
    G = graph.G
    if isinstance(G, (nx.MultiDiGraph, nx.MultiGraph)):
        G = nx.DiGraph(G)

    # Qualité des sources primaires — lecture directe du fiability_score stocké
    primary_nids = [nid for nid, d in G.nodes(data=True) if d.get("is_primary")]
    if not primary_nids:
        primary_nids = [nid for nid in G.nodes() if not list(G.predecessors(nid))]

    best_quality = max(
        (G.nodes[nid].get("fiability_score", 0.2) for nid in primary_nids),
        default=0.5,
    )

    # Distorsion globale — déjà calculée par le graphe, pas de doublon
    global_dist = graph._global_distortion()
    if global_dist is None:
        return round(min(1.0, best_quality), 3)

    return round(max(0.0, min(1.0, best_quality * (1.0 - global_dist))), 3)


# ─────────────────────────────────────────────────────────────────────────────
# Analyse structurelle du graphe de provenance complet
# ─────────────────────────────────────────────────────────────────────────────

def _analyze_graph_structure(graph) -> dict:
    """
    Calcule des métriques structurelles sur le graphe de provenance complet
    (pas seulement un chemin), afin de contextualiser la propagation.
    """
    G = graph.G
    n = G.number_of_nodes()
    e = G.number_of_edges()

    density = nx.density(G) if n > 1 else 0.0

    in_degrees = [d for _, d in G.in_degree()]
    avg_in_degree = round(sum(in_degrees) / len(in_degrees), 2) if in_degrees else 0.0
    max_in_degree = max(in_degrees) if in_degrees else 0

    try:
        avg_clustering = round(nx.average_clustering(G.to_undirected()), 3)
    except Exception:
        avg_clustering = 0.0

    n_components = nx.number_weakly_connected_components(G) if n > 0 else 1

    return {
        "n_nodes":        n,
        "n_edges":        e,
        "density":        round(density, 4),
        "avg_in_degree":  avg_in_degree,
        "max_in_degree":  max_in_degree,
        "avg_clustering": avg_clustering,
        "n_components":   n_components,
    }


def _serialize_graph_structure(metrics: dict) -> str:
    """Formate les métriques structurelles en phrase contextuelle pour le prompt LLM."""
    n     = metrics["n_nodes"]
    e     = metrics["n_edges"]
    d     = metrics["density"]
    max_in = metrics["max_in_degree"]
    clust = metrics["avg_clustering"]
    n_comp = metrics["n_components"]

    if d >= 0.5:
        density_interp = "très dense — amplification coordonnée probable, risque élevé de fake news"
    elif d >= 0.15:
        density_interp = "modérément dense — propagation ramifiée avec plusieurs relais croisés"
    else:
        density_interp = "peu dense — propagation majoritairement linéaire"

    if clust >= 0.5:
        clust_interp = "forte tendance aux chambres d'écho (information recirculant dans un groupe fermé)"
    elif clust >= 0.2:
        clust_interp = "regroupements partiels observés"
    else:
        clust_interp = "diffusion dispersée sans clustering significatif"

    if max_in >= 5:
        hub_interp = f"présence d'un hub d'amplification majeur (degré entrant max = {max_in})"
    elif max_in >= 2:
        hub_interp = f"quelques nœuds relais notables (degré entrant max = {max_in})"
    else:
        hub_interp = "aucun hub dominant identifié"

    return (
        f"ANALYSE STRUCTURELLE DU GRAPHE DE PROVENANCE ({n} nœud(s), {e} arête(s)) : "
        f"densité={d:.3f} ({density_interp}) ; "
        f"clustering moyen={clust:.3f} ({clust_interp}) ; "
        f"{hub_interp} ; "
        f"{n_comp} composante(s) connexe(s). "
        "Prends en compte ces caractéristiques structurelles dans ton évaluation : "
        "un graphe dense avec fort clustering et hubs actifs est structurellement "
        "propice à l'amplification de désinformation, indépendamment du contenu textuel."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sélection et ranking des chemins avant le prompt LLM
# ─────────────────────────────────────────────────────────────────────────────

def _path_priority_score(chain_nodes: list[dict], chain_edges: list[dict]) -> float:
    """
    Score de priorité d'un chemin pour sa sélection dans le prompt Gemini.

    Critères (toutes les valeurs viennent du graphe, sans recalcul) :
      - fiability_score de la source (60 %) — facteur dominant :
          une source fiable ancre l'analyse sur une vérité de référence.
      - distortion moyenne des arêtes (40 %) — les chemins très déformants
          sont précieux : ils montrent les étapes les plus problématiques.
      - pénalité "amplificateur de bruit" : si la source a une faible fiabilité
          ET une forte viralité, elle amplifie du bruit sans apporter de vérité.
          Ce profil est pénalisé pour qu'il n'écrase pas les chemins fiables.

    Returns:
        float — score composite, plus haut = chemin plus prioritaire.
    """
    source = chain_nodes[0] if chain_nodes else {}
    source_quality  = source.get("fiability_score",  0.2)
    source_virality = source.get("virality_score",   0.0)

    ds_values = [
        e.get("distortion_score")
        for e in chain_edges
        if e.get("distortion_score") is not None
    ]
    avg_distortion = sum(ds_values) / len(ds_values) if ds_values else 0.0

    # Pénalité amplificateur de bruit : fiabilité faible + viralité élevée
    is_noise_amplifier = (
        source_quality  < _LOW_FIABILITY_THRESH and
        source_virality > _HIGH_VIRALITY_THRESH
    )
    noise_penalty = 0.3 if is_noise_amplifier else 0.0

    return source_quality * 0.6 + avg_distortion * 0.4 - noise_penalty


def _select_paths(
    all_paths: list[tuple[list[dict], list[dict]]],
    max_paths: int = _MAX_PATHS_IN_PROMPT,
) -> list[tuple[list[dict], list[dict]]]:
    """
    Sélectionne les chemins les plus informatifs à envoyer à Gemini.

    Garanties :
      1. Toujours au moins 1 chemin retourné.
      2. Le chemin depuis la source la plus fiable est toujours inclus.
      3. Le chemin le plus distordu (le plus informatif sur les déformations)
         est toujours inclus, même s'il vient d'une source peu fiable.
      4. Les chemins restants sont sélectionnés par score décroissant.
      5. Les chemins depuis des sources peu fiables ET très virales
         (amplificateurs de bruit) sont placés en fin de classement.

    Args:
        all_paths : Tous les chemins extraits par _extract_all_paths().
        max_paths : Nombre maximum de chemins à retenir.

    Returns:
        Sous-liste triée par pertinence décroissante.
    """
    if len(all_paths) <= max_paths:
        return all_paths

    # Score chaque chemin
    scored = sorted(
        all_paths,
        key=lambda p: _path_priority_score(*p),
        reverse=True,
    )

    selected: list[tuple[list[dict], list[dict]]] = []

    # Garantie 1 : source la plus fiable
    best_reliable = max(
        all_paths,
        key=lambda p: p[0][0].get("fiability_score", 0.0) if p[0] else 0.0,
    )
    selected.append(best_reliable)

    # Garantie 2 : chemin le plus distordu (informatif sur les déformations)
    most_distorted = max(
        all_paths,
        key=lambda p: sum(e.get("distortion_score") or 0.0 for e in p[1]),
    )
    if most_distorted not in selected:
        selected.append(most_distorted)

    # Compléter jusqu'à max_paths par score décroissant
    for path in scored:
        if len(selected) >= max_paths:
            break
        if path not in selected:
            selected.append(path)

    dropped = len(all_paths) - len(selected)
    if dropped > 0:
        logger.info(
            "Étape 9 — %d/%d chemin(s) retenus pour le prompt "
            "(%d écarté(s) : faible fiabilité / bruit / redondance).",
            len(selected), len(all_paths), dropped,
        )

    return selected


# ─────────────────────────────────────────────────────────────────────────────
# Fonction principale — API publique
# ─────────────────────────────────────────────────────────────────────────────

def synthesize(graph, model: Optional[str] = None) -> SynthesisResult:
    """
    Étape 9 — Synthèse narrative du parcours de l'information.

    Prend un ProvenanceGraph déjà construit (après .build()) et appelle
    Gemini pour produire une analyse complète couvrant TOUS les chemins
    de propagation identifiés dans le graphe.

    Args:
        graph : Instance de ProvenanceGraph avec .G (nx.DiGraph) rempli.
        model : Identifiant du modèle Gemini. Si None, lit SYNTHESIS_MODEL
                depuis l'environnement au moment de l'appel.

    Returns:
        SynthesisResult avec l'analyse complète.

    Raises:
        ValueError              : graphe vide ou sans racine identifiable.
        EnvironmentError        : GOOGLE_API_KEY absente.
        genai_errors.ClientError: erreur client non récupérable (auth, quota).
    """
    if model is None:
        model = os.getenv("SYNTHESIS_MODEL", "gemini-2.5-flash")

    logger.info("Étape 9 — synthèse narrative via %s…", model)

    all_paths = _extract_all_paths(graph)

    # ── Cas trivial : nœud racine isolé (aucun chemin de propagation) ─────
    if len(all_paths) == 1 and len(all_paths[0][0]) == 1:
        logger.info("Étape 9 — nœud unique, synthèse triviale (pas d'appel LLM).")
        node = all_paths[0][0][0]
        claims = node.get("claims", [])
        fact = claims[0] if claims else node.get("text_preview", "")
        is_trusted = node.get("is_trusted_source", False)
        verdict_suffix = " (source de confiance)" if is_trusted else ""

        return SynthesisResult(
            original_fact=fact,
            final_version=fact,
            distortions=[],
            verdict=f"Ce post est la source primaire. Aucune propagation détectée{verdict_suffix}.",
            veracity_score=1.0,
            narrative=(
                f"Le post de @{node.get('author_handle', '?')} est la source originale "
                "de cette information. Aucun relais antérieur n'a été trouvé dans le graphe."
            ),
            top_amplifiers=[],
        )

    # ── Sélection des chemins les plus informatifs ────────────────────────
    # all_paths contient tous les chemins bruts ; selected_paths est le
    # sous-ensemble envoyé à Gemini (chemins les plus fiables/distordus,
    # amplificateurs de bruit écartés si des alternatives existent).
    selected_paths = _select_paths(all_paths)

    # ── Construction du prompt ────────────────────────────────────────────
    all_paths_text  = _serialize_all_paths(selected_paths)
    graph_structure = _serialize_graph_structure(_analyze_graph_structure(graph))
    prompt = _USER_TEMPLATE.format(
        all_paths_text=all_paths_text,
        graph_structure=graph_structure,
    )
    client = _get_client()

    logger.info(
        "Étape 9 — appel Gemini (%d/%d chemin(s) sélectionné(s), prompt ~%d chars)…",
        len(selected_paths), len(all_paths), len(prompt),
    )

    for attempt in range(_MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    system_instruction=_SYSTEM_PROMPT,
                    temperature=0.0,
                    max_output_tokens=8192,
                ),
            )
            raw = response.text.strip()
            result = _parse_response(raw)

            if result is None:
                logger.warning("Étape 9 — parsing échoué, retour du résultat de secours.")
                return _fallback_result(selected_paths)

            # ── Ancrage objectif du veracity_score ────────────────────────
            # Score structurel : utilise les données déjà dans le graphe
            # (fiability_score des nœuds + _global_distortion du graphe).
            # Score LLM        : évaluation sémantique de Gemini.
            # Blend 50/50      : combine rigueur objective et nuance sémantique.
            structural = _structural_veracity_from_graph(graph)
            blended = round(0.5 * structural + 0.5 * result.veracity_score, 3)
            logger.info(
                "Étape 9 — veracity : structurel=%.3f  LLM=%.3f  pondéré=%.3f",
                structural, result.veracity_score, blended,
            )
            result.veracity_score = blended

            logger.info(
                "Étape 9 — OK : veracity=%.2f, %d déformation(s), %d amplificateur(s)",
                result.veracity_score,
                len(result.distortions),
                len(result.top_amplifiers),
            )
            return result

        except genai_errors.ClientError:
            raise

        except genai_errors.ServerError as exc:
            if attempt == _MAX_RETRIES - 1:
                logger.warning(
                    "Étape 9 — %d tentatives échouées (%s), fallback.", _MAX_RETRIES, exc
                )
                return _fallback_result(selected_paths)
            delay = _RETRY_BASE_DELAY * (2 ** attempt)
            logger.warning(
                "Étape 9 — erreur transitoire (%s) — retry %d/%d dans %.1f s.",
                exc, attempt + 1, _MAX_RETRIES, delay,
            )
            time.sleep(delay)


# ─────────────────────────────────────────────────────────────────────────────
# Smoke test local
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not os.environ.get("GOOGLE_API_KEY"):
        print("GOOGLE_API_KEY absente — test Gemini réel ignoré.")
        sys.exit(0)

    import glob as _glob
    json_files = sorted(_glob.glob("provenance_*.json"))
    if not json_files:
        print("Aucun fichier provenance_*.json trouvé.")
        sys.exit(1)

    target = json_files[-1]
    print(f"Chargement : {target}")
    with open(target) as f:
        data = json.load(f)

    G = nx.node_link_graph(data, directed=True, edges="links")

    graph = ProvenanceGraph()
    graph.G = G

    print("\n[Gemini] appel réel en cours…")
    _result_real = synthesize(graph)
    _result_real.print_summary()
    print(json.dumps(_result_real.to_dict(), ensure_ascii=False, indent=2))
