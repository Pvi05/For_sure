"""
provenance_graph.py — Étape 6
==============================
Construit le graphe de provenance d'un post Bluesky en s'appuyant
sur la pipeline existante (étapes 1→5).

Dépendances attendues (étapes 1-5 déjà implémentées) :
    from pipeline import (
        fetch_post,           # Étape 1 : BlueskyPost depuis une URI/URL
        extract_keywords,     # Étape 2 : { keywords, entities, claims }
        search_prior_posts,   # Étape 3 : liste de BlueskyPost candidats
        validate_candidates,  # Étape 4 : filtre sémantique → [(post, score)]
        analyze_branch,       # Étape 5 : BranchAnalysis entre deux posts
    )

Dépendances système :
    pip install networkx
"""

import json
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional
import networkx as nx


# ─────────────────────────────────────────────
# Imports de la pipeline (étapes 1-5)
# ─────────────────────────────────────────────
# Ces modules sont supposés exister. Leurs signatures attendues :
#
#   fetch_post(uri: str) -> BlueskyPost
#       .uri, .text, .date (ISO), .author_handle, .author_did
#       .likes, .reposts, .replies
#
#   extract_keywords(post: BlueskyPost) -> dict
#       { "keywords": [...], "entities": [...], "claims": [...] }
#
#   search_prior_posts(keywords: dict, before_date: str, window_days=7) -> list[BlueskyPost]
#       Retourne les posts candidats triés par engagement, filtrés (likes+reposts >= 5)
#
#   validate_candidates(ref_post: BlueskyPost, candidates: list[BlueskyPost]) -> list[tuple[BlueskyPost, float]]
#       Retourne les paires (post, similarity_score) avec score >= 0.65 uniquement
#
#   analyze_branch(prior: BlueskyPost, later: BlueskyPost) -> BranchAnalysis
#       .info_lost, .info_added, .distortion_score (0-1)
#       .tone_shift ("neutral"|"more_alarmist"|"more_partisan"|"minimizes")
#       .is_fact_check (bool)

import os as _os
import sys as _sys
_pipeline_dir = _os.path.dirname(_os.path.abspath(__file__))
if _pipeline_dir not in _sys.path:
    _sys.path.insert(0, _pipeline_dir)

try:
    from fetcher import fetch_post
    from keywords_etape2 import extract_keywords
    from searcher_etape3 import search_prior_posts, NO_PARENT_FOUND
    # from searcher_etape3_likes import search_prior_posts, NO_PARENT_FOUND
    from validate_candidates_etape4 import validate_candidates
    from analyzer_etape5 import compare_information, InfoNode
    PIPELINE_AVAILABLE = True
except ImportError as e:
    PIPELINE_AVAILABLE = False
    # ... stubs ...

try:
    from Post_Heuristic_etape7 import evaluate_node
except ImportError:
    def evaluate_node(post): return (0.5, 0.0)


# ─────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────

DEFAULT_MAX_DEPTH = 4
DEFAULT_WINDOW_DAYS = 2
# Seuil étape 4 (rappel : déjà filtré dans validate_candidates)
MIN_SIMILARITY = 0.4
MAX_PARENTS_PER_NODE = 3     # Nb max d'ancêtres directs explorés par nœud


FIABILITY_STOP = 0.85   # Arrêt si source suffisamment fiable
MIN_VIRALITY_TO_EXPAND = 0.4      # Ne pas récurser sur des posts trop peu viraux
MAX_TOTAL_NODES = 50     # Coupe-circuit global sur la taille du graphe


# ─────────────────────────────────────────────
# Identifiant de nœud
# ─────────────────────────────────────────────

def _node_id(post) -> str:
    """URI AT Protocol comme identifiant stable du nœud."""
    return getattr(post, "uri", None) or f"{post.author_handle}__{post.date}"


# ─────────────────────────────────────────────
# ProvenanceGraph
# ─────────────────────────────────────────────

class ProvenanceGraph:
    """
    Graphe de provenance orienté : antérieur → postérieur.

    Chaque nœud  = un BlueskyPost avec ses métadonnées + résultat étape 2.
    Chaque arête = un BranchAnalysis (étape 5) entre deux posts liés sémantiquement.

    Usage minimal :
        graph = ProvenanceGraph()
        graph.build("at://did:plc:xxx/app.bsky.feed.post/yyy")
        graph.save_json("output.json")
        graph.print_summary()
    """

    def __init__(
        self,
        max_depth: int = DEFAULT_MAX_DEPTH,
        window_days: int = DEFAULT_WINDOW_DAYS,
    ):
        self.G = nx.DiGraph()
        self.max_depth = max_depth
        self.window_days = window_days
        self._visited: set[str] = set()   # Anti-cycle
        self.built_at = datetime.now().isoformat()

    # ─────────────────────────────────────────
    # Gestion des nœuds
    # ─────────────────────────────────────────

    def _register_node(self, post, depth: int, is_root: bool = False) -> str:
        """Ajoute un post comme nœud dans le graphe (idempotent)."""
        nid = _node_id(post)
        if not self.G.has_node(nid):
            fiability_score, virality_score = evaluate_node(post)
            self.G.add_node(nid,
                            # Identité
                            uri=getattr(post, "uri", ""),
                            author_handle=getattr(post, "author_handle", ""),
                            author_did=getattr(post, "author_did", ""),
                            date=getattr(post, "date", ""),
                            # Contenu
                            text_preview=post.text[:140] +
                            "…" if len(post.text) > 140 else post.text,
                            # Engagement
                            likes=getattr(post, "likes", 0),
                            reposts=getattr(post, "reposts", 0),
                            replies=getattr(post, "replies", 0),
                            # Méta-graphe
                            depth=depth,
                            is_root=is_root,
                            is_primary=False,
                            fiability_score=fiability_score,
                            virality_score=virality_score,
                            keywords=[],
                            )
        return nid

    def _set_keywords(self, nid: str, kw_data: dict) -> None:
        """Stocke le résultat de l'étape 2 sur le nœud."""
        self.G.nodes[nid]["keywords"] = kw_data.get("keywords", [])
        self.G.nodes[nid]["entities"] = kw_data.get("entities", [])
        self.G.nodes[nid]["claims"] = kw_data.get("claims", [])

    def _mark_primary(self, nid: str) -> None:
        self.G.nodes[nid]["is_primary"] = True

    # ─────────────────────────────────────────
    # Gestion des arêtes
    # ─────────────────────────────────────────

    def _register_edge(self, prior_nid: str, later_nid: str, branch, similarity_score: float = 0.0, is_fact_check=False) -> None:
        """
        Ajoute une arête dirigée prior → later avec les données de l'étape 5.
        branch : DeformationAnalysis (issu de analyzer.py)
        similarity_score : score sémantique de l'étape 4 (ValidationResult)
        """
        self.G.add_edge(prior_nid, later_nid,
                        # Étape 4
                        similarity_score=round(similarity_score, 4),
                        # Étape 5
                        info_lost=getattr(branch, "info_lost", []),
                        info_added=getattr(branch, "info_added", []),
                        distortion_score=getattr(
                            branch, "distortion_score", None),
                        tone_shift=getattr(branch, "tone_shift", "unknown"),
                        is_fact_check=False,
                        analyzed_at=datetime.now().isoformat(),
                        )

    # ─────────────────────────────────────────────
    # Constantes de pruning
    # ─────────────────────────────────────────────

    def _expand(self, post, depth: int) -> None:
        nid = _node_id(post)
        indent = "  " * depth

        # ── Garde-fous ────────────────────────────────────────────────────────
        if nid in self._visited:
            print(f"{indent}↩  Déjà visité : {nid[:60]}")
            return
        self._visited.add(nid)

        if depth >= self.max_depth:
            print(f"{indent}⛔ Profondeur max ({self.max_depth}) atteinte")
            self._mark_primary(nid)
            return

        # ── Coupe-circuit global ──────────────────────────────────────────────
        if self.G.number_of_nodes() >= MAX_TOTAL_NODES:
            print(
                f"{indent}⛔ Limite de {MAX_TOTAL_NODES} nœuds atteinte — arrêt global")
            self._mark_primary(nid)
            return

        # ── Arrêt sur fiabilité élevée ────────────────────────────────────────
        node_data = self.G.nodes[nid]
        fiability = node_data.get("fiability_score", 0.0)
        if fiability >= FIABILITY_STOP:
            print(
                f"{indent}🏛️  Fiabilité élevée ({fiability:.2f} ≥ {FIABILITY_STOP}) → source primaire")
            self._mark_primary(nid)
            return

        # ── Étape 2 : extraction des mots-clés ───────────────────────────────
        print(f"{indent}🔑 [Étape 2] Extraction des keywords…")
        try:
            kw_data = extract_keywords(post.text).to_dict()
            self._set_keywords(nid, kw_data)
            print(f"{indent}   → {len(kw_data.get('keywords', []))} keywords, "
                  f"{len(kw_data.get('claims', []))} claims")
        except Exception as e:
            print(f"{indent}   ⚠️  Étape 2 échouée : {e}")
            self._mark_primary(nid)
            return

        # ── Étape 3 : recherche des posts antérieurs ─────────────────────────
        print(
            f"{indent}🔍 [Étape 3] Recherche de posts antérieurs (fenêtre -{self.window_days}j)…")
        try:
            candidates = search_prior_posts(
                post=post, keywords=kw_data, window_days=self.window_days)
            print(f"{indent}   → {len(candidates)} candidat(s) bruts")
        except Exception as e:
            print(f"{indent}   ⚠️  Étape 3 échouée : {e}")
            self._mark_primary(nid)
            return

        if not candidates or (len(candidates) == 1 and candidates[0].uri == "unknown"):
            print(f"{indent}   ✅ Aucun antérieur trouvé → source primaire")
            self._mark_primary(nid)
            return

        # ── Étape 4 : validation sémantique ──────────────────────────────────
        print(f"{indent}🧠 [Étape 4] Validation sémantique des candidats…")
        try:
            validated = validate_candidates(post, candidates)
            print(f"{indent}   → {len(validated)} candidat(s) validé(s)")
        except Exception as e:
            print(f"{indent}   ⚠️  Étape 4 échouée : {e}")
            self._mark_primary(nid)
            return

        if not validated:
            print(f"{indent}   ✅ Aucun candidat validé → source primaire")
            self._mark_primary(nid)
            return

        # ── Sélection intelligente des voisins à visiter ─────────────────────
        # 1. Enregistre tous les nœuds validés pour avoir leur virality_score
        for vr in validated:
            self._register_node(vr.post, depth=depth + 1)

        # 2. Trie par virality_score décroissant (les plus viraux en premier)
        validated.sort(
            key=lambda vr: self.G.nodes[_node_id(
                vr.post)].get("virality_score", 0.0),
            reverse=True,
        )

        # 3. Limite le fan-in
        validated = validated[:MAX_PARENTS_PER_NODE]
        print(
            f"{indent}   → {len(validated)} candidat(s) retenus après tri virality + fan-in")

        # ── Étape 5 + récursion ───────────────────────────────────────────────
        for vr in validated:
            prior_post, similarity_score = vr.post, vr.similarity_score
            prior_nid = _node_id(prior_post)

            print(f"{indent}🔗 [Étape 5] Analyse de branche :")
            print(f"{indent}   @{prior_post.author_handle} → @{post.author_handle}"
                  f"  [similarité: {similarity_score:.2f}]")

            try:
                def _to_infonode(p) -> InfoNode:
                    return InfoNode(
                        content=getattr(p, "text", ""),
                        date=getattr(p, "date", "")[:10],
                        source_url=getattr(p, "uri", ""),
                        source_name=f"Bluesky @{getattr(p, 'author_handle', '')}",
                        platform="bluesky",
                        author_handle=getattr(p, "author_handle", ""),
                    )

                branch = compare_information(
                    older=_to_infonode(prior_post),
                    newer=_to_infonode(post),
                    backend="gemini",
                )
                self._register_edge(prior_nid, nid, branch, similarity_score)
                print(f"{indent}   distortion={branch.distortion_score:.2f}"
                      f"  tone={branch.tone_shift}"
                      f"  fact_check={branch.is_fact_check}")

                # ── Pruning post-étape 5 ──────────────────────────────────────
                # Ne pas récurser sur un fact-check (branche terminée sémantiquement)
                if branch.is_fact_check:
                    print(f"{indent}   ✅ Fact-check détecté → arrêt de la branche")
                    self.G.nodes[prior_nid]["is_primary"] = True
                    continue

            except Exception as e:
                print(
                    f"{indent}   ⚠️  Étape 5 échouée : {e} — arête structurelle créée")
                self.G.add_edge(prior_nid, nid,
                                info_lost=[], info_added=[], distortion_score=None,
                                tone_shift="unknown", is_fact_check=False, analyzed_at=None)

            # ── Pruning sur viralité minimale ─────────────────────────────────
            prior_virality = self.G.nodes[prior_nid].get("virality_score", 0.0)
            if prior_virality < MIN_VIRALITY_TO_EXPAND:
                print(
                    f"{indent}   📉 Viralité trop faible ({prior_virality:.2f} < {MIN_VIRALITY_TO_EXPAND}) → pas de récursion")
                continue

            # ── Récursion ─────────────────────────────────────────────────────
            print(f"{indent}↙  Récursion → profondeur {depth + 1}")
            self._expand(prior_post, depth=depth + 1)

    # ─────────────────────────────────────────
    # API publique
    # ─────────────────────────────────────────

    def build(self, root_uri: str) -> "ProvenanceGraph":
        """
        Point d'entrée principal. Lance la pipeline depuis un URI Bluesky.

        Args:
            root_uri: URI AT Protocol ou URL Bluesky du post racine
                      ex: "at://did:plc:xxx/app.bsky.feed.post/yyy"
                      ex: "https://bsky.app/profile/alice.bsky.social/post/abc"

        Returns:
            self (pour chaining)
        """
        print(f"\n🕸️  Démarrage de la construction du graphe")
        print(f"   URI      : {root_uri}")
        print(f"   Max depth: {self.max_depth}")
        print(f"   Fenêtre  : -{self.window_days} jours\n")

        # ── Étape 1 ───────────────────────────────────────────────────────
        print("📡 [Étape 1] Fetch du post racine…")
        root_post = fetch_post(root_uri)
        print(f"   ✓ @{root_post.author_handle} — {root_post.date}")
        print(f"   \"{root_post.text[:80]}…\"\n")

        root_nid = self._register_node(root_post, depth=0, is_root=True)
        self._expand(root_post, depth=0)

        n = self.G.number_of_nodes()
        e = self.G.number_of_edges()
        print(f"\n✅ Graphe terminé : {n} nœud(s), {e} arête(s)")
        return self

    # ─────────────────────────────────────────
    # Métriques globales
    # ─────────────────────────────────────────

    def _global_distortion(self) -> Optional[float]:
        """
        Score de distorsion cumulée sur le chemin le plus long
        (source primaire → racine).
        Formule : 1 - produit des (1 - distortion_score) sur le chemin.
        """
        # Trouve les nœuds primaires
        primaries = [n for n, d in self.G.nodes(
            data=True) if d.get("is_primary")]
        roots = [n for n, d in self.G.nodes(data=True) if d.get("is_root")]

        if not primaries or not roots:
            return None

        best = None
        for primary in primaries:
            for root in roots:
                try:
                    path = nx.shortest_path(
                        self.G, source=primary, target=root)
                    fidelity = 1.0
                    for i in range(len(path) - 1):
                        edge_data = self.G.edges[path[i], path[i+1]]
                        s = edge_data.get("distortion_score")
                        if s is not None:
                            fidelity *= (1 - s)
                    distortion = round(1 - fidelity, 3)
                    if best is None or distortion > best:
                        best = distortion
                except nx.NetworkXNoPath:
                    continue
        return best

    # ─────────────────────────────────────────
    # Export JSON (compatible D3.js)
    # ─────────────────────────────────────────

    def to_json(self) -> dict:
        nodes = [{"id": nid, **data} for nid, data in self.G.nodes(data=True)]
        links = [{"source": s, "target": t, **data}
                 for s, t, data in self.G.edges(data=True)]
        primaries = [nid for nid, d in self.G.nodes(
            data=True) if d.get("is_primary")]

        return {
            "meta": {
                "built_at": self.built_at,
                "node_count": self.G.number_of_nodes(),
                "edge_count": self.G.number_of_edges(),
                "max_depth": self.max_depth,
                "global_distortion": self._global_distortion(),
                "primary_source_ids": primaries,
            },
            "nodes": nodes,
            "links": links,
        }

    def save_json(self, path: str) -> None:
        data = self.to_json()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        print(f"💾 JSON sauvegardé : {path}")

    def save_graphml(self, path: str) -> None:
        """Export GraphML compatible Gephi / Cytoscape."""
        # NetworkX ne supporte pas les listes en attributs GraphML → on sérialise en JSON string
        G_export = self.G.copy()
        for _, _, data in G_export.edges(data=True):
            for k, v in data.items():
                if isinstance(v, list):
                    data[k] = json.dumps(v, ensure_ascii=False)
        for _, data in G_export.nodes(data=True):
            for k, v in data.items():
                if isinstance(v, list):
                    data[k] = json.dumps(v, ensure_ascii=False)
        nx.write_graphml(G_export, path)
        print(f"💾 GraphML sauvegardé : {path}")

    # ─────────────────────────────────────────
    # Résumé console
    # ─────────────────────────────────────────

    def print_summary(self) -> None:
        meta = self.to_json()["meta"]
        print("\n" + "═"*55)
        print("📊  RÉSUMÉ — GRAPHE DE PROVENANCE")
        print("═"*55)
        print(f"  Nœuds              : {meta['node_count']}")
        print(f"  Arêtes             : {meta['edge_count']}")
        distortion = meta['global_distortion']
        print(
            f"  Distorsion globale : {f'{distortion:.2f}' if distortion is not None else 'N/A'}")
        print(f"  Construit le       : {meta['built_at']}")

        print(f"\n  Sources primaires ({len(meta['primary_source_ids'])}) :")
        for pid in meta["primary_source_ids"]:
            d = self.G.nodes[pid]
            print(
                f"    ✅ @{d.get('author_handle', '?')}  {d.get('date', '?')}")
            print(f"       {d.get('text_preview', '')[:80]}")

        print(f"\n  Arêtes de déformation :")
        for src, tgt, data in self.G.edges(data=True):
            s_handle = self.G.nodes[src].get("author_handle", src[:20])
            t_handle = self.G.nodes[tgt].get("author_handle", tgt[:20])
            ds = data.get("distortion_score")
            tone = data.get("tone_shift", "?")
            fc = "✓ fact-check" if data.get("is_fact_check") else ""
            print(f"    @{s_handle} → @{t_handle}"
                  f"  distortion={f'{ds:.2f}' if ds else 'N/A'}"
                  f"  tone={tone}  {fc}")
        print("═"*55)


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    """
    Usage :
        python provenance_graph.py <uri_or_url> [max_depth] [window_days]

    Exemples :
        python provenance_graph.py at://did:plc:xxx/app.bsky.feed.post/yyy
        python provenance_graph.py https://bsky.app/profile/alice.bsky.social/post/abc 3 14
    """
    if not PIPELINE_AVAILABLE:
        print("❌ La pipeline (étapes 1-5) n'est pas disponible.")
        print("   Assure-toi que pipeline.py est présent et que les dépendances sont installées.")
        sys.exit(1)

    args = sys.argv[1:]

    root_uri = args[0] if len(
        args) > 0 else "https://bsky.app/profile/mathieuhourdin.bsky.social/post/3mfetst7zbc2z"
    max_depth = int(args[1]) if len(args) > 1 else DEFAULT_MAX_DEPTH
    window_days = int(args[2]) if len(args) > 2 else DEFAULT_WINDOW_DAYS

    graph = ProvenanceGraph(max_depth=max_depth, window_days=window_days)
    graph.build(root_uri)
    graph.print_summary()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    graph.save_json(f"provenance_{ts}.json")
    graph.save_graphml(f"provenance_{ts}.graphml")
