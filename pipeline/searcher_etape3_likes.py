"""
pipeline/searcher.py — Étape 3
================================
4 stratégies en cascade, par ordre de précision :

  A1 — Réponse (reply) : si le post répond à un autre post,
       le post parent est le lien de provenance le plus direct.

  A2 — Repost / Quote : si le post est un repost ou un quote,
       on remonte au post original.

  B  — Likers : cherche dans les feeds des likers du post
       un post antérieur sur le même sujet.
       (Plus rapide : 1 seul appel pour récupérer les likers vs. parcourir les abonnements.)

  C  — Keywords : recherche globale par mots-clés (fallback large).

Priorité dans le tri final : reply > repost_parent > liker > keyword_search
"""

import os
from datetime import datetime, timedelta

from typing import Optional
from collections import Counter
from fetcher import _is_bsky_verified, _has_domain_handle
from ClassDefs import *

from itertools import combinations

from atproto import Client


# ─────────────────────────────────────────────
# Sources de confiance
# ─────────────────────────────────────────────
# Handles Bluesky ou domaines dans embed_url considérés comme sources certifiées.
# Quand un post vient d'une de ces sources, la récursion s'arrête sur cette branche.

_client: Optional[Client] = None


def _get_client() -> Client:
    global _client
    if _client is None:
        _client = Client()
        handle = os.getenv("BSKY_HANDLE", "for-sure.bsky.social")
        password = os.getenv("BSKY_APP_PASSWORD", "LFfqA_n..7kz9KZ")
        _client.login(handle, password)
    return _client


def _parse_post(post_view, strategy: str = "unknown") -> BlueskyPost:
    client = _get_client()
    author = getattr(post_view, "author", None)
    record = getattr(post_view, "record", None)
    handle = getattr(author, "handle", "") if author else ""
    embed_url = None
    author_handle = getattr(author, "handle", "") if author else ""
    trusted = False
    try:
        profile = client.app.bsky.actor.get_profile({"actor": author_handle})
        trusted = _is_bsky_verified(
            profile) or _has_domain_handle(author_handle)
    except Exception:
        pass
    domained = _has_domain_handle(author_handle)

    if hasattr(post_view, "embed") and post_view.embed:
        embed = post_view.embed
        if hasattr(embed, "external") and embed.external:
            embed_url = getattr(embed.external, "uri", None)
    return BlueskyPost(
        uri=post_view.uri, cid=post_view.cid,
        text=getattr(record, "text", ""),
        date=getattr(record, "created_at", ""),
        author_handle=handle,
        author_did=getattr(author, "did", ""),
        author_display_name=getattr(author, "display_name", "") or "",
        likes=post_view.like_count or 0,
        reposts=post_view.repost_count or 0,
        replies=post_view.reply_count or 0,
        embed_url=embed_url,
        source_strategy=strategy,
        reply_parent_uri=_extract_reply_parent_uri(record),
        is_trusted_source=trusted,
        is_domained=domained
    )


def _extract_reply_parent_uri(record) -> Optional[str]:
    """
    Extrait l'URI du post parent si ce post est une réponse (reply).
    Dans AT Protocol, les replies ont un champ record.reply.parent.uri
    """
    reply = getattr(record, "reply", None)
    if not reply:
        return None
    parent = getattr(reply, "parent", None)
    if parent and hasattr(parent, "uri"):
        return parent.uri
    return None


def _before_date(date_str: str, before: str) -> bool:
    try:
        return (datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                < datetime.fromisoformat(before.replace("Z", "+00:00")))
    except ValueError:
        return True


def _within_window(date_str: str, before: str, window_days: int) -> bool:
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        dt_ref = datetime.fromisoformat(before.replace("Z", "+00:00"))
        return dt_ref - timedelta(days=window_days) <= dt < dt_ref
    except ValueError:
        return True


def _build_queries(keywords: dict, combo_size: int = 3, max_queries: int = 5) -> list[str]:
    """
    Génère des requêtes de recherche en combinant keywords et entities.

    Prend toutes les combinaisons possibles de `combo_size` termes parmi
    l'union des keywords et entities, en priorisant les entities (plus précises).
    Les claims ne sont pas utilisés pour la construction des requêtes.

    Args:
        keywords:    Dictionnaire avec les clés 'keywords' et 'entities'.
        combo_size:  Nombre de termes par combinaison (défaut : 3).
        max_queries: Nombre maximum de requêtes retournées (défaut : 5).

    Returns:
        Liste de chaînes de requêtes dédupliquées, limitée à max_queries.
    """
    entities = keywords.get("entities", [])
    kws = keywords.get("keywords", [])

    # Entities en premier : elles sont plus discriminantes
    terms = list(dict.fromkeys(entities + kws))  # dédupliqué, ordre préservé

    queries = []
    seen: set[str] = set()

    # Combinaisons de taille combo_size, puis taille réduite en fallback
    for size in range(min(combo_size, len(terms)), 0, -1):
        for combo in combinations(terms, size):
            q = " ".join(combo)[:100].strip()
            if q and q not in seen:
                seen.add(q)
                queries.append(q)
            if len(queries) >= max_queries:
                return queries

    return queries


def _print_strategy_summary(posts: list) -> None:
    counts = Counter(p.source_strategy for p in posts)
    for strategy, count in counts.items():
        print(f"          {strategy}: {count} post(s)")


# ── Stratégie A1 — Réponse directe ───────────

def _get_reply_parent(post) -> Optional[BlueskyPost]:
    """
    Vérifie si le post est une réponse (reply) à un autre post
    et retourne le post parent si c'est le cas.

    Dans AT Protocol, les replies ont record.reply.parent.uri
    C'est le lien de provenance le plus fort : l'auteur répond
    explicitement à un post précis.
    """
    client = _get_client()
    record = getattr(post, "record", None)
    if not record:
        return None

    parent_uri = _extract_reply_parent_uri(record)
    if not parent_uri:
        return None

    print(
        f"[Étape 3-A1] 💬 Reply détecté → fetch du post parent : {parent_uri}")
    try:
        resp = client.app.bsky.feed.get_posts(params={"uris": [parent_uri]})
        if resp.posts:
            p = _parse_post(resp.posts[0], strategy="reply")
            p.is_reply_parent = True
            return p
    except Exception as e:
        print(f"[Étape 3-A1] ⚠️  Fetch parent échoué : {e}")
    return None


# ── Stratégie A2 — Repost / Quote ────────────

def _get_repost_parent(post) -> Optional[BlueskyPost]:
    client = _get_client()
    record = getattr(post, "record", None)
    if not record:
        return None

    # Repost natif
    subject = getattr(record, "subject", None)
    if subject and hasattr(subject, "uri"):
        print(f"[Étape 3-A] 🔁 Repost natif → {subject.uri}")
        try:
            resp = client.app.bsky.feed.get_posts(
                params={"uris": [subject.uri]})
            if resp.posts:
                p = _parse_post(resp.posts[0], strategy="repost_parent")
                p.is_repost_parent = True
                return p
        except Exception as e:
            print(f"[Étape 3-A] ⚠️  {e}")
        return None

    # Quote post
    embed = getattr(record, "embed", None)
    if embed:
        embed_record = getattr(embed, "record", None)
        if embed_record and hasattr(embed_record, "uri"):
            print(f"[Étape 3-A] 💬 Quote post → {embed_record.uri}")
            try:
                resp = client.app.bsky.feed.get_posts(
                    params={"uris": [embed_record.uri]})
                if resp.posts:
                    p = _parse_post(resp.posts[0], strategy="repost_parent")
                    p.is_repost_parent = True
                    return p
            except Exception as e:
                print(f"[Étape 3-A] ⚠️  {e}")
    return None


# ── Stratégie B ──────────────────────────────

def _search_in_likers(
    post_uri: str, keywords: dict, before_date: str,
    window_days: int, min_engagement: int,
    max_likers: int = 20, max_posts_per_liker: int = 10,
) -> list[BlueskyPost]:
    """
    Stratégie B — Likers : récupère les utilisateurs ayant liké le post,
    puis cherche dans leurs feeds un post antérieur sur le même sujet.

    Avantage par rapport aux followees :
    - Un seul appel API pour obtenir les likers (vs. paginer les abonnements).
    - Les likers ont probablement été exposés au même sujet et l'ont relayé.
    """
    client = _get_client()
    results = []
    seen: set[str] = set()

    print(f"[Étape 3-B] ❤️  Récupération des likers...")
    try:
        resp = client.app.bsky.feed.get_likes(
            params={"uri": post_uri, "limit": max_likers}
        )
        liker_dids = [like.actor.did for like in (resp.likes or []) if getattr(like, "actor", None)]
        print(f"[Étape 3-B]    → {len(liker_dids)} liker(s)")
    except Exception as e:
        print(f"[Étape 3-B] ⚠️  {e}")
        return []

    kws_all = keywords.get("keywords", []) + keywords.get("entities", [])

    for did in liker_dids[:max_likers]:
        try:
            feed_resp = client.app.bsky.feed.get_author_feed(
                params={"actor": did, "limit": max_posts_per_liker}
            )
            for item in (feed_resp.feed or []):
                try:
                    post = _parse_post(item.post, strategy="liker")
                except Exception:
                    continue
                if post.uri in seen:
                    continue
                if not _before_date(post.date, before_date):
                    continue
                if not _within_window(post.date, before_date, window_days):
                    continue
                if post.engagement < min_engagement:
                    continue
                text_lower = post.text.lower()
                if not any(k.lower() in text_lower for k in kws_all):
                    continue
                seen.add(post.uri)
                results.append(post)
        except Exception:
            continue

    print(f"[Étape 3-B] ✅ {len(results)} post(s) trouvés chez les likers")
    return results


# ── Stratégie C ──────────────────────────────

def _search_by_keywords(
    keywords: dict, before_date: str, window_days: int,
    min_engagement: int, limit_per_query: int, exclude_uris: set,
) -> list[BlueskyPost]:
    client = _get_client()
    queries = _build_queries(keywords)
    results = []
    seen: set[str] = set()

    for q in queries:
        print(f"[Étape 3-C] 🔍 \"{q}\"")
        try:
            resp = client.app.bsky.feed.search_posts(
                params={"q": q, "limit": limit_per_query})
            for raw in (resp.posts or []):
                try:
                    post = _parse_post(raw, strategy="keyword_search")
                except Exception:
                    continue
                if post.uri in seen or post.uri in exclude_uris:
                    continue
                if not _before_date(post.date, before_date):
                    continue
                if not _within_window(post.date, before_date, window_days):
                    continue
                if post.engagement < min_engagement:
                    continue
                seen.add(post.uri)
                results.append(post)
        except Exception as e:
            print(f"[Étape 3-C] ⚠️  {e}")

    print(f"[Étape 3-C] ✅ {len(results)} candidat(s)")
    return results


# ── Fonction principale ───────────────────────

# Sentinel retourné quand aucun parent n'est trouvé après les 4 stratégies
NO_PARENT_FOUND = BlueskyPost(
    uri="unknown",
    cid="unknown",
    text="Aucun post parent identifiable — source primaire probable.",
    date="????-??-??T??:??:??Z",
    author_handle="unknown",
    author_did="unknown",
    author_display_name="Unknown",
    source_strategy="unknown",
)

STRATEGY_ORDER = {"reply": 0, "repost_parent": 1,
                  "liker": 2, "keyword_search": 3, "unknown": 4}


def search_prior_posts(
    post: BlueskyPost,
    keywords: dict,
    window_days: int = 1,
    min_engagement: int = 5,
    limit_per_query: int = 25,
    max_results: int = 10,
) -> list[BlueskyPost]:
    """
    Recherche les posts Bluesky antérieurs susceptibles d'être la source du post donné.

    Applique 4 stratégies en cascade, par ordre de priorité décroissante :
    - A1 (reply)          : remonte au post parent si le post est une réponse directe.
    - A2 (repost/quote)   : remonte au post original si le post est un repost ou un quote.
    - B  (likers)         : cherche dans les feeds des likers du post un post antérieur
                            correspondant aux mots-clés.
    - C  (keyword_search) : recherche globale par mots-clés (fallback large).

    La date de référence, le DID de l'auteur et l'URI à exclure sont déduits
    directement du post fourni (post.date, post.author_did, post.uri).

    Les candidats sont triés par priorité de stratégie puis par engagement décroissant.
    Si aucun candidat n'est trouvé, retourne [NO_PARENT_FOUND].

    Args:
        post:            Post Bluesky à analyser (source à remonter).
        keywords:        Dictionnaire avec les clés 'keywords', 'entities', 'claims'
                        (produit par keywords_etape2.py).
        window_days:     Fenêtre temporelle en jours avant post.date (défaut : 7).
        min_engagement:  Seuil minimum de likes + reposts pour retenir un candidat (défaut : 5).
        limit_per_query: Nombre maximum de résultats par requête (stratégie C, défaut : 25).
        max_results:     Nombre maximum de candidats retournés (défaut : 10).

    Returns:
        Liste de BlueskyPost triés par pertinence, ou [NO_PARENT_FOUND] si aucun résultat.
    """
    before_date = post.date
    author_did = post.author_did
    exclude_uri = post.uri

    all_candidates = []
    seen_uris: set[str] = {exclude_uri}
    # rest of the body unchanged ...

    # ── Stratégie A1 : réponse directe ───────────────────────────────────
    print("[Étape 3] ── Stratégie A1 : détection reply")
    reply_parent = _get_reply_parent(post)
    if reply_parent:
        seen_uris.add(reply_parent.uri)
        all_candidates.append(reply_parent)
        print(f"[Étape 3-A1] ✅ Parent reply : @{reply_parent.author_handle}")
    else:
        print("[Étape 3-A1]    Pas de reply détecté")

    # ── Stratégie A2 : repost / quote ────────────────────────────────────
    print("[Étape 3] ── Stratégie A2 : détection repost/quote")
    repost_parent = _get_repost_parent(post)
    if repost_parent:
        if repost_parent.uri not in seen_uris:
            seen_uris.add(repost_parent.uri)
            all_candidates.append(repost_parent)
            print(
                f"[Étape 3-A2] ✅ Parent repost : @{repost_parent.author_handle}")
    else:
        print("[Étape 3-A2]    Pas de repost détecté")

    # ── Stratégie B : likers ──────────────────────────────────────────────
    print("[Étape 3] ── Stratégie B : likers")
    for p in _search_in_likers(exclude_uri, keywords, before_date, window_days, min_engagement):
        if p.uri not in seen_uris:
            seen_uris.add(p.uri)
            all_candidates.append(p)

    # ── Stratégie C : mots-clés globaux ──────────────────────────────────
    print("[Étape 3] ── Stratégie C : mots-clés globaux")
    for p in _search_by_keywords(keywords, before_date, window_days, min_engagement, limit_per_query, seen_uris):
        if p.uri not in seen_uris:
            seen_uris.add(p.uri)
            all_candidates.append(p)

    all_candidates.sort(key=lambda p: (
        STRATEGY_ORDER.get(p.source_strategy, 4), -p.engagement))
    results = all_candidates[:max_results]

    if not results:
        print(f"\n[Étape 3] ⚠️  Aucun parent trouvé → retour NO_PARENT_FOUND")
        return [NO_PARENT_FOUND]

    print(f"\n[Étape 3] ✅ {len(results)} candidat(s) retenus")
    _print_strategy_summary(results)
    return results


if __name__ == "__main__":
    kw = {
        "keywords": ["café", "diabète", "étude"],
        "entities": ["Nature", "diabète de type 2"],
        "claims":   ["le café réduit le risque de diabète de type 2"]
    }

    post = BlueskyPost(uri='at://did:plc:jyszdkkd7q4ejj4mah36jgde/app.bsky.feed.post/3mfetst7zbc2z', cid='bafyreiafh27rlhgmdmip6greht2g6ybkbohejqndaoqqxomh4qwhma55r4', text="Bravo au journaliste de bfm sur place, qui répète à chaque fois qu'il prend la parole que les participants au rassemblement de Lyon essaient de masquer leur appartenance mais qu'il voit des croix gammées etc et que c'est des néonazis",
                       date='2026-02-21T14:35:04.993Z', author_handle='mathieuhourdin.bsky.social', author_did='did:plc:jyszdkkd7q4ejj4mah36jgde', author_display_name='Mathieu Hourdin', likes=390, reposts=119, replies=10, embed_url=None, source_strategy='root', is_repost_parent=False, is_reply_parent=False, is_trusted_source=False, reply_parent_uri=None)
    keywords = {'keywords': ['journaliste', 'bfm', 'rassemblement', 'Lyon', 'néonazis', 'croix gammées'], 'entities': ['bfm', 'Lyon'], 'claims': [
        'Un journaliste de BFM à Lyon affirme que les participants à un rassemblement masquent leur appartenance.', 'Le journaliste de BFM à Lyon voit des croix gammées parmi les participants.', 'Le journaliste de BFM à Lyon qualifie les participants de néonazis.']}

    posts = search_prior_posts(post, keywords)
    print(f"\n{len(posts)} post(s) trouvé(s) :\n")
    for p in posts:
        if p.is_reply_parent:
            tag = "💬 REPLY PARENT"
        elif p.is_repost_parent:
            tag = "🔁 REPOST PARENT"
        else:
            tag = f"[{p.source_strategy}]"
        print(
            f"  {tag} @{p.author_handle} [{p.date_short}] ❤️ {p.likes} 🔁 {p.reposts}")
        print(f"  {p.text[:100]}…\n")