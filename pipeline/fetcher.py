"""
pipeline/fetcher.py — Étapes 1 & 7
=====================================
Étape 1 : Fetch d'un post Bluesky à partir d'une URI AT Protocol ou d'une URL.
Étape 7 : Scoring de fiabilité de la source originale.

Place dans la pipeline :
    fetcher.py → keywords_etape2.py → searcher_etape3.py → ...

Retourne :
    BlueskyPost (dataclass défini dans searcher_etape3.py)

Dépendances :
    pip install atproto

Variables d'environnement :
    BLUESKY_HANDLE    — handle du compte Bluesky (ex: for-sure.bsky.social)
    BLUESKY_PASSWORD  — app password Bluesky
"""

from __future__ import annotations

import os
from datetime import datetime, timezone


from atproto import Client
from ClassDefs import *


# ─────────────────────────────────────────────────────────────────────────────
# Client atproto (singleton authentifié)
# ─────────────────────────────────────────────────────────────────────────────

_client: Optional[Client] = None


def _get_client() -> Client:
    global _client
    if _client is None:
        _client = Client()
        handle = os.getenv("BLUESKY_HANDLE", "")
        password = os.getenv("BLUESKY_PASSWORD", "")
        if handle and password:
            _client.login(handle, password)
    return _client


# ─────────────────────────────────────────────────────────────────────────────
# Conversion URL → URI AT Protocol
# ─────────────────────────────────────────────────────────────────────────────

def _url_to_uri(url: str) -> str:
    """
    Convertit une URL bsky.app en AT URI.

    Formats supportés :
        https://bsky.app/profile/alice.bsky.social/post/3lc4xyzabc
        https://bsky.app/profile/did:plc:xxxxx/post/3lc4xyzabc
        at://did:plc:xxx/app.bsky.feed.post/yyy  (passé tel quel)
    """
    if url.startswith("at://"):
        return url

    parts = url.rstrip("/").split("/")
    # Format attendu : ['https:', '', 'bsky.app', 'profile', '<handle>', 'post', '<rkey>']
    try:
        handle = parts[4]
        rkey = parts[6]
    except IndexError:
        raise ValueError(f"URL Bluesky invalide : {url}")

    # Si c'est déjà un DID, on construit l'URI directement
    if handle.startswith("did:"):
        return f"at://{handle}/app.bsky.feed.post/{rkey}"

    # Sinon on résout le handle en DID via l'API
    client = _get_client()
    result = client.app.bsky.actor.search_actors({"term": handle})
    for actor in (result.actors or []):
        if actor.handle == handle:
            return f"at://{actor.did}/app.bsky.feed.post/{rkey}"

    raise ValueError(f"Handle introuvable : {handle}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

BSKY_OFFICIAL_LABELER = "did:plc:ar7c4by46qjdydhdevvrndac"


def _is_bsky_verified(profile) -> bool:
    """profile : objet retourné par client.app.bsky.actor.get_profile()"""
    for label in (getattr(profile, "labels", []) or []):
        if (getattr(label, "val", "") == "verified"
                and getattr(label, "src", "") == BSKY_OFFICIAL_LABELER):
            return True
    return False


def _has_domain_handle(handle: str) -> bool:
    return not handle.endswith(".bsky.social")
# e.g. "lemonde.fr" or "reuters.com" → True


# def _is_trusted(handle: str, embed_url: Optional[str]) -> bool:
#     if handle in TRUSTED_HANDLES:
#         return True
#     if embed_url:
#         return any(domain in embed_url for domain in TRUSTED_DOMAINS)
#     return False


# ─────────────────────────────────────────────────────────────────────────────
# Étape 1 — Fetch principal
# ─────────────────────────────────────────────────────────────────────────────

def fetch_post(uri_or_url: str) -> BlueskyPost:
    """
    Étape 1 — Fetch d'un post Bluesky depuis son URI ou son URL.

    Args:
        uri_or_url : AT URI (at://...) ou URL bsky.app

    Returns:
        BlueskyPost avec toutes ses métadonnées, prêt pour la pipeline.

    Raises:
        ValueError  : si l'URL est invalide ou le post introuvable.
        RuntimeError: si l'authentification échoue.
    """
    client = _get_client()
    uri = _url_to_uri(uri_or_url)

    resp = client.app.bsky.feed.get_posts(params={"uris": [uri]})
    if not resp.posts:
        raise ValueError(f"Post introuvable : {uri}")

    post_view = resp.posts[0]
    author = getattr(post_view, "author", None)
    record = getattr(post_view, "record", None)

    author_handle = getattr(author, "handle", "") if author else ""

    trusted = False
    try:
        profile = client.app.bsky.actor.get_profile({"actor": author_handle})
        trusted = _is_bsky_verified(
            profile) or _has_domain_handle(author_handle)
    except Exception:
        pass
    domained = _has_domain_handle(author_handle)

    embed_url = None
    if hasattr(post_view, "embed") and post_view.embed:
        embed = post_view.embed
        if hasattr(embed, "external") and embed.external:
            embed_url = getattr(embed.external, "uri", None)

    return BlueskyPost(
        uri=post_view.uri,
        cid=post_view.cid,
        text=getattr(record, "text", ""),
        date=getattr(record, "created_at", ""),
        author_handle=author_handle,
        author_did=getattr(author, "did", "") if author else "",
        author_display_name=getattr(author, "display_name", "") or "",
        likes=post_view.like_count or 0,
        reposts=post_view.repost_count or 0,
        replies=post_view.reply_count or 0,
        embed_url=embed_url,
        source_strategy="root",
        is_trusted_source=trusted,
        is_domained=domained
    )


# ─────────────────────────────────────────────────────────────────────────────
# Smoke test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    url = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "https://bsky.app/profile/mathieuhourdin.bsky.social/post/3mfetst7zbc2z"
    )

    print(f"Fetch de : {url}\n")
    post = fetch_post(url)
    print(f"✓ @{post.author_handle} — {post.date[:10]}")
    print(f"  Texte   : {post.text[:100]}")
    print(f"  ❤️  {post.likes}  🔁 {post.reposts}  💬 {post.replies}")
    print(f"  Source certifiée : {post.is_trusted_source}")

    print(post)
