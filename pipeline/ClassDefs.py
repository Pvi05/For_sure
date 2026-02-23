from dataclasses import dataclass
from typing import Optional


@dataclass
class BlueskyPost:
    uri: str
    cid: str
    text: str
    date: str
    author_handle: str
    author_did: str
    author_display_name: str
    likes: int = 0
    reposts: int = 0
    replies: int = 0
    embed_url: Optional[str] = None
    # "reply" | "repost_parent" | "followee" | "keyword_search"
    source_strategy: str = "unknown"
    is_repost_parent: bool = False
    is_reply_parent: bool = False
    # True si l'auteur/domaine est dans TRUSTED_DOMAINS
    is_trusted_source: bool = False
    # URI du post auquel ce post répond
    reply_parent_uri: Optional[str] = None
    is_domained: bool = False

    @property
    def engagement(self) -> int:
        return self.likes + self.reposts

    @property
    def date_short(self) -> str:
        return self.date[:10]
