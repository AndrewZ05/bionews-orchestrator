#!/usr/bin/env python3
"""
Facebook Post Spool Pattern
Implements per-run parent post extraction with temporary manifest for dependent tables.
This eliminates duplicate post fetches and provides a streaming data pattern.
"""

import logging
from typing import Dict, List, Iterator, Tuple, Optional
from datetime import date

logger = logging.getLogger(__name__)


class PostSpool:
    """
    Manages a temporary in-memory spool of posts with dependent metadata.
    Used by post_insights and post_attachments to avoid re-fetching posts.
    """

    def __init__(self):
        self.posts_full = {}  # Full post records indexed by post_id
        self.posts_manifest = {}  # Minimal metadata for dependents
        self.post_count = 0
        self.cache_hits = 0
        self.cache_misses = 0

    def add_post(self, post: Dict) -> None:
        """Add a post to the spool."""
        post_id = post.get('id')
        if not post_id:
            return

        self.posts_full[post_id] = post

        # Extract minimal fields needed by post_insights and post_attachments
        self.posts_manifest[post_id] = {
            'id': post_id,
            'account_id': post.get('account_id'),
            'page_id': post.get('page_id'),
            'page_name': post.get('page_name', ''),
            'created_time': post.get('created_time', ''),
            'comments_count': post.get('comments_count', 0),
            'shares_count': post.get('shares_count', 0),
            'link': post.get('link', ''),
            'object_id': post.get('object_id', ''),
            'page_access_token': post.get('page_access_token', ''),
            'message': post.get('message', ''),
        }

        self.post_count += 1

    def get_post_by_id(self, post_id: str) -> Optional[Dict]:
        """Retrieve a post by ID."""
        if post_id in self.posts_full:
            self.cache_hits += 1
            return self.posts_full[post_id]
        self.cache_misses += 1
        return None

    def get_manifest_by_id(self, post_id: str) -> Optional[Dict]:
        """Retrieve minimal post metadata by ID."""
        if post_id in self.posts_manifest:
            self.cache_hits += 1
            return self.posts_manifest[post_id]
        self.cache_misses += 1
        return None

    def get_all_manifests(self) -> Iterator[Dict]:
        """Iterate all minimal post records (for dependent extraction)."""
        for manifest in self.posts_manifest.values():
            yield manifest

    def get_all_full_posts(self) -> Iterator[Dict]:
        """Iterate all full post records (for posts table output)."""
        for post in self.posts_full.values():
            yield post

    def size(self) -> int:
        """Return number of posts in spool."""
        return self.post_count

    def stats(self) -> Dict:
        """Return spool statistics."""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total_requests * 100) if total_requests > 0 else 0

        return {
            'post_count': self.post_count,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'hit_rate': hit_rate,
        }

    def clear(self) -> None:
        """Clear the spool."""
        self.posts_full.clear()
        self.posts_manifest.clear()
        self.post_count = 0
        self.cache_hits = 0
        self.cache_misses = 0


# Global spool instance (per-run lifecycle)
_current_spool = None


def create_spool() -> PostSpool:
    """Create a new spool for this extraction run."""
    global _current_spool
    _current_spool = PostSpool()
    logger.info("Created new post spool for this extraction run")
    return _current_spool


def get_current_spool() -> Optional[PostSpool]:
    """Get the current active spool."""
    return _current_spool


def reset_spool() -> None:
    """Reset the global spool (call between runs)."""
    global _current_spool
    if _current_spool:
        stats = _current_spool.stats()
        logger.info(f"Spool statistics: {stats}")
        _current_spool.clear()
    _current_spool = None
    logger.info("Post spool reset")
