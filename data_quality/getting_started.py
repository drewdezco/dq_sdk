"""
Getting started guide for new users.
Returns markdown-formatted quick start guide.
"""

from .docs_utils import get_getting_started_markdown


def get_getting_started_guide() -> str:
    """
    Returns a markdown-formatted getting started guide for new users.

    This is a thin wrapper around docs_utils.get_getting_started_markdown()
    so the public API stays stable while the content lives in docs/GETTING_STARTED.md.
    """
    return get_getting_started_markdown()
