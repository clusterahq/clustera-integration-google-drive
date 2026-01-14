"""Fetch message schemas for Google Drive worker.

Pydantic models for validating and parsing fetch request parameters.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class FetchFilters(BaseModel):
    """Filters for fetch requests."""

    id: str | None = None  # Single resource ID
    query: str | None = None  # Drive API query string
    trashed: bool | None = Field(default=False)  # Include trashed files
    start_page_token: str | None = None  # For changes API
    label_id: str | None = None  # Filter by label


class FetchPagination(BaseModel):
    """Pagination parameters for fetch requests."""

    page_token: str | None = None
    max_results: int = Field(default=100, ge=1, le=1000)


class FetchOptions(BaseModel):
    """Options for fetch requests."""

    format: str = Field(default="metadata")  # "metadata", "full", "minimal"
    include_raw: bool = Field(default=False)  # Include raw API response


def parse_fetch_params(
    raw_filters: dict[str, Any],
    raw_pagination: dict[str, Any],
    raw_options: dict[str, Any],
) -> tuple[FetchFilters, FetchPagination, FetchOptions]:
    """Parse and validate fetch parameters.

    Args:
        raw_filters: Raw filter dict from message
        raw_pagination: Raw pagination dict from message
        raw_options: Raw options dict from message

    Returns:
        Tuple of (filters, pagination, options) validated models
    """
    filters = FetchFilters(**raw_filters)
    pagination = FetchPagination(**raw_pagination)
    options = FetchOptions(**raw_options)

    return filters, pagination, options
