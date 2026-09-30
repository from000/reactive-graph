"""Example tools for the SuperAgent (M3 verification).

Kept dependency-free so the whole example runs with no external services.
"""

from __future__ import annotations

from reactivegraph.prebuilt import ToolSpec

__all__ = ("default_tools", "get_weather", "search_notes")


def get_weather(city: str = "Shanghai") -> dict:
    """Return a deterministic weather reading for a city."""
    return {"city": city, "temp_c": 20, "condition": "sunny"}


def search_notes(query: str = "") -> dict:
    """Return an example note store hit for a query."""
    notes = {
        "reactive": "ReactiveGraph executes only affected nodes (selective update).",
        "langgraph": "LangGraph re-runs every node per invoke.",
    }
    for key, text in notes.items():
        if key in query.lower():
            return {"note": text}
    return {"note": "no matching note"}


def default_tools() -> list[ToolSpec]:
    return [
        ToolSpec(name="get_weather", fn=get_weather, description="weather for a city"),
        ToolSpec(name="search_notes", fn=search_notes, description="search internal notes"),
    ]