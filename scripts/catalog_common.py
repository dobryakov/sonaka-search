"""Общие хелперы для описания атрибутов каталога."""

from __future__ import annotations


def a_cat(slug: str, name: str, values: list[str]) -> dict:
    assert 5 <= len(values) <= 10, (name, len(values))
    return {"categories": [slug], "name": name, "values": values}


def a_global(name: str, values: list[str]) -> dict:
    assert 5 <= len(values) <= 10, (name, len(values))
    return {"categories": ["*"], "name": name, "values": values}
