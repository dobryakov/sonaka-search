#!/usr/bin/env python3
"""
Собирает elastic/variations.index.json, elastic/products.index.json
и elastic/variation_attribute_field_map.json из data/product_attributes.json.

Имена полей вариаций — snake_case по смыслу (транслит с русского), без attr_NNN.
При коллизиях добавляется короткий числовой суффикс из номера атрибута (001…300).
Карта attr_* → имя поля нужна пайплайну индексации вариаций.

Маппинг продуктов — вложенные attributes с полем values (массив keyword),
в соответствии с product_catalog_products.json (schema_version ≥ 2).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# Транслитерация (упрощённая латиница, удобная для имён полей)
_RU2LAT_LOWER = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "sch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}

# Замена отдельных символов до транслита
_CHAR_REPLACEMENTS = str.maketrans(
    {
        "«": " ",
        "»": " ",
        "№": "n",
        "°": " ",
        "×": "x",
        "–": " ",
        "—": " ",
        "…": " ",
    }
)


def _transliterate_ru(text: str) -> str:
    buf: list[str] = []
    for ch in text.lower():
        if ch in _RU2LAT_LOWER:
            buf.append(_RU2LAT_LOWER[ch])
        elif "a" <= ch <= "z" or ch.isdigit():
            buf.append(ch)
        elif ch in " .,_/\\-:+()[]{}|\"'!?":
            buf.append("_")
        else:
            buf.append("_")
    return "".join(buf)


def attribute_name_to_slug(name: str) -> str:
    s = name.translate(_CHAR_REPLACEMENTS)
    s = _transliterate_ru(s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    s = re.sub(r"_+", "_", s)
    return s


def unique_field_name(slug: str, attr_id: str, taken: set[str]) -> str:
    """Уникальное имя поля; при занятости добавляет _NNN из attr_XXX."""
    num = attr_id.replace("attr_", "").lstrip("0") or "0"
    base = slug or "field"
    if not re.match(r"^[a-z]", base):
        base = "f_" + base
    candidate = base
    if candidate not in taken:
        taken.add(candidate)
        return candidate
    candidate = f"{base}_{num}"
    k = 0
    while candidate in taken:
        k += 1
        candidate = f"{base}_{num}_{k}"
    taken.add(candidate)
    return candidate


def products_index_body() -> dict:
    """Тело PUT для индекса карточек продуктов (вложенные атрибуты, values — массив)."""
    return {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "1s",
            }
        },
        "mappings": {
            "properties": {
                "product_id": {"type": "keyword"},
                "category_slug": {"type": "keyword"},
                "category_name": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                    },
                },
                "title": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 512},
                    },
                },
                "description": {"type": "text"},
                "attributes": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                            },
                        },
                        "values": {"type": "keyword", "ignore_above": 512},
                    },
                },
            }
        },
    }


def build_field_map(attributes: list[dict]) -> dict[str, str]:
    """attr_id -> elasticsearch field name."""
    taken: set[str] = {
        "variation_id",
        "product_id",
        "sku",
        "variation_index",
        "search_all",
    }
    mapping: dict[str, str] = {}
    for a in attributes:
        aid = a["id"]
        name = a.get("name") or ""
        slug = attribute_name_to_slug(name)
        field = unique_field_name(slug, aid, taken)
        mapping[aid] = field
    return mapping


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    attrs_path = root / "data" / "product_attributes.json"
    out_variations = root / "elastic" / "variations.index.json"
    out_products = root / "elastic" / "products.index.json"
    out_map = root / "elastic" / "variation_attribute_field_map.json"

    if not attrs_path.is_file():
        print(f"Нет файла {attrs_path}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(attrs_path.read_text(encoding="utf-8"))
    attributes = data.get("attributes")
    if not attributes:
        print("В product_attributes.json нет attributes.", file=sys.stderr)
        sys.exit(1)

    field_map = build_field_map(attributes)

    properties: dict = {
        "variation_id": {"type": "keyword"},
        "product_id": {"type": "keyword"},
        "sku": {"type": "keyword"},
        "variation_index": {"type": "integer"},
        "search_all": {
            "type": "text",
            "analyzer": "standard",
        },
    }

    for a in attributes:
        aid = a["id"]
        fname = field_map[aid]
        properties[fname] = {
            "type": "keyword",
            "ignore_above": 256,
        }

    body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "1s",
            }
        },
        "mappings": {
            "properties": properties,
        },
    }

    try:
        source_rel = str(attrs_path.relative_to(root))
    except ValueError:
        source_rel = str(attrs_path)

    map_payload = {
        "schema_version": 1,
        "source_attributes": source_rel,
        "description": "Маппинг id атрибута из product_attributes.json → имя поля в индексе вариаций",
        "fields": field_map,
    }

    out_variations.parent.mkdir(parents=True, exist_ok=True)
    out_variations.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
    out_products.write_text(
        json.dumps(products_index_body(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    out_map.write_text(json.dumps(map_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Записано {out_variations} ({len(field_map)} полей атрибутов + ядро).")
    print(f"Записано {out_products} (карточки продуктов, nested values[]).")
    print(f"Записано {out_map} (карта attr_* → поле).")


if __name__ == "__main__":
    main()
