#!/usr/bin/env python3
"""
Сборка каталога из 300 атрибутов (5–10 значений) для стенда поиска.
Данные подгружаются из модулей catalog_*.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Часть 1: только каркас. Данные добавляются в catalog_parts.py
from catalog_parts import build_all_attributes, build_categories


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    out_path = root / "data" / "product_attributes.json"

    categories = build_categories()
    attributes = build_all_attributes()

    if len(attributes) != 300:
        print(f"Ожидалось 300 атрибутов, получено {len(attributes)}", file=sys.stderr)
        sys.exit(1)

    seen_names: set[str] = set()
    for i, a in enumerate(attributes, start=1):
        name = a["name"]
        if name in seen_names:
            print(f"Дубликат имени атрибута: {name!r}", file=sys.stderr)
            sys.exit(1)
        seen_names.add(name)
        vals = a["values"]
        if not (5 <= len(vals) <= 10):
            print(
                f"Атрибут #{i} {name!r}: ожидалось 5–10 значений, получено {len(vals)}",
                file=sys.stderr,
            )
            sys.exit(1)
        if len(set(vals)) != len(vals):
            print(f"Атрибут #{i} {name!r}: дубликаты в values", file=sys.stderr)
            sys.exit(1)

    payload = {
        "schema_version": 1,
        "domain": "furniture_sleep_appliances",
        "reference_style": "askona-like",
        "categories": categories,
        "attributes": attributes,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Записано {len(attributes)} атрибутов в {out_path}")


if __name__ == "__main__":
    main()
