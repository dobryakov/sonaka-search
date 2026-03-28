#!/usr/bin/env python3
"""
Печать атрибутов продукта из data/product_catalog_products.json.

Пример:
  python3 scripts/show_product_attributes.py prod_000007
  python3 scripts/show_product_attributes.py prod_000007 --max-values 5
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    default_catalog = root / "data" / "product_catalog_products.json"

    parser = argparse.ArgumentParser(description="Показать атрибуты продукта из каталога JSON.")
    parser.add_argument("product_id", help="Например prod_000007")
    parser.add_argument(
        "--file",
        type=Path,
        default=default_catalog,
        help=f"JSON с ключом products (по умолчанию: {default_catalog})",
    )
    parser.add_argument(
        "--max-values",
        type=int,
        default=0,
        metavar="N",
        help="Не больше N значений на атрибут (0 = без ограничения).",
    )
    args = parser.parse_args()

    if not args.file.is_file():
        print(f"Нет файла: {args.file}", file=sys.stderr)
        sys.exit(1)

    payload = json.loads(args.file.read_text(encoding="utf-8"))
    products = payload.get("products")
    if not products:
        print("В файле нет массива products.", file=sys.stderr)
        sys.exit(1)

    pid = args.product_id.strip()
    product = next((p for p in products if p.get("product_id") == pid), None)
    if product is None:
        print(f"Продукт {pid!r} не найден.", file=sys.stderr)
        sys.exit(1)

    cat = product.get("category_name") or product.get("category_slug") or "—"
    title = product.get("title") or "—"

    print(f"product_id:  {pid}")
    print(f"категория:   {cat}")
    print(f"название:    {title}")
    print()
    print("Атрибуты:")
    print()

    attrs = product.get("attributes") or []
    if not attrs:
        print("  (нет атрибутов)")
        return

    for row in attrs:
        aid = row.get("id", "?")
        name = row.get("name", "—")
        values = row.get("values")
        if not isinstance(values, list):
            values = []
        n = len(values)
        uniq = len(set(values))

        print(f"  [{aid}] {name}")
        if n == 0:
            print("    (нет значений)")
            print()
            continue

        cap = args.max_values
        if cap and cap > 0:
            to_show = values[:cap]
        else:
            to_show = values

        for v in to_show:
            print(f"    – {v}")
        if cap and cap > 0 and n > cap:
            print(f"    … ещё {n - cap} значений (всего {n}, уникальных {uniq})")
        elif uniq != n:
            print(f"    — всего значений: {n}, уникальных: {uniq}")
        print()


if __name__ == "__main__":
    main()
