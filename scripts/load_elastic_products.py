#!/usr/bin/env python3
"""
Bulk-загрузка продуктов в Elasticsearch из data/product_catalog_products.json.
Индекс должен уже существовать (см. load_elastic_indices.py).
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path


def bulk(es_base: str, lines: list[str]) -> dict:
    body = "\n".join(lines) + "\n"
    req = urllib.request.Request(
        f"{es_base.rstrip('/')}/_bulk",
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=300) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    default_file = root / "data" / "product_catalog_products.json"

    parser = argparse.ArgumentParser(description="Загрузить продукты в ES (_bulk)")
    parser.add_argument("--es-url", default="http://127.0.0.1:9200")
    parser.add_argument("--index", default="sonaka_products", help="Имя индекса")
    parser.add_argument(
        "--file",
        type=Path,
        default=default_file,
        help="JSON с ключом products (как из generate_product_cards.py)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=40,
        metavar="N",
        help="Документов за один _bulk (у больших карточек меньше — ниже риск 429).",
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        print("--batch-size должен быть >= 1.", file=sys.stderr)
        sys.exit(1)

    if not args.file.is_file():
        print(f"Нет файла: {args.file}", file=sys.stderr)
        sys.exit(1)

    payload = json.loads(args.file.read_text(encoding="utf-8"))
    products = payload.get("products")
    if not products:
        print("В файле нет массива products.", file=sys.stderr)
        sys.exit(1)

    idx = args.index
    lines: list[str] = []
    for p in products:
        pid = p.get("product_id")
        if not pid:
            print("Пропуск записи без product_id", file=sys.stderr)
            continue
        meta = {"index": {"_index": idx, "_id": pid}}
        lines.append(json.dumps(meta, ensure_ascii=False))
        lines.append(json.dumps(p, ensure_ascii=False))

    if not lines:
        print("Нет документов для загрузки.", file=sys.stderr)
        sys.exit(1)

    ndjson_lines = 2 * args.batch_size
    total_items = 0
    n_batches = (len(lines) + ndjson_lines - 1) // ndjson_lines

    try:
        for b in range(n_batches):
            start = b * ndjson_lines
            chunk = lines[start : start + ndjson_lines]
            result = bulk(args.es_url, chunk)
            items = result.get("items", [])
            failed = [i for i in items if (i.get("index") or {}).get("status", 200) >= 300]
            total_items += len(items)
            if failed:
                print(
                    f"Пакет {b + 1}/{n_batches}: ошибок {len(failed)}",
                    file=sys.stderr,
                )
                print(json.dumps(failed[0], ensure_ascii=False, indent=2)[:2000], file=sys.stderr)
                sys.exit(1)
            if result.get("errors"):
                print(f"Пакет {b + 1}/{n_batches}: bulk errors=true", file=sys.stderr)
                sys.exit(1)
    except urllib.error.HTTPError as e:
        print(e.read().decode("utf-8", errors="replace")[:4000], file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Сеть: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Пакетов: {n_batches}, строк bulk (документов): {total_items}, ошибок: 0")
    print("Готово.")


if __name__ == "__main__":
    main()
