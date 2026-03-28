#!/usr/bin/env python3
"""
Запрашивает у Elasticsearch фактическое число документов в индексах продуктов и вариаций.

Пример:
  python3 scripts/count_elastic_documents.py
  python3 scripts/count_elastic_documents.py --es-url http://localhost:9200
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


def get_count(es_base: str, index: str) -> int:
    url = f"{es_base.rstrip('/')}/{index}/_count"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return int(payload.get("count", 0))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Показать число документов в индексах продуктов и вариаций в Elasticsearch."
    )
    parser.add_argument("--es-url", default="http://127.0.0.1:9200", help="Базовый URL кластера")
    parser.add_argument("--products-index", default="sonaka_products", help="Имя индекса продуктов")
    parser.add_argument("--variations-index", default="sonaka_variations", help="Имя индекса вариаций")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Вывести одну строку JSON вместо текста",
    )
    args = parser.parse_args()

    base = args.es_url.rstrip("/")

    try:
        n_products = get_count(base, args.products_index)
        n_variations = get_count(base, args.variations_index)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:2000]
        print(f"HTTP {e.code}: {body}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Сеть: {e}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(
            json.dumps(
                {
                    "elasticsearch_url": base,
                    "products_index": args.products_index,
                    "products_count": n_products,
                    "variations_index": args.variations_index,
                    "variations_count": n_variations,
                },
                ensure_ascii=False,
            )
        )
    else:
        print(f"URL:           {base}")
        print(f"Продукты:      {n_products}  (индекс {args.products_index})")
        print(f"Вариации:      {n_variations}  (индекс {args.variations_index})")


if __name__ == "__main__":
    main()
