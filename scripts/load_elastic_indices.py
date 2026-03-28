#!/usr/bin/env python3
"""
Создание индексов Elasticsearch по заготовкам из каталога elastic/.
Не загружает документы — только PUT маппингов (см. load_elastic_products.py, load_elastic_variations.py).
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path


def http_request(
    method: str,
    url: str,
    *,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, str]:
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    default_products_body = root / "elastic" / "products.index.json"
    default_variations_body = root / "elastic" / "variations.index.json"

    parser = argparse.ArgumentParser(description="Создать индексы ES из elastic/*.index.json")
    parser.add_argument("--es-url", default="http://127.0.0.1:9200", help="Базовый URL кластера")
    parser.add_argument("--products-index", default="sonaka_products", help="Имя индекса продуктов")
    parser.add_argument("--variations-index", default="sonaka_variations", help="Имя индекса вариаций")
    parser.add_argument(
        "--products-mapping",
        type=Path,
        default=default_products_body,
        help="JSON тело PUT для индекса продуктов",
    )
    parser.add_argument(
        "--variations-mapping",
        type=Path,
        default=default_variations_body,
        help="JSON тело PUT для индекса вариаций",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Удалить индекс, если уже существует, затем создать заново",
    )
    parser.add_argument(
        "--relax-disk-watermarks",
        action="store_true",
        help="Выставить transient пороги диска (для стендов с заполненным диском)",
    )
    args = parser.parse_args()

    base = args.es_url.rstrip("/")

    if args.relax_disk_watermarks:
        settings = {
            "transient": {
                "cluster.routing.allocation.disk.watermark.low": "95%",
                "cluster.routing.allocation.disk.watermark.high": "98%",
                "cluster.routing.allocation.disk.watermark.flood_stage": "99%",
            }
        }
        code, body = http_request(
            "PUT",
            f"{base}/_cluster/settings",
            data=json.dumps(settings).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        print(f"_cluster/settings → HTTP {code}")
        if code not in (200, 201):
            print(body[:2000], file=sys.stderr)
            sys.exit(1)

    for name, path in [
        (args.products_index, args.products_mapping),
        (args.variations_index, args.variations_mapping),
    ]:
        if not path.is_file():
            print(f"Нет файла: {path}", file=sys.stderr)
            sys.exit(1)
        body_bytes = path.read_bytes()

        if args.recreate:
            code, _ = http_request("DELETE", f"{base}/{name}")
            print(f"DELETE {name} → HTTP {code}")

        code, resp_body = http_request(
            "PUT",
            f"{base}/{name}",
            data=body_bytes,
            headers={"Content-Type": "application/json"},
        )
        print(f"PUT {name} → HTTP {code}")
        if code not in (200, 201):
            print(resp_body[:4000], file=sys.stderr)
            sys.exit(1)

    print("Готово: индексы созданы.")


if __name__ == "__main__":
    main()
