#!/usr/bin/env python3
"""
Bulk-загрузка вариаций в Elasticsearch из data/product_catalog_variations.json.
Плоские поля имён атрибутов берутся из elastic/variation_attribute_field_map.json.
Поле search_all — склейка значений атрибутов для полнотекста.
Индекс должен уже существовать (см. load_elastic_indices.py).

Для больших файлов объекты из variations читаются потоком через jq (если есть в PATH);
иначе — json.loads целиком (может занять много памяти).
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path


def bulk(es_base: str, lines: list[str]) -> dict:
    body = "\n".join(lines) + "\n"
    req = urllib.request.Request(
        f"{es_base.rstrip('/')}/_bulk",
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as resp:
        return json.loads(resp.read().decode("utf-8"))


def variation_to_doc(row: dict, field_map: dict[str, str]) -> dict:
    doc: dict = {
        "variation_id": row["variation_id"],
        "product_id": row["product_id"],
        "sku": row["sku"],
        "variation_index": row["variation_index"],
    }
    parts: list[str] = []
    for item in row.get("attributes") or []:
        aid = item.get("id")
        val = item.get("value")
        if not aid or val is None:
            continue
        fname = field_map.get(aid)
        if not fname:
            print(f"Нет маппинга для {aid}, значение пропущено", file=sys.stderr)
            continue
        doc[fname] = val
        parts.append(str(val))
    doc["search_all"] = " ".join(parts)
    return doc


def iter_variations_rows(path: Path) -> Iterator[dict]:
    """
    Потоково отдаёт объекты из массива variations.
    Если доступен jq — читаем без полного json.loads (для больших файлов);
    иначе загружаем JSON целиком (только для небольших каталогов).
    """
    jq = shutil.which("jq")
    if jq:
        proc = subprocess.Popen(
            [jq, "-c", ".variations[]", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        if proc.stdout is None:
            raise RuntimeError("jq: не удалось открыть stdout")
        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
        finally:
            proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()
            rc = proc.wait()
            if rc != 0:
                raise RuntimeError(f"jq завершился с кодом {rc}")
        return

    payload = json.loads(path.read_text(encoding="utf-8"))
    variations = payload.get("variations")
    if not variations:
        return
    yield from variations


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    default_variations = root / "data" / "product_catalog_variations.json"
    default_map = root / "elastic" / "variation_attribute_field_map.json"

    parser = argparse.ArgumentParser(description="Загрузить вариации в ES (_bulk)")
    parser.add_argument("--es-url", default="http://127.0.0.1:9200")
    parser.add_argument("--index", default="sonaka_variations", help="Имя индекса")
    parser.add_argument(
        "--file",
        type=Path,
        default=default_variations,
        help="JSON с ключом variations",
    )
    parser.add_argument(
        "--field-map",
        type=Path,
        default=default_map,
        help="variation_attribute_field_map.json (ключ fields)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        metavar="N",
        help="Число документов за один _bulk (меньше — ниже риск 429 при большом каталоге).",
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        print("--batch-size должен быть >= 1.", file=sys.stderr)
        sys.exit(1)

    if not args.file.is_file():
        print(f"Нет файла: {args.file}", file=sys.stderr)
        sys.exit(1)
    if not args.field_map.is_file():
        print(f"Нет файла: {args.field_map}", file=sys.stderr)
        sys.exit(1)

    map_payload = json.loads(args.field_map.read_text(encoding="utf-8"))
    field_map = map_payload.get("fields")
    if not field_map:
        print("В field-map нет объекта fields.", file=sys.stderr)
        sys.exit(1)

    idx = args.index
    ndjson_lines = 2 * args.batch_size
    chunk: list[str] = []
    total_items = 0
    batch_idx = 0

    def flush_batch() -> None:
        nonlocal chunk, total_items, batch_idx
        if not chunk:
            return
        batch_idx += 1
        result = bulk(args.es_url, chunk)
        items = result.get("items", [])
        failed = [i for i in items if (i.get("index") or {}).get("status", 200) >= 300]
        total_items += len(items)
        if failed:
            print(f"Пакет {batch_idx}: ошибок {len(failed)}", file=sys.stderr)
            print(json.dumps(failed[0], ensure_ascii=False, indent=2)[:2000], file=sys.stderr)
            sys.exit(1)
        if result.get("errors"):
            print(f"Пакет {batch_idx}: bulk errors=true", file=sys.stderr)
            sys.exit(1)
        if batch_idx % 50 == 0:
            print(f"  … загружено документов: {total_items}", file=sys.stderr, flush=True)
        chunk = []

    try:
        n_rows = 0
        for row in iter_variations_rows(args.file):
            n_rows += 1
            vid = row.get("variation_id")
            if not vid:
                print("Пропуск записи без variation_id", file=sys.stderr)
                continue
            doc = variation_to_doc(row, field_map)
            meta = {"index": {"_index": idx, "_id": vid}}
            chunk.append(json.dumps(meta, ensure_ascii=False))
            chunk.append(json.dumps(doc, ensure_ascii=False))
            if len(chunk) >= ndjson_lines:
                flush_batch()
        flush_batch()
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except urllib.error.HTTPError as e:
        print(e.read().decode("utf-8", errors="replace")[:4000], file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Сеть: {e}", file=sys.stderr)
        sys.exit(1)

    if total_items == 0:
        print("В файле нет массива variations или он пуст.", file=sys.stderr)
        sys.exit(1)

    print(f"Пакетов: {batch_idx}, строк bulk (документов): {total_items}, ошибок: 0")
    print("Готово.")


if __name__ == "__main__":
    main()
