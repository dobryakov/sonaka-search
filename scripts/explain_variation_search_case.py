#!/usr/bin/env python3
"""
Поясняет «что искал бы тест» в человекочитаемом виде: имя атрибута + значение,
а не только поле ES и attr_NNN.

Использует ту же логику выбора вариации и фильтров, что tests/test_variations_search.py
(random seed из TEST_VARIATIONS_SEARCH_SEED, по умолчанию 42).

Пример:
  python3 scripts/explain_variation_search_case.py
  TEST_VARIATIONS_SEARCH_SEED=1 python3 scripts/explain_variation_search_case.py
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from pathlib import Path

# импорт из tests при запуске из корня репозитория
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import tests.test_variations_search as tvs  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Показать условия поиска как в тесте, с именами атрибутов из каталога."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Переопределить seed (иначе TEST_VARIATIONS_SEARCH_SEED или 42).",
    )
    args = parser.parse_args()

    seed = args.seed if args.seed is not None else tvs._SEARCH_SEED
    rng = random.Random(seed)

    catalog_path = tvs._CATALOG_PATH
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    variations = catalog["variations"]
    fm = json.loads(tvs._FIELD_MAP_PATH.read_text(encoding="utf-8"))["fields"]
    rev_field_to_id = {v: k for k, v in fm.items()}

    term_filters, variation_id, product_id = tvs.load_random_term_filters_from_catalog(rng=rng)
    vrow = next(v for v in variations if v.get("variation_id") == variation_id)

    # id -> (name, value) из этой вариации
    by_id: dict[str, tuple[str, str]] = {}
    for item in vrow.get("attributes") or []:
        aid = item.get("id")
        if not aid:
            continue
        by_id[aid] = (str(item.get("name") or "—"), str(item.get("value") or ""))

    print(f"seed:              {seed}")
    print(f"variation_id:      {variation_id}")
    print(f"product_id:        {product_id}")
    print()
    print("Условия (как в Elasticsearch) ↔ атрибут в JSON вариации:")
    print()

    for es_field, value in term_filters:
        aid = rev_field_to_id.get(es_field, "?")
        name, v_in_row = by_id.get(aid, ("(нет в вариации)", ""))
        match = "✓" if v_in_row == value else "✗"
        print(f"  {match}  term[{es_field!r}] == {value!r}")
        print(f"       «{name}» ({aid})")
        if v_in_row != value:
            print(f"       в вариации по каталогу value={v_in_row!r} — несовпадение!")
        print()

    print(
        "Проверка «тест нашёл верно»: ожидаемый product_id должен попасть в ответ ES "
        "на этот bool.filter + collapse по product_id (см. тест)."
    )
    print("Ручная проверка в ES: GET sonaka_variations/_search с тем же телом запроса.")


if __name__ == "__main__":
    main()
