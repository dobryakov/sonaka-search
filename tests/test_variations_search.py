"""
Поиск по индексу вариаций с коллапсом по product_id.

Условия запроса выбираются псевдослучайно, но детерминированно (фиксированный seed):
случайная вариация из каталога и случайное подмножество её атрибутов (7–10 полей)
с маппингом в ES. Запрос: bool.filter из term по этим полям — менее жёстко, чем
фиксированные 10 атрибутов первой строки.

Ожидаемый product_id — у выбранной вариации; она обязана попадать в выдачу,
так как все term-ы — её собственные значения.

Переменные окружения:
  ELASTICSEARCH_URL — по умолчанию http://127.0.0.1:9200
  ELASTICSEARCH_VARIATIONS_INDEX — по умолчанию sonaka_variations
  TEST_VARIATIONS_SEARCH_SEED — seed RNG (по умолчанию 42)
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ES_URL = os.environ.get("ELASTICSEARCH_URL", "http://127.0.0.1:9200").rstrip("/")
VARIATIONS_INDEX = os.environ.get("ELASTICSEARCH_VARIATIONS_INDEX", "sonaka_variations")
_SEARCH_SEED = int(os.environ.get("TEST_VARIATIONS_SEARCH_SEED", "42"))

_ROOT = Path(__file__).resolve().parent.parent
_CATALOG_PATH = _ROOT / "data" / "product_catalog_variations.json"
_FIELD_MAP_PATH = _ROOT / "elastic" / "variation_attribute_field_map.json"

# Сколько term-условий в одном запросе (случайно в этом диапазоне).
_FILTERS_MIN = 7
_FILTERS_MAX = 10

# Сколько раз пробуем другую вариацию, если у текущей мало атрибутов с маппингом.
_MAX_VARIATION_TRIES = 200


def _mappable_pairs(variation: dict, field_map: dict[str, str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in variation.get("attributes") or []:
        aid = item.get("id")
        val = item.get("value")
        if aid is None or val is None:
            continue
        fname = field_map.get(aid)
        if not fname:
            continue
        out.append((fname, str(val)))
    return out


def load_random_term_filters_from_catalog(
    *,
    catalog_path: Path = _CATALOG_PATH,
    field_map_path: Path = _FIELD_MAP_PATH,
    rng: random.Random | None = None,
    filters_min: int = _FILTERS_MIN,
    filters_max: int = _FILTERS_MAX,
) -> tuple[list[tuple[str, str]], str, str]:
    """
    Случайная вариация и случайное подмножество её атрибутов с маппингом.

    Возвращает: (список (поле_es, значение), variation_id, product_id).
    """
    if not catalog_path.is_file():
        raise FileNotFoundError(str(catalog_path))
    if not field_map_path.is_file():
        raise FileNotFoundError(str(field_map_path))
    if filters_min < 1 or filters_max < filters_min:
        raise ValueError("Некорректный диапазон filters_min / filters_max")

    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    variations = catalog.get("variations") or []
    if not variations:
        raise ValueError("В каталоге нет вариаций")

    map_payload = json.loads(field_map_path.read_text(encoding="utf-8"))
    field_map: dict[str, str] = map_payload.get("fields") or {}
    if not field_map:
        raise ValueError("В field map нет fields")

    rnd = rng if rng is not None else random.Random(_SEARCH_SEED)
    order = list(range(len(variations)))
    rnd.shuffle(order)

    for attempt in range(min(_MAX_VARIATION_TRIES, len(order))):
        v = variations[order[attempt]]
        vid = v.get("variation_id")
        pid = v.get("product_id")
        if not vid or not pid:
            continue
        pairs = _mappable_pairs(v, field_map)
        hi = min(filters_max, len(pairs))
        if hi < filters_min:
            continue
        n = rnd.randint(filters_min, hi)
        chosen = rnd.sample(pairs, n)
        return chosen, str(vid), str(pid)

    raise ValueError(
        f"Не нашли вариацию с ≥{filters_min} атрибутами с маппингом "
        f"(попыток: {min(_MAX_VARIATION_TRIES, len(variations))})."
    )


def _http_json(method: str, url: str, body: dict | None = None, timeout: float = 120.0) -> tuple[int, dict]:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return resp.status, json.loads(raw) if raw else {}


def es_reachable() -> bool:
    try:
        code, _ = _http_json("GET", f"{ES_URL}/", timeout=10.0)
        return code == 200
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


@unittest.skipUnless(es_reachable(), f"Elasticsearch недоступен ({ES_URL})")
class TestVariationsAttributeSearch(unittest.TestCase):
    def test_search_random_term_subset_collapsed_to_products(self) -> None:
        try:
            term_filters, _expected_variation_id, expected_product_id = load_random_term_filters_from_catalog()
        except (FileNotFoundError, ValueError) as e:
            self.skipTest(str(e))

        self.assertGreaterEqual(len(term_filters), _FILTERS_MIN)
        self.assertLessEqual(len(term_filters), _FILTERS_MAX)

        filters = [{"term": {field: value}} for field, value in term_filters]
        query_body = {
            "query": {"bool": {"filter": filters}},
            "collapse": {"field": "product_id"},
            "sort": [{"product_id": "asc"}],
            "size": 500,
            "_source": ["product_id"],
        }

        url = f"{ES_URL}/{VARIATIONS_INDEX}/_search"
        t0 = time.perf_counter()
        status, payload = _http_json("GET", url, body=query_body)
        es_search_s = time.perf_counter() - t0
        print(
            f"[test_variations_search] Elasticsearch _search: {es_search_s:.3f} s "
            f"(terms={len(term_filters)}, collapse=product_id)",
            file=sys.stderr,
            flush=True,
        )
        self.assertEqual(status, 200, msg=json.dumps(payload, ensure_ascii=False)[:2000])

        hits = payload.get("hits") or {}
        total = hits.get("total")
        if isinstance(total, dict):
            n_match_variations = total.get("value", 0)
        else:
            n_match_variations = int(total or 0)

        self.assertGreaterEqual(
            n_match_variations,
            1,
            msg="Нет совпавших вариаций — индекс пуст или не совпадает с каталогом.",
        )

        hit_list = hits.get("hits") or []
        self.assertTrue(hit_list, msg="hits.hits пуст при total > 0")

        product_ids = []
        for h in hit_list:
            src = h.get("_source") or {}
            self.assertEqual(
                set(src.keys()),
                {"product_id"},
                msg=f"Ожидали только product_id в _source, получили {set(src.keys())!r}",
            )
            pid = src.get("product_id")
            self.assertIsNotNone(pid)
            product_ids.append(pid)

        self.assertEqual(
            len(product_ids),
            len(set(product_ids)),
            msg="После collapse не должно быть повторяющихся product_id в hits.hits",
        )

        self.assertIn(
            expected_product_id,
            product_ids,
            msg=(
                f"Ожидали product_id {expected_product_id!r} среди коллапсированных "
                f"(seed={_SEARCH_SEED}), получили {len(product_ids)} id; "
                f"при нехватке size увеличьте его в тесте."
            ),
        )


if __name__ == "__main__":
    unittest.main()
