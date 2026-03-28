#!/usr/bin/env python3
"""
Генерация продуктовых карточек и вариаций на основе data/product_attributes.json.

P — число продуктов. V — базовое число вариаций на продукт; фактически у каждого
товара своё случайное число вариаций в диапазоне ±20% от V (не меньше 1).

Для каждого продукта сначала генерируются все вариации (набор атрибутов один и тот
же, значения выбираются независимо на каждую вариацию). Затем формируется карточка
продукта: у каждого атрибута поле values — массив всех значений этого атрибута
по вариациям (в порядке вариаций, дубликаты сохраняются).

Результат: два файла — продукты и вариации (см. --output-products / --output-variations).
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from pathlib import Path


def load_attributes(path: Path) -> tuple[list[dict], list[dict]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data["categories"], data["attributes"]


def applicable_attributes(category_slug: str, all_attrs: list[dict]) -> list[dict]:
    out: list[dict] = []
    for a in all_attrs:
        cats = a["categories"]
        if "*" in cats or category_slug in cats:
            out.append(a)
    return out


def pick_product_attributes(
    pool: list[dict],
    rng: random.Random,
    *,
    max_total: int = 36,
) -> list[dict]:
    """Фиксированный набор атрибутов для всех вариаций данного продукта."""
    if not pool:
        return []
    n = min(max_total, len(pool))
    picked = rng.sample(pool, n)
    picked.sort(key=lambda a: a["id"])
    return picked


def pick_value(attr: dict, rng: random.Random) -> str:
    return rng.choice(attr["values"])


def variation_count_for_product(base_v: int, rng: random.Random) -> int:
    """Случайное число вариаций в [0.8·V, 1.2·V], целое, не меньше 1."""
    low = max(1, math.floor(base_v * 0.8))
    high = max(low, math.ceil(base_v * 1.2))
    return rng.randint(low, high)


def build_variation_attributes(
    attr_defs: list[dict], rng: random.Random
) -> list[dict]:
    rows: list[dict] = []
    for a in attr_defs:
        rows.append(
            {
                "id": a["id"],
                "name": a["name"],
                "value": pick_value(a, rng),
            }
        )
    return rows


def aggregate_product_attributes(
    variations_attrs: list[list[dict]],
) -> list[dict]:
    """
    По списку attributes вариаций строит атрибуты продукта:
    id, name, values — все значения по вариациям по порядку.
    """
    if not variations_attrs:
        return []

    ids_order: list[str] = []
    id_to_name: dict[str, str] = {}
    for row in variations_attrs[0]:
        ids_order.append(row["id"])
        id_to_name[row["id"]] = row["name"]

    out: list[dict] = []
    for aid in ids_order:
        values: list[str] = []
        for var_rows in variations_attrs:
            for r in var_rows:
                if r["id"] == aid:
                    values.append(r["value"])
                    break
        out.append({"id": aid, "name": id_to_name[aid], "values": values})
    return out


def build_title(category_name: str, rng: random.Random) -> str:
    adj = rng.choice(
        [
            "Комфорт",
            "Премиум",
            "Уют",
            "Стиль",
            "Практик",
            "Лайт",
            "Про",
            "Домашний",
            "Сити",
            "Норд",
        ]
    )
    noun = rng.choice(
        [
            "Линия",
            "Серия",
            "Коллекция",
            "Модель",
            "Селект",
            "Гранд",
            "Эко",
            "Компакт",
        ]
    )
    code = rng.choice("ABCDEFGHJKLMNPRSTUVWXYZ") + rng.choice("23456789") + rng.choice("23456789")
    return f"{category_name} — {adj} {noun} {code}"


def build_description(category_name: str, title: str, rng: random.Random) -> str:
    phrases = [
        f"Товар категории «{category_name}» для стенда поиска.",
        "Синтетическое описание: характеристики см. в атрибутах и вариациях.",
        rng.choice(
            [
                "Подходит для типовой квартиры.",
                "Универсальное решение для дома.",
                "Сочетается с популярными интерьерами.",
            ]
        ),
    ]
    return " ".join(phrases)


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    default_attrs = root / "data" / "product_attributes.json"
    default_products_out = root / "data" / "product_catalog_products.json"
    default_variations_out = root / "data" / "product_catalog_variations.json"

    parser = argparse.ArgumentParser(
        description="Генерация P продуктов; у каждого случайное число вариаций ≈ V ±20%."
    )
    parser.add_argument("-P", "--products", type=int, required=True, help="Число продуктовых карточек.")
    parser.add_argument(
        "-V",
        "--variations",
        type=int,
        required=True,
        help="Базовое число вариаций на продукт; фактически случайно от 80%% до 120%% от этого числа.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Seed RNG для воспроизводимости.")
    parser.add_argument(
        "--attributes",
        type=Path,
        default=default_attrs,
        help=f"Путь к product_attributes.json (по умолчанию: {default_attrs})",
    )
    parser.add_argument(
        "--output-products",
        type=Path,
        default=default_products_out,
        help=f"JSON с карточками продуктов (по умолчанию: {default_products_out})",
    )
    parser.add_argument(
        "--output-variations",
        type=Path,
        default=default_variations_out,
        help=f"JSON с вариациями (по умолчанию: {default_variations_out})",
    )
    args = parser.parse_args()

    if args.products < 1:
        print("P должно быть >= 1.", file=sys.stderr)
        sys.exit(1)
    if args.variations < 1:
        print("V должно быть >= 1.", file=sys.stderr)
        sys.exit(1)

    seed = args.seed if args.seed is not None else random.randrange(1 << 30)
    rng = random.Random(seed)

    categories, all_attrs = load_attributes(args.attributes)
    if not categories:
        print("В файле атрибутов нет категорий.", file=sys.stderr)
        sys.exit(1)

    slug_to_name = {c["slug"]: c["name"] for c in categories}
    cat_slugs = [c["slug"] for c in categories]

    per_product_var_counts = [
        variation_count_for_product(args.variations, rng) for _ in range(args.products)
    ]
    total_variations = sum(per_product_var_counts)
    max_var_per_product = max(per_product_var_counts) if per_product_var_counts else 0

    prod_digits = max(6, len(str(args.products)))
    var_digits = max(7, len(str(total_variations)))

    meta = {
        "schema_version": 2,
        "source_attributes": str(args.attributes),
        "seed": seed,
        "counts": {
            "products": args.products,
            "variations_per_product_base": args.variations,
            "variations_total": total_variations,
            "variations_per_product_min": min(per_product_var_counts) if per_product_var_counts else 0,
            "variations_per_product_max": max_var_per_product,
        },
    }

    args.output_products.parent.mkdir(parents=True, exist_ok=True)
    args.output_variations.parent.mkdir(parents=True, exist_ok=True)

    sep = (",", ":")
    var_counter = 0

    def write_json_header(fp, array_key: str) -> None:
        fp.write("{\n")
        fp.write(f'  "schema_version":{json.dumps(meta["schema_version"])},\n')
        fp.write(f'  "source_attributes":{json.dumps(meta["source_attributes"], ensure_ascii=False)},\n')
        fp.write(f'  "seed":{json.dumps(meta["seed"])},\n')
        fp.write(f'  "counts":{json.dumps(meta["counts"], ensure_ascii=False)},\n')
        fp.write(f'  "{array_key}":[\n')

    def write_json_footer(fp) -> None:
        fp.write("\n  ]\n}\n")

    with open(args.output_products, "w", encoding="utf-8") as fp_p, open(
        args.output_variations, "w", encoding="utf-8"
    ) as fp_v:
        write_json_header(fp_p, "products")
        write_json_header(fp_v, "variations")
        first_p = True
        first_v = True

        for pi in range(args.products):
            slug = rng.choice(cat_slugs)
            category_name = slug_to_name[slug]
            product_id = f"prod_{pi + 1:0{prod_digits}d}"

            pool = applicable_attributes(slug, all_attrs)
            if not pool:
                print(f"Нет атрибутов для категории {slug!r}.", file=sys.stderr)
                sys.exit(1)

            attr_defs = pick_product_attributes(pool, rng)
            if not attr_defs:
                print(f"Пустой набор атрибутов для категории {slug!r}.", file=sys.stderr)
                sys.exit(1)

            n_var = per_product_var_counts[pi]
            variations_attrs: list[list[dict]] = []
            variation_records: list[dict] = []

            for vi in range(n_var):
                var_counter += 1
                variation_id = f"var_{var_counter:0{var_digits}d}"
                sku = f"{product_id}-{vi + 1:07d}"
                var_attrs = build_variation_attributes(attr_defs, rng)
                variations_attrs.append(var_attrs)
                variation_records.append(
                    {
                        "variation_id": variation_id,
                        "product_id": product_id,
                        "sku": sku,
                        "variation_index": vi + 1,
                        "attributes": var_attrs,
                    }
                )

            title = build_title(category_name, rng)
            product = {
                "product_id": product_id,
                "category_slug": slug,
                "category_name": category_name,
                "title": title,
                "description": build_description(category_name, title, rng),
                "attributes": aggregate_product_attributes(variations_attrs),
            }

            if not first_p:
                fp_p.write(",\n")
            first_p = False
            fp_p.write("    ")
            json.dump(product, fp_p, ensure_ascii=False, separators=sep)

            for variation in variation_records:
                if not first_v:
                    fp_v.write(",\n")
                first_v = False
                fp_v.write("    ")
                json.dump(variation, fp_v, ensure_ascii=False, separators=sep)

        write_json_footer(fp_p)
        write_json_footer(fp_v)

    print(
        f"Записано {args.products} карточек → {args.output_products}\n"
        f"Записано {total_variations} вариаций → {args.output_variations}\n"
        f"seed={seed}"
    )


if __name__ == "__main__":
    main()
