# sonaka-search

Стенд для тестирования поиска интернет-магазина в духе ассортимента «мебель, сон, бытовая техника».

## Каталог атрибутов

Сгенерировано **300 атрибутов товаров**, у каждого **от 5 до 10** допустимых значений. Часть полей сквозные для всех категорий, часть — только для своей категории.

### Структура репозитория (генератор по частям)

| Файл | Назначение |
|------|------------|
| `scripts/catalog_common.py` | Хелперы `a_cat()` и `a_global()` (без циклических импортов). |
| `scripts/catalog_parts.py` | 18 категорий в JSON, **30 сквозных** атрибутов (`categories: ["*"]`), блоки «матрасы», «кровати», «основания», функция `build_all_attributes()`. |
| `scripts/catalog_parts_more.py` | **15 категорий × 15 атрибутов** (мебель, сон, текстиль, холодильники, стиральные машины). |
| `scripts/generate_product_attributes.py` | Точка входа: проверка ровно **300** уникальных имён, **5–10** значений, отсутствие дубликатов в `values`, запись результата. |
| `scripts/generate_product_cards.py` | **P** карточек и **V** вариаций → `product_catalog_products.json` и `product_catalog_variations.json`. |
| `scripts/generate_elastic_variations_index.py` | Маппинг `elastic/variations.index.json` и `variation_attribute_field_map.json` из `product_attributes.json`. |
| `scripts/load_elastic_indices.py` | Создание индексов продуктов и вариаций по `elastic/*.index.json` (без документов). |
| `scripts/load_elastic_products.py` | `_bulk` загрузка `data/product_catalog_products.json`. |
| `scripts/load_elastic_variations.py` | `_bulk` загрузка вариаций с плоскими полями по карте полей. |
| `scripts/count_elastic_documents.py` | Запрос `/_count`: сколько документов физически в индексах продуктов и вариаций. |
| `data/product_attributes.json` | Итоговый каталог атрибутов. |

Сумма: **30 + 45 + 225 = 300** атрибутов. Имена части полей сделаны контекстными (например, «цвет корпуса шкафа» / «цвет корпуса холодильника»), чтобы при плоском списке не пересекались разные смыслы.

### Формат элемента атрибута в JSON

- `id` — стабильный идентификатор (`attr_001` … `attr_300`).
- `name` — название на русском.
- `categories` — `["*"]` для глобальных атрибутов или один slug категории из таксономии.
- `values` — массив из 5–10 строк (варианты значения).

В корне JSON также есть `categories` (slug и человекочитаемое имя), `schema_version`, `domain`, `reference_style`.

## Пересборка данных

Зависимости не нужны, достаточно Python 3:

```bash
cd scripts && python3 generate_product_attributes.py
```

Файл перезаписывается: `data/product_attributes.json`.

После изменения атрибутов пересоберите индекс вариаций в Elasticsearch:

```bash
python3 scripts/generate_elastic_variations_index.py
```

## Загрузка в Elasticsearch

Порядок: сгенерировать `elastic/variations.index.json` и карту полей (`python3 scripts/generate_elastic_variations_index.py`), затем:

```bash
# только создание индексов (маппинги)
python3 scripts/load_elastic_indices.py --recreate --relax-disk-watermarks

# отдельно загрузка документов
python3 scripts/load_elastic_products.py
python3 scripts/load_elastic_variations.py
```

Проверить, сколько документов реально лежит в кластере:

```bash
python3 scripts/count_elastic_documents.py
# или JSON: python3 scripts/count_elastic_documents.py --json
```

У загрузчиков: `--es-url`, `--index`, `--file` (путь к JSON), для вариаций ещё карта полей `elastic/variation_attribute_field_map.json`. У `count_elastic_documents.py`: `--es-url`, `--products-index`, `--variations-index`, `--json`.

## Индексы Elasticsearch

- `elastic/products.index.json` — карточки продуктов (вручную; вложенные `attributes` при необходимости).
- `elastic/variations.index.json` — вариации: ядро документа + **поле `keyword` на каждый атрибут**; имена полей — **транслит названий** из каталога (например `brend`, `osnovnoy_tsvet_izdeliya`), а не `attr_001`. Поле **`search_all`** (`text`) заполняйте склейкой значений для полнотекста.
- `elastic/variation_attribute_field_map.json` — соответствие **`attr_XXX` → имя поля** в индексе (нужно при загрузке из JSON каталога, где в документе ещё используются id атрибутов).

### Структура файлов в `elastic/`

Все три файла — JSON. Индексные шаблоны (`*.index.json`) содержат объект верхнего уровня с **`settings.index`** и **`mappings.properties`**, как у тела запроса `PUT /<index>` в Elasticsearch.

| Файл | Назначение и поля |
|------|-------------------|
| **`products.index.json`** | **`settings`**: 1 шард, 0 реплик, `refresh_interval` 1s. **`mappings`**: `product_id`, `category_slug` — `keyword`; `category_name`, `title` — `text` + подполе `.keyword` (`ignore_above` 256/512); `description` — `text`; `attributes` — **nested** с `id` (`keyword`), `name` (`text` + `.keyword`), `values` — `keyword` (мультизначение). |
| **`variations.index.json`** | Те же **`settings`**. **`mappings`**: идентификаторы `variation_id`, `product_id`, `sku` — `keyword`; `variation_index` — `integer`; `search_all` — `text` с анализатором `standard`; далее **по одному полю на атрибут каталога** — везде `keyword` с `ignore_above: 256` (имена полей — транслит, см. карту). |
| **`variation_attribute_field_map.json`** | Служебный справочник, не маппинг ES: `schema_version`, `source_attributes`, `description`, объект **`fields`** — пары `"attr_XXX": "imya_polya_v_indekse"` для всех атрибутов из `product_attributes.json`. |

Скрипты создания индексов подставляют эти JSON как тело запроса; фактические имена индексов задаются в загрузчиках (см. `scripts/load_elastic_*.py`).

## Каталог товаров и вариаций

Скрипт `scripts/generate_product_cards.py` читает `data/product_attributes.json` и создаёт **P** продуктовых карточек и **V** вариаций **всего** по магазину (у каждой карточки не меньше одной вариации, поэтому нужно **V ≥ P**). Лишние вариации случайно распределяются по карточкам.

```bash
python3 scripts/generate_product_cards.py -P 200 -V 500 --seed 42
```

Пути к файлам задаются опционально: `--output-products`, `--output-variations` (по умолчанию `data/product_catalog_products.json` и `data/product_catalog_variations.json`). В обоих файлах дублируются метаданные: `schema_version`, `source_attributes`, `seed`, `counts`.

- **Карточка** (файл продуктов): категория, заголовок, описание, набор общих атрибутов (одинаковых для всех вариаций этого товара).
- **Вариация** (файл вариаций): `sku`, ссылка на `product_id`, полный список атрибутов (общие + варьируемые со случайными значениями из каталога).

Параметр `--seed` фиксирует воспроизводимость.

### Объёмы данных и параметры теста (пример)

Ориентир после генерации `-P 1000 -V 100` и загрузки в кластер. Актуальные числа документов: `python3 scripts/count_elastic_documents.py`.

| Показатель | Значение |
|------------|----------|
| Продуктов в Elasticsearch (`sonaka_products`, `GET …/_count`) | 1000 |
| Вариаций в Elasticsearch (`sonaka_variations`, `GET …/_count`) | 98831 |
| Атрибутов в запросе теста (число `term` в `bool.filter`) | **3–7** (случайный поднабор полей одной вариации) |
| Время **только** `_search` к Elasticsearch в тесте | См. **stderr**: `[test_variations_search] Elasticsearch _search: … s` при `python3 -m unittest tests.test_variations_search -v`. При ~100k вариаций на стенде часто **~0.02 с**; полный прогон теста больше из‑за чтения большого `product_catalog_variations.json`. |

## Тест поиска по вариациям

Файл `tests/test_variations_search.py` проверяет, что **поиск по индексу вариаций в Elasticsearch** ведёт себя ожидаемо для случайного, но **детерминированного** сценария (фиксированный seed).

Что делает тест:

1. Берёт из `data/product_catalog_variations.json` случайную вариацию и **3–7** её атрибутов, у которых есть маппинг в `elastic/variation_attribute_field_map.json` (те же правила, что в модуле теста).
2. Строит запрос `bool.filter` из `term` по полям ES и значениям этой вариации — документ **обязан** совпасть, потому что фильтры — его собственные значения.
3. Выполняет `_search` с **`collapse` по `product_id`** и проверяет: ответ 200, есть совпадения, в `hits` нет дубликатов `product_id`, и **ожидаемый `product_id`** выбранной вариации присутствует среди коллапсированных результатов.

Тест **пропускается**, если кластер недоступен (`GET` к корню ES не даёт 200). Переменные окружения: `ELASTICSEARCH_URL` (по умолчанию `http://127.0.0.1:9200`), `ELASTICSEARCH_VARIATIONS_INDEX` (`sonaka_variations`), `TEST_VARIATIONS_SEARCH_SEED` (по умолчанию `42`).

```bash
python3 -m unittest tests.test_variations_search -v
```

## Скрипт `explain_variation_search_case.py`

Скрипт `scripts/explain_variation_search_case.py` **расшифровывает тот же кейс**, что строит тест: печатает `seed`, `variation_id`, `product_id` и список условий **в виде «поле ES ↔ человекочитаемое имя атрибута и значение из каталога»**, чтобы было понятно, *что именно* имитировал бы тест, без чтения только транслитерированных имён полей Elasticsearch.

Логика выбора вариации и набора `term`-фильтров совпадает с тестом; seed задаётся через `TEST_VARIATIONS_SEARCH_SEED` или флаг `--seed`.

```bash
python3 scripts/explain_variation_search_case.py
TEST_VARIATIONS_SEARCH_SEED=1 python3 scripts/explain_variation_search_case.py
```

В конце скрипт напоминает, что ожидаемый `product_id` должен попасть в ответ на тот же запрос с `collapse` по `product_id`.

---

Если контейнер падает с ошибкой про virtual memory (часто на Linux), на хосте один раз:

```bash
sudo sysctl -w vm.max_map_count=262144
```
