# Price Monitor ETL

`price-monitor-etl` is a Python ETL starter project for monitoring e-commerce product prices over time.

The first eleven sprints now deliver a working local foundation, two real scrapers, a normalization and validation layer, browser automation support, change detection, Alembic-based schema migrations, a processed data layer, business-facing exports, and pipeline-style orchestration.

The project now has a clear three-layer data flow:

- `data/raw/` for audit and debugging artifacts
- PostgreSQL for operational source-of-truth records
- `data/processed/` for curated analytics-ready datasets
- `data/exports/` for client-facing CSV and JSON outputs

## Highlights

- Python 3.11+
- SQLAlchemy 2.x
- Alembic
- Psycopg 3
- Pydantic v2
- Requests
- Beautiful Soup 4
- Playwright
- Pytest
- PostgreSQL 16 via Docker Compose

## Current Status

Implemented:

- `pricemonitor show-config`
- `pricemonitor init-db`
- `pricemonitor scrape --source site_a`
- `pricemonitor scrape --source site_b`
- `pricemonitor scrape --source all`
- `pricemonitor process --source <name>`
- `pricemonitor export --source <name>`
- `pricemonitor run --source <name>`
- real static-site scraper for `site_a` using Books to Scrape
- browser-rendered scraper support using Playwright
- browser fetcher and HTTP fetcher selected per source configuration
- real browser-rendered scraper for `site_b` using the Web Scraper AJAX test e-commerce site
- shared scrape, validation, repository, and archive pipeline across both static and dynamic sources
- normalization of scraped text, prices, URLs, and availability values
- validation rules for missing names, missing URLs, and invalid prices
- tracking of valid vs invalid scraped record counts
- repository helpers for scrape-run lifecycle updates and snapshot insertion
- repository helpers for previous-run lookup and price-change event insertion
- raw HTML archive support under `data/raw/`
- change detection between the current run and the previous successful run for the same source
- absolute and percentage price-difference calculation
- Alembic migration environment wired to the app's config and SQLAlchemy metadata
- a committed baseline revision for the current schema
- `pricemonitor init-db` applying Alembic migrations instead of relying on `create_all()`
- processed latest product, price change, and run summary datasets under `data/processed/`
- processed enrichment fields such as `effective_price`, `is_discounted`, `availability_group`, `change_direction`, `change_bucket`, `change_magnitude`, `duration_seconds`, `insert_rate`, and `validity_rate`
- business-facing CSV and JSON exports under `data/exports/`
- exports now built from the processed data layer rather than directly from the database
- dedicated pipeline modules under `src/pricemonitor/pipelines/`
- clearer multi-source orchestration and per-source lifecycle handling
- end-to-end `run` orchestration that performs scrape, process, then export
- `scrape_runs`, `product_snapshots`, and `price_change_events` tables
- local logging to `logs/pricemonitor.log`
- smoke tests and unit tests covering scrape, normalization, validation, fetcher selection, repository behavior, change detection, processed datasets, exports, pipeline orchestration, and config output

Not implemented yet:

- Parquet or Excel outputs
- scheduling, retries, or background orchestration

## Quickstart

### 1. Create and activate an environment

Example with Conda:

```powershell
conda create -n price-monitor-etl python=3.11 -y
conda activate price-monitor-etl
```

### 2. Install the project

```powershell
pip install -e .[dev]
```

Install Chromium for Playwright:

```powershell
playwright install chromium
```

### 3. Create `.env`

Use `.env.example` as the template:

```env
APP_ENV=development
LOG_LEVEL=INFO
DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5433/price_monitor
```

### 4. Start PostgreSQL

```powershell
docker compose up -d db
```

If you want a clean database:

```powershell
docker compose down -v
docker compose up -d db
```

### 5. Apply database migrations

```powershell
pricemonitor init-db
```

`pricemonitor init-db` runs Alembic `upgrade head` against the configured database.

If you already have a local database that was created before Alembic was introduced and it already matches the committed baseline schema, do not replay the baseline migration on top of those existing tables. Stamp it once instead:

```powershell
make stamp-head
```

### 6. Run a real scrape

```powershell
pricemonitor scrape --source site_a --limit 5
```

Or run both enabled sources:

```powershell
pricemonitor scrape --source all --limit 2
```

### 7. Build processed datasets

```powershell
pricemonitor process --source site_a
```

Or process all enabled sources:

```powershell
pricemonitor process --source all --limit 50
```

### 8. Generate business-facing exports

```powershell
pricemonitor export --source site_a
```

Or export all enabled sources:

```powershell
pricemonitor export --source all --limit 50
```

### 9. Run the full end-to-end pipeline

```powershell
pricemonitor run --source site_a --limit 5 --report-limit 50
```

Or run the full pipeline for all enabled sources:

```powershell
pricemonitor run --source all --limit 2 --report-limit 25
```

### 10. Run tests

```powershell
pytest
```

## Example Workflow

```powershell
pricemonitor show-config
pricemonitor init-db
pricemonitor scrape --source all --limit 2
pricemonitor process --source all --limit 50
pricemonitor export --source all --limit 50
pricemonitor run --source all --limit 2 --report-limit 25
pytest
```

Expected scrape output:

```text
Scrape completed for site_a: fetched=5 valid=5 invalid=0 inserted=5 archived=6 changes=0
Scrape completed for site_b: fetched=5 valid=5 invalid=0 inserted=5 archived=1 changes=0
```

On a later run, `changes` becomes non-zero only when a product price differs from the previous successful run for that same source.

Expected process output:

```text
Process completed for site_a: latest_products=5 price_changes=2 run_summary=6 dir=...\data\processed\site_a
Process completed for site_b: latest_products=6 price_changes=0 run_summary=4 dir=...\data\processed\site_b
```

Expected export output:

```text
Export completed for site_a: latest_products=5 price_changes=2 run_summary=6 dir=...\data\exports\site_a
Export completed for site_b: latest_products=6 price_changes=0 run_summary=4 dir=...\data\exports\site_b
```

Expected pipeline output:

```text
Scrape completed for site_a: fetched=2 valid=2 invalid=0 inserted=2 archived=3 changes=0
Process completed for site_a: latest_products=2 price_changes=0 run_summary=3 dir=...\data\processed\site_a
Export completed for site_a: latest_products=2 price_changes=0 run_summary=3 dir=...\data\exports\site_a
Pipeline run completed for site_a.
```

## Configuration

The application resolves settings in this order:

1. OS environment variables
2. `.env` in the repository root
3. `configs/settings.yaml`
4. hardcoded defaults in `src/pricemonitor/config.py`

Important:

- `.env.example` is documentation only
- `.env.example` is not loaded automatically

Default local database URL:

```env
DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5433/price_monitor
```

Port `5433` is used to avoid collisions with machines that already have PostgreSQL running on `5432`.

## CLI Commands

### Show resolved configuration

```powershell
pricemonitor show-config
```

### Initialize the database

```powershell
pricemonitor init-db
```

### Run one scraper

```powershell
pricemonitor scrape --source site_a
```

With a limit:

```powershell
pricemonitor scrape --source site_a --limit 1
```

Run the second scraper:

```powershell
pricemonitor scrape --source site_b --limit 5
```

Run all enabled scrapers:

```powershell
pricemonitor scrape --source all --limit 2
```

### Build processed datasets for one source

```powershell
pricemonitor process --source site_a
```

Limit recent rows included in processed `price_changes` and `run_summary`:

```powershell
pricemonitor process --source site_a --limit 25
```

Process all enabled sources:

```powershell
pricemonitor process --source all --limit 50
```

### Export one source

```powershell
pricemonitor export --source site_a
```

Limit recent rows included in exported `price_changes` and `run_summary`:

```powershell
pricemonitor export --source site_a --limit 25
```

Export all enabled sources:

```powershell
pricemonitor export --source all --limit 50
```

### Run the full pipeline for one source

```powershell
pricemonitor run --source site_a --limit 5 --report-limit 50
```

### Run the full pipeline for all enabled sources

```powershell
pricemonitor run --source all --limit 2 --report-limit 25
```

## Makefile Shortcuts

```powershell
make install
make install-dev
make show-config
make init-db
make scrape
make process
make export
make run
make test
make migrate
make revision message="add new column"
make downgrade
make history
make stamp-head
```

If `make` is not available on your machine, run the underlying commands directly.

## Database Migrations

Alembic is now the source of truth for schema evolution.

Rules going forward:

- use `pricemonitor init-db` or `make migrate` to apply schema changes
- use `make revision message="..."` after changing SQLAlchemy models
- review every autogenerated migration manually before committing it
- do not use `Base.metadata.create_all()` for normal development or production schema changes
- keep direct metadata-based schema creation only for isolated tests

Typical workflow for a model change:

1. Update the ORM models.
2. Run `make revision message="describe the change"`.
3. Review and, if needed, edit the generated migration file.
4. Run `make migrate`.
5. Run the test suite.

Existing pre-Alembic databases:

- if the database was already created before Sprint 8 and already matches the current schema, use `make stamp-head` once
- after that, use normal Alembic revisions and upgrades

## Processed Data Layer

Sprint 11 adds a real processed layer under `data/processed/<source>/`.

Each process run regenerates:

- `latest_products_processed.csv`
- `latest_products_processed.json`
- `price_changes_processed.csv`
- `price_changes_processed.json`
- `run_summary_processed.csv`
- `run_summary_processed.json`

Dataset meaning:

- `latest_products_processed` contains the latest known snapshot per product plus analytics-oriented enrichment fields such as `effective_price`, `is_discounted`, `availability_group`, and `scrape_date`
- `price_changes_processed` contains recent change events plus enrichment fields such as `change_direction`, `change_bucket`, `change_magnitude`, `is_price_increase`, `is_price_decrease`, and `changed_date`
- `run_summary_processed` contains recent run outcomes enriched with `success_flag`, `duration_seconds`, `insert_rate`, and `validity_rate`

Important:

- `process` reads from the database only; it does not scrape new data
- processed files are overwritten on each run so they always reflect the latest stored state
- `--limit` applies to processed `price_changes` and `run_summary`; `latest_products_processed` always represents the current catalog for that source

## Exports And Reporting

The export layer now reads curated processed datasets and writes client-friendly files under `data/exports/<source>/`.

Each export run regenerates:

- `latest_products.csv`
- `latest_products.json`
- `price_changes.csv`
- `price_changes.json`
- `run_summary.csv`
- `run_summary.json`

Dataset meaning:

- `latest_products` is the client-facing projection of the processed latest catalog dataset
- `price_changes` is the client-facing projection of recent processed price-change rows
- `run_summary` is the client-facing projection of recent processed run summaries

Important:

- `export` no longer scrapes data and no longer reads the database directly
- `export` expects processed datasets to exist first
- if processed files are missing, run `pricemonitor process --source <name>` or `pricemonitor run --source <name>` first
- export files are overwritten on each run so they always reflect the latest processed state
- `--limit` applies to exported `price_changes` and `run_summary`; `latest_products` always exports the full current processed catalog for that source

## Pipeline Flow

Sprint 10 introduced the orchestration layer under `src/pricemonitor/pipelines/`, and Sprint 11 extends it with a dedicated process stage.

Pipeline modules:

- `common.py` contains shared helpers such as source resolution and DB error formatting
- `scrape_run.py` owns the scrape pipeline for one source or many sources
- `process_run.py` owns the processed-data pipeline for one source or many sources
- `report_run.py` owns the reporting/export pipeline for one source or many sources

The `run` command now composes all three stages:

1. scrape the source
2. persist snapshots and change events
3. build processed datasets
4. build client-facing exports

Important behavior:

- each source is handled independently inside multi-source orchestration
- failures are isolated per source during `--source all`
- a failure in one source does not prevent other enabled sources from being attempted
- the end-to-end `run` command performs scrape first, then process, then export, for each source in sequence

## Project Structure

```text
price-monitor-etl/
|-- alembic/
|   |-- env.py
|   |-- script.py.mako
|   `-- versions/
|       `-- 20260320_0001_baseline_sprint_7_schema.py
|-- alembic.ini
|-- configs/
|   |-- settings.yaml
|   `-- sources/
|       |-- site_a.yaml
|       `-- site_b.yaml
|-- data/
|   |-- exports/
|   |-- processed/
|   `-- raw/
|-- logs/
|-- src/
|   `-- pricemonitor/
|       |-- config.py
|       |-- logging_config.py
|       |-- main.py
|       |-- fetchers/
|       |-- models/
|       |-- parsers/
|       |-- pipelines/
|       |   |-- common.py
|       |   |-- process_run.py
|       |   |-- report_run.py
|       |   `-- scrape_run.py
|       |-- scrapers/
|       |-- services/
|       |   |-- change_detection.py
|       |   |-- export.py
|       |   `-- process.py
|       `-- storage/
|-- tests/
|   |-- test_change_detection.py
|   |-- test_export.py
|   |-- test_fetcher_factory.py
|   |-- test_pipeline.py
|   |-- test_process.py
|   |-- test_smoke.py
|   |-- test_storage_repositories.py
|   `-- test_validation.py
|-- .env.example
|-- docker-compose.yaml
|-- Makefile
`-- pyproject.toml
```

## Data Model

### `scrape_runs`

Tracks each scrape execution:

- source name
- run status
- start and finish timestamps
- fetched and inserted record counts
- optional error message

### `product_snapshots`

Stores point-in-time product observations:

- source and external product id
- product metadata
- pricing and availability
- scrape timestamp
- raw payload copy for traceability
- parent scrape run id

### `price_change_events`

Stores detected price movements between consecutive successful runs for the same source:

- source and external product id
- previous and current snapshot ids
- previous and current monitored prices
- absolute and percentage difference
- change timestamp
- parent scrape run id

## Verifying the Database

After a scrape, inspect the database with:

```powershell
docker exec -it price-monitor-postgres psql -U postgres -d price_monitor
```

Then run:

```sql
SELECT COUNT(*) FROM scrape_runs;
SELECT COUNT(*) FROM product_snapshots;
SELECT COUNT(*) FROM price_change_events;
SELECT version_num FROM alembic_version;

SELECT id, source_name, status, records_fetched, records_inserted
FROM scrape_runs
ORDER BY id DESC
LIMIT 5;

SELECT id, source_name, external_id, previous_price, current_price, absolute_difference, percentage_difference
FROM price_change_events
ORDER BY id DESC
LIMIT 10;
```

## Testing

Run:

```powershell
pytest
```

The smoke tests use a temporary SQLite database, so they stay fast and do not depend on Docker.
They stub both source HTML responses, so tests stay deterministic and do not depend on network access.
Additional unit tests cover normalization helpers and validation rules directly.
Repository tests cover scrape-run lifecycle helpers, snapshot insertion, raw-page archive output, and price-change event persistence.
Fetcher tests cover HTTP vs browser fetcher selection.
Change-detection tests cover changed-price, unchanged-price, and new-product scenarios directly.
Process tests cover processed CSV/JSON generation and the `process` CLI flow directly.
Export tests cover client-facing CSV/JSON generation from processed inputs and the export CLI flow directly.
Pipeline tests cover end-to-end orchestration across multiple sources and all three pipeline stages.
Alembic remains the normal migration path, while direct metadata-based schema creation stays limited to isolated test setup.

## Source Configuration

Each source is defined under `configs/sources/`.

`site_a`:

- enabled
- backed by the real `site_a` scraper
- targets `https://books.toscrape.com/`
- fetches listing pages and product detail pages
- extracts product name, category, pricing, availability, product URL, and image URL
- normalizes messy values before creating `ProductRecord` objects
- rejects invalid records while continuing the scrape
- archives fetched listing and detail HTML pages for each run
- compares each successful run to the previous successful `site_a` run and records any price changes

`site_b`:

- enabled
- backed by the real `site_b` scraper
- targets `https://webscraper.io/test-sites/e-commerce/ajax/computers/laptops`
- uses the Playwright-backed browser fetcher to parse rendered HTML
- reuses the same validation, repository, and raw-archive pipeline as `site_a`
- extracts product name, category, price, URL, and image URL from a different card layout
- archives the source listing HTML for each run
- participates in the same per-source change-detection flow as `site_a`

## Logging and Output

Logs are written to:

```text
logs/pricemonitor.log
```

Scrape metadata, snapshots, and detected price changes are persisted in PostgreSQL.
`data/raw` is populated after scrapes, `data/processed` is populated after process runs, and `data/exports` is populated after export runs.
The `product_snapshots.payload` field keeps the full scraped record, including fields such as `image_url` that are not yet modeled as dedicated columns.
The raw page archive is written under `data/raw/<source>/run_<id>/` with HTML files and a `manifest.json`.
The CLI output and logs report fetched, valid, invalid, inserted, archived, and detected-change counts for each scrape.
The process stage writes curated CSV and JSON datasets for latest products, price changes, and run summaries.
The export stage writes smaller client-facing CSV and JSON datasets derived from the processed layer.
When you use `--source all`, the orchestration runs each enabled source in sequence and prints a final summary line after all of them complete.
The first successful run for a source creates the monitoring baseline, so `changes=0` is expected until a later run sees a different price.
Change detection compares the current run to the previous successful run for the same source and records events in `price_change_events`.
Schema changes are tracked through Alembic revisions, and the `alembic_version` table records the currently applied revision.

## Common Migration Cases

### Brand-new database

```powershell
pricemonitor init-db
```

or:

```powershell
make migrate
```

### Existing database from before Alembic

If the database already matches the committed baseline schema, mark it once instead of replaying the baseline migration on top of existing tables:

```powershell
make stamp-head
```

### New schema change

```powershell
make revision message="add useful column"
make migrate
```

Always review autogenerated migrations before committing them.

## Common Process Cases

### Process one source

```powershell
pricemonitor process --source site_a
```

### Process all enabled sources

```powershell
pricemonitor process --source all --limit 50
```

### Check generated processed files

```text
data/processed/site_a/latest_products_processed.csv
data/processed/site_a/price_changes_processed.csv
data/processed/site_a/run_summary_processed.csv
```

## Common Export Cases

### Export one source

```powershell
pricemonitor export --source site_a
```

### Export all enabled sources

```powershell
pricemonitor export --source all --limit 50
```

### If processed files are missing

```powershell
pricemonitor process --source site_a
```

or:

```powershell
pricemonitor run --source site_a
```

### Check generated export files

```text
data/exports/site_a/latest_products.csv
data/exports/site_a/price_changes.csv
data/exports/site_a/run_summary.csv
```

## Common Pipeline Cases

### Run scrape, process, and export for one source

```powershell
pricemonitor run --source site_a --limit 5 --report-limit 50
```

### Run scrape, process, and export for all enabled sources

```powershell
pricemonitor run --source all --limit 2 --report-limit 25
```

## Common Issues

### PostgreSQL authentication failure

Check the effective config first:

```powershell
pricemonitor show-config
```

Then verify:

- `.env` and `configs/settings.yaml` agree
- the app is connecting to `localhost:5433`
- the Docker volume was not initialized with old credentials

If needed:

```powershell
docker compose down -v
docker compose up -d db
```

### Missing processed datasets during export

The export layer now depends on files under `data/processed/<source>/`.

Run one of:

```powershell
pricemonitor process --source site_a
pricemonitor run --source site_a
```

### Local PostgreSQL already uses `5432`

This repository intentionally maps Docker PostgreSQL to `5433`.

## Next Sprint Candidates

- add richer processed or export formats such as Parquet or Excel
- add retries, scheduling, and background execution
- add browser-specific wait strategies and richer page interaction helpers
- expand repository and scraper query helpers further
- add notifications or alerting for detected price changes
- improve failure handling and observability

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

## Author

Georgios Panagiotopoulos 2026
