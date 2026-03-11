# Price Monitor ETL

`price-monitor-etl` is a Python ETL starter project for tracking e-commerce product prices.

The first sprint delivers a working local foundation:

- configurable settings via YAML and environment variables
- a CLI for database setup, config inspection, and scraping
- PostgreSQL persistence for scrape runs and product snapshots
- Docker Compose for local database setup
- smoke tests using SQLite

The current implementation is intentionally narrow. It proves the project shape and data flow with a dummy scraper, while leaving real scraping, file-based ETL stages, and migrations for future sprints.

## Highlights

- Python 3.11+
- SQLAlchemy 2.x
- Psycopg 3
- Pydantic v2
- Requests
- Pytest
- PostgreSQL 16 via Docker Compose

## Current Status

Implemented:

- `pricemonitor show-config`
- `pricemonitor init-db`
- `pricemonitor scrape --source site_a`
- `scrape_runs` and `product_snapshots` tables
- local logging to `logs/pricemonitor.log`
- smoke tests covering init + scrape + config output

Not implemented yet:

- real scraper integrations beyond the dummy example
- writes to `data/raw`, `data/processed`, or `data/exports`
- scheduling, retries, or orchestration
- schema migrations

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

### 5. Initialize the schema

```powershell
pricemonitor init-db
```

### 6. Run the demo scrape

```powershell
pricemonitor scrape --source site_a
```

### 7. Run tests

```powershell
pytest
```

## Example Workflow

```powershell
pricemonitor show-config
pricemonitor init-db
pricemonitor scrape --source site_a
pytest
```

Expected scrape output:

```text
Scrape completed for site_a: fetched=2 inserted=2
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

### Run the demo scraper

```powershell
pricemonitor scrape --source site_a
```

With a limit:

```powershell
pricemonitor scrape --source site_a --limit 1
```

## Makefile Shortcuts

```powershell
make install
make install-dev
make show-config
make init-db
make scrape
make test
```

If `make` is not available on your machine, run the underlying commands directly.

## Project Structure

```text
price-monitor-etl/
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
|       |-- scrapers/
|       `-- storage/
|-- tests/
|   `-- test_smoke.py
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

## Verifying the Database

After a scrape, inspect the database with:

```powershell
docker exec -it price-monitor-postgres psql -U postgres -d price_monitor
```

Then run:

```sql
SELECT COUNT(*) FROM scrape_runs;
SELECT COUNT(*) FROM product_snapshots;

SELECT id, source_name, status, records_fetched, records_inserted
FROM scrape_runs
ORDER BY id DESC
LIMIT 5;
```

## Testing

Run:

```powershell
pytest
```

The smoke tests use a temporary SQLite database, so they stay fast and do not depend on Docker.

## Source Configuration

Each source is defined under `configs/sources/`.

`site_a`:

- enabled
- backed by the `dummy_site_a` scraper
- includes sample products for local validation

`site_b`:

- disabled
- placeholder for a future source implementation

## Logging and Output

Logs are written to:

```text
logs/pricemonitor.log
```

The current sprint stores scrape results in PostgreSQL only. Empty `data/raw`, `data/processed`, and `data/exports` directories are expected for now.

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

### Local PostgreSQL already uses `5432`

This repository intentionally maps Docker PostgreSQL to `5433`.

## Next Sprint Candidates

- replace the dummy scraper with a real source integration
- add raw and processed filesystem outputs
- export snapshots to CSV or Parquet
- introduce Alembic migrations
- expand repository and scraper test coverage
- improve failure handling and observability

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

## Author

Georgios Panagiotopoulos 2026
