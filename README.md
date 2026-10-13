# Netflix Data Engineering ELT Pipeline
An end-to-end, metadata-driven data pipeline that ingests Netflix catalog data (titles, cast, directors, countries, categories) from GitHub into Azure, transforms it through a Bronze → Silver → Gold (medallion architecture), and serves it as a star schema for analytics.

Built with **Azure Data Factory** (orchestration) + **Azure Databricks Jobs** (orchestration) + **Azure Databricks** (processing) + **Azure Data Lake Storage Gen2** (storage), with **Delta Lake** at every layer.

---
## Architecture

<img width="1598" height="896" alt="image" src="https://github.com/user-attachments/assets/3907a304-4ce9-4428-95c7-a01d9969e4e7" />

### 2. Orchestration - One Databricks Workflow Job

ADF makes a single call to one Databricks job (`ELT Pipeline`), which internally runs the full medallion chain as a **task DAG**, with Serverless compute.

<img width="1972" height="465" alt="image" src="https://github.com/user-attachments/assets/b5fd134c-64ee-4f79-bb52-2d956dd80740" />

- Each task runs only after the upstream task succeeds; any failure stops the chain.
- **Email notifications** are configured on the job, the team is alerted automatically on failure.
- Serverless compute is used for the Silver transformation tasks, so no cluster management is needed and the job only pays for actual runtime.

### 3. Bronze - Incremental Ingestion with Auto Loader

`Bronze_Autoloader` incrementally reads new files from the `raw` container using **Auto Loader** (`cloudFiles` with the directory listing mode):

- **Idempotency** : each table gets its own checkpoint location (backed by RocksDB), guaranteeing every file is processed exactly once and only new files are picked up on reruns.
- **Schema handling** : the schema is inferred on first load, cached in a per-table `SchemaLocation`, and evolved on change (`mergeSchema = true`), with malformed records rescued to `_rescued_data`.
- Audit columns `_ingested_date` and `_file_modified` are added (titles table) to support downstream incremental processing.

### 4. Silver - Cleaning, Deduplication & Upsert

**`Silver_Netflix_Titles`** (SCD 1-style upsert, one row per `show_id`):

- **Watermark-based incremental loading** : only bronze rows newer than `max(_ingested_date)` in silver are processed; on the very first run the watermark comes from the `ingested_date` parameter set by Azure Data Factory.
- **Cleaning** : drop exact duplicates; cast `show_id` to int (invalid → NULL) and filter NULLs; drop `_rescued_data`; keep only valid `type` values (`Movie` / `TV Show`); cast `duration_minutes` / `duration_seasons` to int with 0-fill; derive `short_title` (text before the first `:`) and `type_flag` (1 = Movie, 2 = TV Show).
- **Deduplication** : a window over `show_id` ordered by `_ingested_date` DESC, `_file_modified` DESC keeps the newest row per show, so the MERGE never sees duplicate keys.
- **Upsert** : `MERGE INTO` with `UPDATE SET *` only when the source row is newer, `INSERT` for new shows → reruns are fully idempotent.

**`Silver_Other_Tables`** - bridge tables (`netflix_cast`, `netflix_category`, `netflix_countries`, `netflix_directors`) via a reusable `BridgeLoader` class:

- Cast `show_id` to int, drop NULL/empty values, trim trailing commas/whitespace, drop duplicates.
- **Insert-only MERGE** on `(show_id, value)`,  new pairs are inserted, existing pairs skipped → safe to reprocess the entire bronze history any number of times.

---

## Tech Stack

**Azure:** Data Factory · Data Lake Storage Gen2 · Databricks · Unity Catalog
**Databricks:** PySpark · Delta Lake · Auto Loader (`cloudFiles`) · Delta Live Tables · Workflows
**Source:** GitHub (CSV) · 
**Languages:** Python, SQL

## How It Works (TL;DR)

ADF detects new CSVs in GitHub and lands them in the `raw` zone → Databricks Auto Loader incrementally ingests them into Bronze Delta tables → Silver notebooks clean, deduplicate, and upsert into curated tables → Delta Live Tables builds the Gold star schema, ready for BI and analytics.

---
