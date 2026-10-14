# Netflix Data Engineering ELT Pipeline

![Azure Data Factory](https://img.shields.io/badge/orchestration-Azure_Data_Factory-0078D4)
![Databricks](https://img.shields.io/badge/processing-Azure_Databricks-FF3621)
![Storage](https://img.shields.io/badge/storage-ADLS_Gen2-0078D4)
![Delta Lake](https://img.shields.io/badge/format-Delta_Lake-00ADD8)
![PySpark](https://img.shields.io/badge/language-PySpark-E25A1C)
![SQL](https://img.shields.io/badge/language-SQL-4479A1)

An end-to-end, metadata-driven data pipeline that ingests Netflix catalog data (titles, cast, directors, countries, categories) from GitHub into Azure, transforms it through a Bronze → Silver → Gold (medallion architecture), and serves it as a star schema for analytics.

## Tech Stack

**Azure:** Data Factory · Data Lake Storage Gen2 · Databricks · Unity Catalog

**Databricks:** PySpark · Delta Lake · Auto Loader (`cloudFiles`) · Declarative Pipelines · Workflows · LakeFlow Jobs

**Source:** GitHub (CSV)

**Languages:** Python, SQL

## Architecture
<p align="center">
  <img width="1598" height="896" alt="End-to-end architecture: GitHub CSVs to ADLS Gen2 raw zone, Databricks Bronze, Silver and Gold layers, star schema for analytics" src="https://github.com/user-attachments/assets/3907a304-4ce9-4428-95c7-a01d9969e4e7" />
  <br>
  <em>Figure 1: End-to-end architecture, from GitHub source files through ADF and Databricks to the Gold star schema</em>
</p>

## Pipeline Design & Implementation 
### 1. Orchestration - One Databricks Workflow Job

ADF makes a single call to one Databricks job (`ELT Pipeline`), which internally runs the full medallion chain as a **task DAG** with Serverless compute.

<p align="center">
  <img width="1066" height="270" alt="Azure Data Factory pipeline that triggers the Databricks ELT Pipeline job" src="https://github.com/user-attachments/assets/09177e2d-cd6e-4099-b5de-ba08746ca12e" />
  <br>
  <em>Figure 2: Azure Data Factory pipeline that triggers the Databricks job</em>
</p>
<p align="center">
  <img width="1972" height="465" alt="Databricks Workflow job showing the task DAG for the medallion pipeline" src="https://github.com/user-attachments/assets/b5fd134c-64ee-4f79-bb52-2d956dd80740" />
  <br>
  <em>Figure 3: Databricks Workflow job (ELT Pipeline) triggered by ADF, running the medallion chain as a task DAG</em>
</p>

- Each task runs only after the upstream task succeeds; any failure stops the chain.
- **Email notifications** are configured on the job, the team is alerted automatically on failure.
- Serverless compute is used for the Silver transformation tasks, so no cluster management is needed and the job only pays for actual runtime.

### 2. Bronze - Incremental Ingestion with Auto Loader

`Bronze_Autoloader` incrementally reads new files from the `raw` container using **Auto Loader** (`cloudFiles` with the directory listing mode):

- **Idempotency** : each table gets its own checkpoint location (backed by RocksDB), guaranteeing every file is processed exactly once and only new files are picked up on reruns.
- **Schema handling** : the schema is inferred on first load, cached in a per-table `SchemaLocation`, and evolved on change (`mergeSchema = true`), with malformed records rescued to `_rescued_data`.
- Audit columns `_ingested_date` and `_file_modified` are added (titles table) to support downstream incremental processing.

### 3. Silver - Cleaning, Deduplication & Upsert

**`Silver_Netflix_Titles`** (SCD 1-style upsert, one row per `show_id`):

- **Watermark-based incremental loading** : only bronze rows newer than `max(_ingested_date)` in silver are processed; on the very first run the watermark comes from the `ingested_date` parameter set by Azure Data Factory.
- **Cleaning** : drop exact duplicates; cast `show_id` to int (invalid → NULL) and filter NULLs; drop `_rescued_data`; keep only valid `type` values (`Movie` / `TV Show`); cast `duration_minutes` / `duration_seasons` to int with 0-fill; derive `short_title` (text before the first `:`) and `type_flag` (1 = Movie, 2 = TV Show).
- **Deduplication** : a window over `show_id` ordered by `_ingested_date` DESC, `_file_modified` DESC keeps the newest row per show, so the MERGE never sees duplicate keys.
- **Upsert** : `MERGE INTO` with `UPDATE SET *` only when the source row is newer, `INSERT` for new shows → reruns are fully idempotent.

**`Silver_Other_Tables`** - bridge tables (`netflix_cast`, `netflix_category`, `netflix_countries`, `netflix_directors`) via a reusable `BridgeLoader` class:

- Cast `show_id` to int, drop NULL/empty values, trim trailing commas/whitespace, drop duplicates.
- **Insert-only MERGE** on `(show_id, value)`,  new pairs are inserted, existing pairs skipped → safe to reprocess the entire bronze history any number of times.

### 4. Gold - Star Schema (Spark Declarative Pipelines)

The Gold layer reshapes the cleaned Silver data into a **star schema with bridge tables**, implemented as **PySpark Declarative Pipelines**, pipeline-managed materialized views that Databricks keeps fresh and validated automatically.

**Surrogate keys** : every dimension gets a deterministic surrogate key via `xxhash64()` over its natural value, so keys are stable across refreshes.

**Dimensions**

| Table | Source | Notes |
| --- | --- | --- |
| `dim_show` | `silver.netflix_titles` | show_id, title, short_title, description |
| `dim_date` | `silver.netflix_titles` | calendar dimension parsed from `date_added`, smart-key `yyyyMMdd` int, plus day / month / month_name / quarter / year |
| `dim_category` | `silver.netflix_category` | distinct trimmed categories |
| `dim_director` | `silver.netflix_directors` | distinct trimmed directors |
| `dim_cast` | `silver.netflix_cast` | distinct trimmed cast members |
| `dim_country` | `silver.netflix_countries` | distinct trimmed countries |

**Bridge tables** : a show can have many cast members, directors, countries and categories (true many-to-many), so the fact connects to those dimensions through dedicated bridge views (`bridge_show_cast`, `bridge_show_director`, `bridge_show_country`, `bridge_show_category`) that resolve `show_id → surrogate key`.

**Fact table** : `fact_show`: one row per show with the `DimDateKey` foreign key, type, rating, release_year, and durations.

## Security - No Stored Secrets

There are **no credentials stored anywhere in this pipeline** - no storage keys, no passwords, no tokens in code, in pipeline configs, or in Git.

- Databricks reaches ADLS through the **Databricks Access Connector for Azure Storage**, which authenticates with its **Microsoft Entra ID managed identity**. Unity Catalog routes every `abfss://` read/write through the connector, so notebooks need no explicit credentials.
- The connector's identity is granted **RBAC** (`Storage Blob Data Contributor`) on the storage account - access is governed by identity, not by shared secrets.

## How It Works (TL;DR)

ADF detects new CSVs in GitHub and lands them in the `raw` zone → Databricks Auto Loader incrementally ingests them into Bronze Delta tables → Silver notebooks clean, deduplicate, and upsert into curated tables → Delta Live Tables builds the Gold star schema, ready for BI and analytics.

---
