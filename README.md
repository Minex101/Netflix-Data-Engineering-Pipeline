# Netflix Data Engineering ELT Pipeline
An end-to-end, metadata-driven data pipeline that ingests Netflix catalog data (titles, cast, directors, countries, categories) from GitHub into Azure, transforms it through a Bronze → Silver → Gold (medallion architecture), and serves it as a star schema for analytics.

Built with **Azure Data Factory** (orchestration) + **Azure Databricks Jobs** (orchestration) + **Azure Databricks** (processing) + **Azure Data Lake Storage Gen2** (storage), with **Delta Lake** at every layer.

<img width="1598" height="898" alt="image" src="https://github.com/user-attachments/assets/7face1df-60c6-40fb-8364-67c46ba46526" />

## Tech Stack

**Azure:** Data Factory · Data Lake Storage Gen2 · Databricks · Key Vault · Unity Catalog
**Databricks:** PySpark · Delta Lake · Auto Loader (`cloudFiles`) · Delta Live Tables · Workflows
**Source:** GitHub (CSV) · **Languages:** Python, SQL

## How It Works (TL;DR)

ADF detects new CSVs in GitHub and lands them in the `raw` zone → Databricks Auto Loader incrementally ingests them into Bronze Delta tables → Silver notebooks clean, deduplicate, and upsert into curated tables → Delta Live Tables builds the Gold star schema — ready for BI and analytics.

---
