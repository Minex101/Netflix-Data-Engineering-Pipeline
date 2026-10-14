from pyspark import pipelines as dp
from pyspark.sql.functions import *

# GOLD LAYER - SPARK DECLARATIVE PIPELINES

# ---------- DIMENSION TABLE ----------

@dp.materialized_view(name="dim_category")
def dim_category():
    df = spark.read.table("netflix_catalog.silver.netflix_category") \
            .select(trim(col("listed_in")).alias("category")) \
            .distinct() \
            .withColumn("DimCategoryKey", xxhash64("category"))
    return df

@dp.materialized_view(name="dim_director")
def dim_director():
    df = spark.read.table("netflix_catalog.silver.netflix_directors") \
            .select(trim(col("director")).alias("director")) \
            .distinct() \
            .withColumn("DimDirectorKey", xxhash64("director"))
    return df

@dp.materialized_view(name="dim_cast")
def dim_cast():
    df = spark.read.table("netflix_catalog.silver.netflix_cast") \
            .select(trim(col("cast")).alias("cast_member")) \
            .distinct() \
            .withColumn("DimCastKey", xxhash64("cast_member"))
    return df

@dp.materialized_view(name="dim_country")
def dim_country():
    df = spark.read.table("netflix_catalog.silver.netflix_countries") \
            .select(trim(col("country")).alias("country")) \
            .distinct() \
            .withColumn("DimCountryKey", xxhash64("country"))
    return df

@dp.materialized_view(name="dim_show")
def dim_show():
    df = spark.read.table("netflix_catalog.silver.netflix_titles") \
        .select("show_id", "title", "short_title", "description")
    return df

@dp.materialized_view(name="dim_date")
def dim_date():
    df = spark.read.table("netflix_catalog.silver.netflix_titles") \
        .select(to_date(col("date_added"), "M/d/yyyy").alias("full_date")) \
        .filter(col("full_date").isNotNull()) \
        .distinct() \
        .select(
            date_format(col("full_date"), "yyyyMMdd").cast("int").alias("DimDateKey"),
            col("full_date"),
            dayofmonth(col("full_date")).alias("day"),
            month(col("full_date")).alias("month"),
            date_format(col("full_date"), "MMMM").alias("month_name"),
            quarter(col("full_date")).alias("quarter"),
            year(col("full_date")).alias("year"),
        )
    return df

# ---------- BRIDGE TABLES ----------

@dp.materialized_view(name="bridge_show_category")
def bridge_show_category():
    df = spark.read.table("netflix_catalog.silver.netflix_category") \
        .select(col("show_id"), xxhash64(trim(col("listed_in"))).alias("DimCategoryKey")) \
        .distinct()
    return df

@dp.materialized_view(name="bridge_show_director")
def bridge_show_director():
    df = spark.read.table("netflix_catalog.silver.netflix_directors") \
        .select(col("show_id"), xxhash64(trim(col("director"))).alias("DimDirectorKey")) \
        .distinct()
    return df

@dp.materialized_view(name="bridge_show_cast")
def bridge_show_cast():
    df = spark.read.table("netflix_catalog.silver.netflix_cast") \
        .select(col("show_id"), xxhash64(trim(col("cast"))).alias("DimCastKey")) \
        .distinct()
    return df

@dp.materialized_view(name="bridge_show_country")
def bridge_show_country():
    df = spark.read.table("netflix_catalog.silver.netflix_countries") \
        .select(col("show_id"), xxhash64(trim(col("country"))).alias("DimCountryKey")) \
        .distinct()
    return df

# ---------- FACT TABLE ----------

@dp.materialized_view(name="fact_show")
def fact_show():
    df = spark.read.table("netflix_catalog.silver.netflix_titles") \
        .select(
            col("show_id"),
            date_format(to_date(col("date_added"), "M/d/yyyy"), "yyyyMMdd").cast("int").alias("DimDateKey"),
            col("type"),
            col("rating"),
            col("release_year"),
            expr("nullif(duration_minutes, 0)").alias("duration_minutes"),
            expr("nullif(duration_seasons, 0)").alias("duration_seasons"),
        )
    return df