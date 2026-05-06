"""
CS-404 BDA · Assignment 3 · ETL Pipeline
NUST SEECS · Spring 2026

Picks up from A2 ingestion.  Reads raw CSVs from HDFS,
applies all cleaning transformations documented in the A2 profiling report,
models data into a star schema (1 fact + 4 dimensions), writes Parquet to HDFS,
and validates output tables.

Usage:
    spark-submit etl.py
"""

import logging
import sys
import os

# Ensure Spark authenticates to HDFS as the correct user,
# otherwise it uses the Windows username which causes permission denied errors.
os.environ["HADOOP_USER_NAME"] = "saad"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, quarter, dayofweek, dayofmonth,
    split, when, lit, trim, lower, monotonically_increasing_id,
    count, sum as spark_sum, broadcast, regexp_extract
)
from pyspark.sql.types import IntegerType

# ── Logging setup ───────────────────────────────────────────
logging.basicConfig(
    filename="etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)
log = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────
# Full HDFS URI — points to the Ubuntu NameNode on localhost:9000
# Without this prefix, Spark treats the path as local Windows filesystem.
HDFS_NAMENODE = "hdfs://localhost:9000"
HDFS_RAW_DIR = f"{HDFS_NAMENODE}/warehouse/raw/uk_road_safety/year=2026/month=04"
HDFS_PROCESSED_DIR = f"{HDFS_NAMENODE}/warehouse/processed"

ACCIDENT_FILES = [
    f"{HDFS_RAW_DIR}/accidents_2005_to_2007.csv",
    f"{HDFS_RAW_DIR}/accidents_2009_to_2011.csv",
    f"{HDFS_RAW_DIR}/accidents_2012_to_2014.csv",
]
TRAFFIC_FILE = f"{HDFS_RAW_DIR}/ukTrafficAADF.csv"


# ============================================================
#  1. SPARK SESSION
# ============================================================
def create_spark_session():
    """Create a SparkSession configured for the Ubuntu HDFS cluster."""
    spark = SparkSession.builder \
        .appName("UK Road Safety – ETL Pipeline (A3)") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()
    # Also set on the Hadoop config object so all HDFS calls resolve correctly
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://localhost:9000")
    log.info("SparkSession created — connected to HDFS at hdfs://localhost:9000")
    return spark


# ============================================================
#  2. READ RAW DATA FROM HDFS
# ============================================================
def read_raw_data(spark):
    """Read the three accident CSVs from HDFS and union them."""
    log.info("--- Step: Reading raw data from HDFS ---")

    dfs = []
    for path in ACCIDENT_FILES:
        df = spark.read.csv(path, header=True, inferSchema=True)
        row_count = df.count()
        log.info(f"Loaded {path} — {row_count:,} rows, {len(df.columns)} columns")
        dfs.append(df)

    # Union all accident DataFrames
    accidents_df = dfs[0]
    for df in dfs[1:]:
        accidents_df = accidents_df.unionByName(df, allowMissingColumns=True)

    raw_count = accidents_df.count()
    log.info(f"Unioned accident DataFrame — {raw_count:,} total rows")
    return accidents_df, raw_count


# ============================================================
#  3. TRANSFORM — Apply all A2 cleaning strategies
# ============================================================
def transform_data(df):
    """
    Apply every transformation documented in the A2 profiling report.
    Each transformation has an inline comment citing the A2 issue number.
    """
    log.info("--- Step: Applying transformations ---")

    # ── A2 Issue #8: Duplicate Accident_Index (576,763 rows) ────────────
    # Action: dropDuplicates on Accident_Index; duplicates inflate count KPIs.
    before = df.count()
    df = df.dropDuplicates(["Accident_Index"])
    after = df.count()
    log.info(f"A2 #8  Dropped duplicates: {before:,} → {after:,} ({before - after:,} removed)")

    # ── A2 Issue #4: Date stored as string in all rows ──────────────────
    # Action: Parse 'dd/MM/yyyy' → DateType; required for Spark SQL time queries
    #         and the Time dimension in the star schema.
    df = df.withColumn("Date_Parsed", to_date(col("Date"), "dd/MM/yyyy"))

    # Derive calendar attributes for dim_time
    df = df.withColumn("Year_Derived", year(col("Date_Parsed")))
    df = df.withColumn("Month_Num", month(col("Date_Parsed")))
    df = df.withColumn("Quarter_Num", quarter(col("Date_Parsed")))
    df = df.withColumn("Day_Num", dayofmonth(col("Date_Parsed")))
    df = df.withColumn("DayOfWeek_Num", dayofweek(col("Date_Parsed")))  # 1=Sun … 7=Sat
    log.info("A2 #4  Parsed Date → DateType; derived Year, Month, Quarter, Day, DayOfWeek")

    # ── A2 Issue #5: Time stored as HH:MM string ───────────────────────
    # Action: Extract integer Hour (0-23) for BQ1 peak-hour analysis.
    # Note: Some rows contain full timestamp strings (e.g. '2026-05-04 22:30')
    # instead of plain 'HH:MM'. regexp_extract finds the HH:MM pattern anywhere
    # in the string. We keep the result as STRING first, check for empty (no match),
    # then cast to INT — casting before the empty-string check causes a BIGINT error.
    df = df.withColumn(
        "_hour_str",
        regexp_extract(col("Time").cast("string"), r"(\d{1,2}):(\d{2})", 1)
    )
    df = df.withColumn(
        "Hour",
        when(col("_hour_str") == "", lit(None))
        .otherwise(col("_hour_str").cast(IntegerType()))
    ).drop("_hour_str")
    # Derive time_period buckets (Morning/Afternoon/Evening/Night) for analytics
    df = df.withColumn(
        "time_period",
        when(col("Hour").between(6, 11), "Morning")
        .when(col("Hour").between(12, 17), "Afternoon")
        .when(col("Hour").between(18, 21), "Evening")
        .otherwise("Night")
    )
    # Derive is_weekend flag
    df = df.withColumn(
        "is_weekend",
        when(col("DayOfWeek_Num").isin(1, 7), lit(True)).otherwise(lit(False))
    )
    log.info("A2 #5  Extracted Hour from Time; derived time_period and is_weekend")

    # ── A2 Issue #1: Junction_Control nulls at non-junction sites ──────
    # Action: Fill with 'No Junction Applicable' — null means no junction, not unknown.
    df = df.withColumn(
        "Junction_Control",
        when(col("Junction_Control").isNull(), lit("No Junction Applicable"))
        .otherwise(col("Junction_Control"))
    )
    log.info("A2 #1  Filled Junction_Control nulls → 'No Junction Applicable'")

    # ── A2 Issue #7: Carriageway_Hazards — text placeholders & nulls ───
    # Action: Replace 'Data missing or out of range' placeholders with null,
    #         then fill all remaining nulls with 'None' (absence = no hazard).
    df = df.withColumn(
        "Carriageway_Hazards",
        when(
            lower(col("Carriageway_Hazards")).rlike("data missing|out of range"),
            lit(None)
        ).otherwise(col("Carriageway_Hazards"))
    )
    df = df.fillna({"Carriageway_Hazards": "None"})
    log.info("A2 #7  Standardised Carriageway_Hazards placeholders → 'None'")

    # ── A2 Issue #10: Weather_Conditions semantic duplicates ────────────
    # Action: Consolidate into canonical categories so BQ5 weather analysis
    #         does not double-count conditions.
    df = df.withColumn(
        "Weather_Conditions",
        when(col("Weather_Conditions").contains("Fine no high winds"), lit("Fine"))
        .when(col("Weather_Conditions").contains("Fine + high winds"), lit("Fine (High Winds)"))
        .when(col("Weather_Conditions").contains("Raining no high winds"), lit("Rain"))
        .when(col("Weather_Conditions").contains("Raining + high winds"), lit("Rain (High Winds)"))
        .when(col("Weather_Conditions").contains("Snowing no high winds"), lit("Snow"))
        .when(col("Weather_Conditions").contains("Snowing + high winds"), lit("Snow (High Winds)"))
        .when(col("Weather_Conditions").contains("Fog or mist"), lit("Fog / Mist"))
        .when(col("Weather_Conditions").contains("Other"), lit("Other"))
        .otherwise(col("Weather_Conditions"))
    )
    log.info("A2 #10 Consolidated Weather_Conditions semantic duplicates")

    # ── A2 Issue #6: LSOA missing for 108,238 older records ────────────
    # Action: Add boolean flag lsoa_available; LSOAs cannot be reverse-geocoded.
    df = df.withColumn(
        "lsoa_available",
        when(col("LSOA_of_Accident_Location").isNotNull(), lit(True)).otherwise(lit(False))
    )
    log.info("A2 #6  Added lsoa_available flag for 108,238 null LSOA records")

    # ── A2 Issue #9: 101 rows with missing coordinates ─────────────────
    # Action: Add is_coordinate_valid flag; exclude from geo-spatial joins only.
    df = df.withColumn(
        "is_coordinate_valid",
        when(
            col("Latitude").isNotNull() & col("Longitude").isNotNull(),
            lit(True)
        ).otherwise(lit(False))
    )
    log.info("A2 #9  Added is_coordinate_valid flag for missing lat/lon rows")

    # ── A2 Issue #2: 2nd_Road_Class/Number null when no 2nd road ──────
    # Action: Structural absence — fill string with 'N/A', numeric with -1.
    if "2nd_Road_Class" in df.columns:
        df = df.fillna({"2nd_Road_Class": "N/A"})
    if "2nd_Road_Number" in df.columns:
        df = df.fillna({"2nd_Road_Number": -1})
    log.info("A2 #2  Filled 2nd_Road_Class/Number structural nulls")

    # ── A2 Issue #3: Speed_limit invalid 'f0' values ───────────────────
    # A2 profiling found 0 invalid records (0.00%). No action needed — confirmed clean.
    log.info("A2 #3  Speed_limit: 0 invalid values confirmed — no action required")

    # ── Additional: Fill remaining nulls in key dimension columns ──────
    df = df.fillna({
        "Junction_Detail": "No Junction",
        "Light_Conditions": "Unknown",
        "Road_Surface_Conditions": "Unknown",
        "Weather_Conditions": "Unknown",
        "Road_Type": "Unknown",
        "Urban_or_Rural_Area": "Unknown",
    })

    clean_count = df.count()
    log.info(f"Transformation complete — {clean_count:,} clean rows")
    return df, clean_count


# ============================================================
#  4. MODEL — Build Star Schema (Fact + 4 Dimensions)
# ============================================================
def build_dim_time(df_clean):
    """Build dim_time from unique (Year, Month, Quarter, DayOfWeek, Hour) combos."""
    log.info("Building dim_time …")

    dim_time = df_clean.select(
        "Year_Derived", "Month_Num", "Quarter_Num",
        "Day_of_Week", "DayOfWeek_Num", "Hour",
        "time_period", "is_weekend"
    ).distinct()

    dim_time = dim_time.withColumn("time_key", monotonically_increasing_id())
    log.info(f"dim_time — {dim_time.count():,} rows")
    return dim_time


def build_dim_geography(df_clean):
    """Build dim_geography from unique (LSOA, Urban_or_Rural_Area) combos."""
    log.info("Building dim_geography …")

    dim_geo = df_clean.select(
        "LSOA_of_Accident_Location",
        "Urban_or_Rural_Area",
        "lsoa_available"
    ).distinct()

    dim_geo = dim_geo.withColumn("geo_key", monotonically_increasing_id())
    log.info(f"dim_geography — {dim_geo.count():,} rows")
    return dim_geo


def build_dim_road(df_clean):
    """Build dim_road from unique (Road_Type, Speed_limit, Junction_Detail, Junction_Control)."""
    log.info("Building dim_road …")

    dim_road = df_clean.select(
        "Road_Type", "Speed_limit",
        "Junction_Detail", "Junction_Control"
    ).distinct()

    dim_road = dim_road.withColumn("road_key", monotonically_increasing_id())
    log.info(f"dim_road — {dim_road.count():,} rows")
    return dim_road


def build_dim_environment(df_clean):
    """Build dim_environment from unique (Weather, Light, Surface, Hazards)."""
    log.info("Building dim_environment …")

    dim_env = df_clean.select(
        "Weather_Conditions", "Light_Conditions",
        "Road_Surface_Conditions", "Carriageway_Hazards"
    ).distinct()

    dim_env = dim_env.withColumn("env_key", monotonically_increasing_id())
    log.info(f"dim_environment — {dim_env.count():,} rows")
    return dim_env


def build_fact_table(df_clean, dim_time, dim_geo, dim_road, dim_env):
    """
    Build fact_accidents by joining cleaned data with dimension tables
    to attach surrogate keys.  Uses broadcast joins for small dims
    (Optimization: broadcast join — dim tables are small enough to fit in memory).
    """
    log.info("Building fact_accidents …")

    # ── Optimization: Broadcast join ────────────────────────────────────
    # Dim tables are small (hundreds to thousands of rows) compared to
    # the 1.5M-row fact table.  Broadcasting avoids expensive shuffle joins.

    # Join dim_time
    time_join_cols = [
        "Year_Derived", "Month_Num", "Quarter_Num",
        "Day_of_Week", "DayOfWeek_Num", "Hour",
        "time_period", "is_weekend"
    ]
    fact = df_clean.join(broadcast(dim_time), on=time_join_cols, how="left")

    # Join dim_geography
    geo_join_cols = ["LSOA_of_Accident_Location", "Urban_or_Rural_Area", "lsoa_available"]
    fact = fact.join(broadcast(dim_geo), on=geo_join_cols, how="left")

    # Join dim_road
    road_join_cols = ["Road_Type", "Speed_limit", "Junction_Detail", "Junction_Control"]
    fact = fact.join(broadcast(dim_road), on=road_join_cols, how="left")

    # Join dim_environment
    env_join_cols = [
        "Weather_Conditions", "Light_Conditions",
        "Road_Surface_Conditions", "Carriageway_Hazards"
    ]
    fact = fact.join(broadcast(dim_env), on=env_join_cols, how="left")

    # Select fact columns: surrogate keys + measures + degenerate dimensions
    fact = fact.select(
        # Primary key
        "Accident_Index",
        # Surrogate foreign keys
        "time_key", "geo_key", "road_key", "env_key",
        # Measures
        "Number_of_Casualties",
        "Number_of_Vehicles",
        # Degenerate dimensions (kept in fact for convenience)
        "Accident_Severity",
        "Latitude", "Longitude",
        "Date_Parsed",
        "is_coordinate_valid",
        # Partition column
        "Year_Derived",
    )

    fact_count = fact.count()
    log.info(f"fact_accidents — {fact_count:,} rows")
    return fact, fact_count


# ============================================================
#  5. LOAD — Write Parquet to HDFS
# ============================================================
def write_table(df, table_name, partition_col=None):
    """Write a DataFrame to HDFS as Parquet."""
    path = f"{HDFS_PROCESSED_DIR}/{table_name}"
    writer = df.write.mode("overwrite")

    if partition_col:
        # ── Optimization: Partitioning ──────────────────────────────────
        # Partitioning fact_accidents by Year_Derived enables partition
        # pruning in year-scoped Spark SQL queries, drastically reducing
        # the amount of data scanned.
        writer = writer.partitionBy(partition_col)
        log.info(f"Writing {table_name} to {path} (partitioned by {partition_col}) …")
    else:
        log.info(f"Writing {table_name} to {path} …")

    writer.parquet(path)
    log.info(f"Successfully wrote {table_name} to HDFS.")


# ============================================================
#  6. VALIDATE — Row counts & null checks
# ============================================================
def validate_tables(spark):
    """Re-read written Parquet tables and validate integrity."""
    log.info("--- Step: Validating written tables ---")

    tables = {
        "fact_accidents": ["Accident_Index", "time_key", "geo_key", "road_key", "env_key"],
        "dim_time":       ["time_key"],
        "dim_geography":  ["geo_key"],
        "dim_road":       ["road_key"],
        "dim_environment": ["env_key"],
    }

    for table_name, key_cols in tables.items():
        path = f"{HDFS_PROCESSED_DIR}/{table_name}"
        df = spark.read.parquet(path)
        total = df.count()
        log.info(f"VALIDATE  {table_name}: {total:,} rows written")

        # Assert no nulls in primary/foreign key columns
        for c in key_cols:
            null_count = df.filter(col(c).isNull()).count()
            if null_count > 0:
                log.warning(f"  ⚠ {table_name}.{c} has {null_count:,} null keys!")
            else:
                log.info(f"  ✓ {table_name}.{c} — 0 nulls (OK)")


# ============================================================
#  MAIN
# ============================================================
def main():
    log.info("=" * 60)
    log.info("  UK Road Safety — ETL Pipeline START")
    log.info("=" * 60)

    spark = create_spark_session()

    # ── Read ────────────────────────────────────────────────
    accidents_df, raw_count = read_raw_data(spark)

    # ── Transform ───────────────────────────────────────────
    df_clean, clean_count = transform_data(accidents_df)

    # ── Optimization: Cache cleaned DataFrame ───────────────
    # The cleaned DataFrame is reused 5 times (4 dim builds + 1 fact build).
    # Caching avoids re-computing all transformations each time.
    df_clean.cache()
    log.info("Cached cleaned DataFrame (reused across all table builds)")

    # ── Model — Build Star Schema ───────────────────────────
    dim_time = build_dim_time(df_clean)
    dim_geo  = build_dim_geography(df_clean)
    dim_road = build_dim_road(df_clean)
    dim_env  = build_dim_environment(df_clean)

    fact, fact_count = build_fact_table(df_clean, dim_time, dim_geo, dim_road, dim_env)

    # ── Load — Write Parquet to HDFS ────────────────────────
    write_table(dim_time, "dim_time")
    write_table(dim_geo,  "dim_geography")
    write_table(dim_road, "dim_road")
    write_table(dim_env,  "dim_environment")
    # Partition fact table by Year for query performance
    write_table(fact, "fact_accidents", partition_col="Year_Derived")

    # ── Validate ────────────────────────────────────────────
    validate_tables(spark)

    # ── Summary ─────────────────────────────────────────────
    log.info("=" * 60)
    log.info("  ETL SUMMARY")
    log.info(f"  Raw rows read:       {raw_count:,}")
    log.info(f"  Clean rows (post-dedup): {clean_count:,}")
    log.info(f"  Fact rows written:   {fact_count:,}")
    log.info("=" * 60)
    log.info("  ETL Pipeline COMPLETE")
    log.info("=" * 60)

    # Unpersist cached DF
    df_clean.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
