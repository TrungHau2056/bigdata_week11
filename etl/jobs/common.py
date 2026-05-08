import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, lit, sha2
from pyspark.sql.utils import AnalysisException


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def get_spark_session(app_name: str) -> SparkSession:
    catalog = env("ICEBERG_CATALOG", "local")
    warehouse = env("ICEBERG_WAREHOUSE", "s3a://warehouse/iceberg")
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", catalog)
        .getOrCreate()
    )


def q(identifier: str) -> str:
    return f"`{identifier}`"


def table_name(namespace: str, table: str) -> str:
    catalog = env("ICEBERG_CATALOG", "local")
    return f"{catalog}.{namespace}.{table}"


def create_namespace(spark: SparkSession, namespace: str) -> None:
    catalog = env("ICEBERG_CATALOG", "local")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def table_exists(spark: SparkSession, full_name: str) -> bool:
    try:
        spark.table(full_name).limit(0).collect()
        return True
    except AnalysisException:
        return False


def with_sync_metadata(df: DataFrame) -> DataFrame:
    hash_columns = [coalesce(col(c).cast("string"), lit("__NULL__")) for c in df.columns]
    return (
        df.withColumn("_row_hash", sha2(concat_ws("||", *hash_columns), 256))
        .withColumn("_is_deleted", lit(False))
        .withColumn("_synced_at", current_timestamp())
    )


def key_expr(column: str) -> str:
    return (
        f"CAST(pmod(xxhash64(CAST({column} AS STRING)), "
        "9223372036854775807) AS BIGINT)"
    )


def nullable_key_expr(column: str) -> str:
    return f"CASE WHEN {column} IS NULL THEN NULL ELSE {key_expr(column)} END"
