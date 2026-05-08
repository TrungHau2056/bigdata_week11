from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from common import (
    create_namespace,
    env,
    q,
    table_exists,
    table_name,
    with_sync_metadata,
)

SOURCE_TABLES: Dict[str, List[str]] = {
    "offices": ["officeCode"],
    "employees": ["employeeNumber"],
    "customers": ["customerNumber"],
    "productlines": ["productLine"],
    "products": ["productCode"],
    "orders": ["orderNumber"],
    "orderdetails": ["orderNumber", "productCode"],
    "payments": ["customerNumber", "checkNumber"],
}


def _jdbc_read(spark: SparkSession, table: str) -> DataFrame:
    mysql_host = env("MYSQL_HOST", "mysql")
    mysql_port = env("MYSQL_PORT", "3306")
    mysql_database = env("MYSQL_DATABASE", "classicmodels")
    mysql_user = env("MYSQL_USER", "etl")
    mysql_password = env("MYSQL_PASSWORD", "etl")
    url = (
        f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
        "?useSSL=false&allowPublicKeyRetrieval=true"
    )
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", table)
        .option("user", mysql_user)
        .option("password", mysql_password)
        .load()
    )


def _pk_join(left_alias: str, right_alias: str, primary_keys: List[str]) -> str:
    return " AND ".join(
        f"{left_alias}.{q(pk)} = {right_alias}.{q(pk)}" for pk in primary_keys
    )


def sync_bronze_table(
    spark: SparkSession, table: str, primary_keys: List[str], bronze_namespace: str
) -> None:
    target = table_name(bronze_namespace, table)
    stage_view = f"stage_{table}"
    source_df = with_sync_metadata(_jdbc_read(spark, table))
    source_df.createOrReplaceTempView(stage_view)

    if not table_exists(spark, target):
        (
            source_df.writeTo(target)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )
        print(f"  created {target}")
        return

    all_columns = source_df.columns
    update_assignments = ",\n          ".join(
        f"{q(column)} = s.{q(column)}" for column in all_columns
    )
    insert_columns = ", ".join(q(column) for column in all_columns)
    insert_values = ", ".join(f"s.{q(column)}" for column in all_columns)

    spark.sql(
        f"""
        MERGE INTO {target} t
        USING {stage_view} s
        ON {_pk_join("t", "s", primary_keys)}
        WHEN MATCHED AND (t._is_deleted = true OR t._row_hash <> s._row_hash)
          THEN UPDATE SET
          {update_assignments}
        WHEN NOT MATCHED
          THEN INSERT ({insert_columns})
          VALUES ({insert_values})
        """
    )

    key_columns = ", ".join(f"t.{q(pk)}" for pk in primary_keys)
    spark.sql(
        f"""
        MERGE INTO {target} t
        USING (
          SELECT {key_columns}
          FROM {target} t
          LEFT ANTI JOIN {stage_view} s
          ON {_pk_join("t", "s", primary_keys)}
          WHERE t._is_deleted = false
        ) d
        ON {_pk_join("t", "d", primary_keys)}
        WHEN MATCHED THEN UPDATE SET
          _is_deleted = true,
          _synced_at = current_timestamp()
        """
    )
    print(f"  synced {target}")


def sync_all_bronze(spark: SparkSession) -> str:
    bronze_namespace = env("ICEBERG_BRONZE_NAMESPACE", "bronze")
    create_namespace(spark, bronze_namespace)

    print("Syncing classicmodels source tables into Iceberg bronze...")
    for source_table, primary_key in SOURCE_TABLES.items():
        sync_bronze_table(spark, source_table, primary_key, bronze_namespace)

    return bronze_namespace
