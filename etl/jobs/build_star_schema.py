from common import env, get_spark_session, table_name
from bronze import sync_all_bronze
from star_schema import build_star_schema, STAR_TABLES

spark = get_spark_session("classicmodels-mysql-to-iceberg-star-schema")

bronze_namespace = sync_all_bronze(spark)
star_namespace = build_star_schema(spark, bronze_namespace)

print("Pipeline complete. Star schema tables:")
for star_table in STAR_TABLES:
    full_name = table_name(star_namespace, star_table)
    count = spark.table(full_name).count()
    print(f"  {full_name}: {count} rows")

spark.stop()
