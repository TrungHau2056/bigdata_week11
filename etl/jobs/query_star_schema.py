from common import env, get_spark_session, table_name

spark = get_spark_session("query-classicmodels-star-schema")

catalog = env("ICEBERG_CATALOG", "local")
star_namespace = env("ICEBERG_STAR_NAMESPACE", "star_schema")
star = f"{catalog}.{star_namespace}"

print("Revenue by order month, customer country, and product line")
spark.sql(
    f"""
    SELECT
      d.calendar_year,
      d.calendar_month,
      c.country,
      p.product_line,
      sum(f.quantity_ordered) AS units_sold,
      CAST(sum(f.gross_sales_amount) AS DECIMAL(12, 2)) AS gross_revenue,
      CAST(sum(f.margin_amount) AS DECIMAL(12, 2)) AS margin
    FROM {star}.fact_order_sales f
    JOIN {star}.dim_date d
      ON f.order_date_key = d.date_key
    JOIN {star}.dim_customer c
      ON f.customer_key = c.customer_key
    JOIN {star}.dim_product p
      ON f.product_key = p.product_key
    GROUP BY d.calendar_year, d.calendar_month, c.country, p.product_line
    ORDER BY d.calendar_year, d.calendar_month, c.country, p.product_line
    """
).show(truncate=False)

print("Top customers by gross revenue")
spark.sql(
    f"""
    SELECT
      c.customer_name,
      c.country,
      CAST(sum(f.gross_sales_amount) AS DECIMAL(12, 2)) AS gross_revenue
    FROM {star}.fact_order_sales f
    JOIN {star}.dim_customer c
      ON f.customer_key = c.customer_key
    GROUP BY c.customer_name, c.country
    ORDER BY gross_revenue DESC
    """
).show(truncate=False)

print("Iceberg star schema tables")
spark.sql(f"SHOW TABLES IN {star}").show(truncate=False)

spark.stop()
