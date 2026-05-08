from pyspark.sql import SparkSession

from common import (
    create_namespace,
    env,
    key_expr,
    nullable_key_expr,
    table_name,
)


def _replace_table_from_query(
    spark: SparkSession, table: str, query: str, partition_clause: str = ""
) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {table}
        USING iceberg
        {partition_clause}
        TBLPROPERTIES ('format-version' = '2')
        AS
        {query}
        """
    )


STAR_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_employee",
    "dim_office",
    "dim_date",
    "fact_order_sales",
    "fact_payments",
]


def build_star_schema(spark: SparkSession, bronze_namespace: str) -> str:
    star_namespace = env("ICEBERG_STAR_NAMESPACE", "star_schema")
    create_namespace(spark, star_namespace)

    offices = table_name(bronze_namespace, "offices")
    employees = table_name(bronze_namespace, "employees")
    customers = table_name(bronze_namespace, "customers")
    productlines = table_name(bronze_namespace, "productlines")
    products = table_name(bronze_namespace, "products")
    orders = table_name(bronze_namespace, "orders")
    orderdetails = table_name(bronze_namespace, "orderdetails")
    payments = table_name(bronze_namespace, "payments")

    print("Building classicmodels star schema...")

    _replace_table_from_query(
        spark,
        table_name(star_namespace, "dim_customer"),
        f"""
        SELECT
          {key_expr("c.customerNumber")} AS customer_key,
          c.customerNumber AS customer_number,
          c.customerName AS customer_name,
          c.contactFirstName AS contact_first_name,
          c.contactLastName AS contact_last_name,
          c.phone,
          c.addressLine1 AS address_line_1,
          c.addressLine2 AS address_line_2,
          c.city,
          c.state,
          c.postalCode AS postal_code,
          c.country,
          {nullable_key_expr("c.salesRepEmployeeNumber")} AS sales_rep_employee_key,
          c.salesRepEmployeeNumber AS sales_rep_employee_number,
          CAST(c.creditLimit AS DECIMAL(12, 2)) AS credit_limit,
          current_timestamp() AS etl_loaded_at
        FROM {customers} c
        WHERE c._is_deleted = false
        """,
    )

    _replace_table_from_query(
        spark,
        table_name(star_namespace, "dim_product"),
        f"""
        SELECT
          {key_expr("p.productCode")} AS product_key,
          p.productCode AS product_code,
          p.productName AS product_name,
          p.productLine AS product_line,
          pl.textDescription AS product_line_description,
          p.productScale AS product_scale,
          p.productVendor AS product_vendor,
          p.productDescription AS product_description,
          CAST(p.quantityInStock AS INT) AS quantity_in_stock,
          CAST(p.buyPrice AS DECIMAL(10, 2)) AS buy_price,
          CAST(p.MSRP AS DECIMAL(10, 2)) AS msrp,
          current_timestamp() AS etl_loaded_at
        FROM {products} p
        LEFT JOIN {productlines} pl
          ON p.productLine = pl.productLine
          AND pl._is_deleted = false
        WHERE p._is_deleted = false
        """,
    )

    _replace_table_from_query(
        spark,
        table_name(star_namespace, "dim_employee"),
        f"""
        SELECT
          {key_expr("e.employeeNumber")} AS employee_key,
          e.employeeNumber AS employee_number,
          e.firstName AS first_name,
          e.lastName AS last_name,
          concat(e.firstName, ' ', e.lastName) AS full_name,
          e.extension,
          e.email,
          e.jobTitle AS job_title,
          {nullable_key_expr("e.reportsTo")} AS manager_employee_key,
          e.reportsTo AS manager_employee_number,
          {key_expr("e.officeCode")} AS office_key,
          e.officeCode AS office_code,
          current_timestamp() AS etl_loaded_at
        FROM {employees} e
        WHERE e._is_deleted = false
        """,
    )

    _replace_table_from_query(
        spark,
        table_name(star_namespace, "dim_office"),
        f"""
        SELECT
          {key_expr("o.officeCode")} AS office_key,
          o.officeCode AS office_code,
          o.city,
          o.phone,
          o.addressLine1 AS address_line_1,
          o.addressLine2 AS address_line_2,
          o.state,
          o.country,
          o.postalCode AS postal_code,
          o.territory,
          current_timestamp() AS etl_loaded_at
        FROM {offices} o
        WHERE o._is_deleted = false
        """,
    )

    _replace_table_from_query(
        spark,
        table_name(star_namespace, "dim_date"),
        f"""
        WITH all_dates AS (
          SELECT orderDate AS calendar_date FROM {orders}
          WHERE _is_deleted = false
          UNION ALL
          SELECT requiredDate AS calendar_date FROM {orders}
          WHERE _is_deleted = false
          UNION ALL
          SELECT shippedDate AS calendar_date FROM {orders}
          WHERE _is_deleted = false AND shippedDate IS NOT NULL
          UNION ALL
          SELECT paymentDate AS calendar_date FROM {payments}
          WHERE _is_deleted = false
        ),
        bounds AS (
          SELECT min(calendar_date) AS min_date, max(calendar_date) AS max_date
          FROM all_dates
        ),
        dates AS (
          SELECT explode(sequence(min_date, max_date, interval 1 day)) AS calendar_date
          FROM bounds
        )
        SELECT
          CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
          calendar_date,
          year(calendar_date) AS calendar_year,
          quarter(calendar_date) AS calendar_quarter,
          month(calendar_date) AS calendar_month,
          date_format(calendar_date, 'MMMM') AS month_name,
          dayofmonth(calendar_date) AS day_of_month,
          dayofweek(calendar_date) AS day_of_week,
          date_format(calendar_date, 'EEEE') AS day_name,
          CASE WHEN dayofweek(calendar_date) IN (1, 7) THEN true ELSE false END AS is_weekend
        FROM dates
        """,
    )

    _replace_table_from_query(
        spark,
        table_name(star_namespace, "fact_order_sales"),
        f"""
        SELECT
          concat(CAST(od.orderNumber AS STRING), '-', od.productCode) AS sales_line_id,
          od.orderNumber AS order_number,
          od.orderLineNumber AS order_line_number,
          {key_expr("o.customerNumber")} AS customer_key,
          {key_expr("od.productCode")} AS product_key,
          {nullable_key_expr("c.salesRepEmployeeNumber")} AS sales_rep_employee_key,
          {nullable_key_expr("e.officeCode")} AS sales_office_key,
          CAST(date_format(o.orderDate, 'yyyyMMdd') AS INT) AS order_date_key,
          CAST(date_format(o.requiredDate, 'yyyyMMdd') AS INT) AS required_date_key,
          CASE
            WHEN o.shippedDate IS NULL THEN NULL
            ELSE CAST(date_format(o.shippedDate, 'yyyyMMdd') AS INT)
          END AS shipped_date_key,
          o.orderDate AS order_date,
          o.requiredDate AS required_date,
          o.shippedDate AS shipped_date,
          o.status AS order_status,
          CAST(od.quantityOrdered AS INT) AS quantity_ordered,
          CAST(od.priceEach AS DECIMAL(10, 2)) AS price_each,
          CAST(p.buyPrice AS DECIMAL(10, 2)) AS buy_price,
          CAST(p.MSRP AS DECIMAL(10, 2)) AS msrp,
          CAST(od.quantityOrdered * od.priceEach AS DECIMAL(12, 2)) AS gross_sales_amount,
          CAST(od.quantityOrdered * p.buyPrice AS DECIMAL(12, 2)) AS cost_amount,
          CAST(od.quantityOrdered * (od.priceEach - p.buyPrice) AS DECIMAL(12, 2)) AS margin_amount,
          current_timestamp() AS etl_loaded_at
        FROM {orderdetails} od
        JOIN {orders} o
          ON od.orderNumber = o.orderNumber
          AND o._is_deleted = false
        JOIN {customers} c
          ON o.customerNumber = c.customerNumber
          AND c._is_deleted = false
        JOIN {products} p
          ON od.productCode = p.productCode
          AND p._is_deleted = false
        LEFT JOIN {employees} e
          ON c.salesRepEmployeeNumber = e.employeeNumber
          AND e._is_deleted = false
        WHERE od._is_deleted = false
          AND o.status <> 'Cancelled'
        """,
        "PARTITIONED BY (days(order_date))",
    )

    _replace_table_from_query(
        spark,
        table_name(star_namespace, "fact_payments"),
        f"""
        SELECT
          concat(CAST(p.customerNumber AS STRING), '-', p.checkNumber) AS payment_id,
          p.customerNumber AS customer_number,
          p.checkNumber AS check_number,
          {key_expr("p.customerNumber")} AS customer_key,
          {nullable_key_expr("c.salesRepEmployeeNumber")} AS sales_rep_employee_key,
          CAST(date_format(p.paymentDate, 'yyyyMMdd') AS INT) AS payment_date_key,
          p.paymentDate AS payment_date,
          CAST(p.amount AS DECIMAL(12, 2)) AS payment_amount,
          current_timestamp() AS etl_loaded_at
        FROM {payments} p
        JOIN {customers} c
          ON p.customerNumber = c.customerNumber
          AND c._is_deleted = false
        WHERE p._is_deleted = false
        """,
        "PARTITIONED BY (days(payment_date))",
    )

    return star_namespace
