# Classicmodels MySQL to Iceberg Star Schema

```text
MySQL classicmodels
  -> Airflow DAG orchestration
  -> Spark ETL định kỳ
  -> Iceberg bronze tables trên MinIO
  -> Iceberg star schema tables trên MinIO
```

## Stack

### Hạ tầng dữ liệu

- `mysql`: source RDBMS, seed database `classicmodels`.
- `minio`: object storage để lưu Iceberg warehouse.
- `postgres`: database lưu Airflow metadata.

### Airflow Orchestration

- `airflow-init`: khởi tạo database và tạo admin user.
- `airflow-webserver`: web UI để monitoring và điều phối DAG.
- `airflow-scheduler`: lên lịch và trigger các DAG runs.

### Spark ETL

- `etl-manual`: chạy một lượt ETL (profile: tools).
- `query-manual`: chạy query kiểm tra (profile: tools).

## Khởi động hệ thống

```bash
# Khởi động tất cả services
docker compose up -d

# Truy cập Airflow UI
http://localhost:8080
# Username: admin
# Password: admin

# Enable DAG "classicmodels_etl_sync" và chạy manually hoặc đợi schedule
```

## Airflow DAG: classicmodels_etl_sync

**Schedule:** `@every 5 minutes` (chạy mỗi 5 phút)

**Tasks:**

```
wait_for_mysql >> wait_for_minio >> create_warehouse_bucket >> run_etl >> run_query
```

| Task | Description |
|------|-------------|
| `wait_for_mysql` | Kiểm tra MySQL health qua JDBC connection |
| `wait_for_minio` | Kiểm tra MinIO health qua HTTP endpoint |
| `create_warehouse_bucket` | Tạo bucket `warehouse` trong MinIO (idempotent) |
| `run_etl` | Chạy `build_star_schema.py` để sync MySQL → Bronze → Star Schema |
| `run_query` | Chạy `query_star_schema.py` để verify kết quả |

**Monitoring:**

- Xem DAG runs: vào Airflow UI → Click vào DAG name
- Xem logs: Click vào task box → Log tab
- Retry failed tasks: Click vào failed task → Retry button
- Xem Gantt chart, duration, task dependencies

## Bronze Layer

Bronze là lớp Iceberg lưu dữ liệu gần giống source nhất. ETL đọc toàn bộ từng bảng MySQL, tính `_row_hash` cho mỗi dòng, rồi đồng bộ vào Iceberg theo primary key.

**Bronze tables:**

- `local.bronze.offices`
- `local.bronze.employees`
- `local.bronze.customers`
- `local.bronze.productlines`
- `local.bronze.products`
- `local.bronze.orders`
- `local.bronze.orderdetails`
- `local.bronze.payments`

**Metadata columns:**

- `_row_hash`: phát hiện update.
- `_is_deleted`: đánh dấu dòng đã bị xóa ở source.
- `_synced_at`: thời điểm sync gần nhất.

## Star Schema

Star schema được build từ các bronze rows còn active:

**Dimension tables:**

- `local.star_schema.dim_customer`
- `local.star_schema.dim_product`
- `local.star_schema.dim_employee`
- `local.star_schema.dim_office`
- `local.star_schema.dim_date`

**Fact tables:**

- `local.star_schema.fact_order_sales`
- `local.star_schema.fact_payments`

**Measures:**

- `quantity_ordered`
- `price_each`
- `gross_sales_amount`
- `cost_amount`
- `margin_amount`

## Pipeline ETL (Python + Spark)

### Modules

**`build_star_schema.py`:** Orchestrator chính, gọi `sync_all_bronze()` rồi `build_star_schema()`.

**`common.py`:** Shared utilities - `env()`, `get_spark_session()`, `key_expr()`, `nullable_key_expr()`, `table_name()`, `q()`, `create_namespace()`, `table_exists()`, `with_sync_metadata()`.

**`bronze.py`:** Đồng bộ các bảng nguồn MySQL vào Iceberg bronze tables trên MinIO. Đọc source qua JDBC, so sánh với bronze bằng primary key và `_row_hash`, rồi `MERGE INTO` Iceberg để insert/update/mark deleted rows.

**`star_schema.py`:** Chuyển dữ liệu từ mô hình OLTP sang Star Schema. Ghi các bảng dimension và fact vào Iceberg warehouse trên MinIO.

**`query_star_schema.py`:** Dùng Spark SQL kết nối đến Iceberg catalog. Đọc các bảng Star Schema từ MinIO. Chạy các truy vấn kiểm tra.

## Chạy thủ công (debug/testing)

```bash
# Chạy ETL manual
docker compose --profile tools up etl-manual

# Chạy query manual
docker compose --profile tools up query-manual
```

## Kết quả query

![Query Result](Picture1.png)

![Star Schema Tables](Picture2.png)
