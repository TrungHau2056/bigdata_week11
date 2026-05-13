# Classicmodels MySQL to Iceberg Star Schema - Airflow Orchestration

```text
MySQL classicmodels
  -> Airflow DAG orchestration (scheduling, monitoring, retry)
  -> Spark ETL định kỳ
  -> Iceberg bronze tables trên MinIO
  -> Iceberg star schema tables trên MinIO
```

## Quick Start

```bash
# Build và khởi động
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d

# Truy cập Airflow UI
http://localhost:8080
# Username: admin
# Password: admin

# Enable DAG "classicmodels_etl_sync" và click Play button để chạy
```

## Stack

### Hạ tầng dữ liệu

| Service | Description |
|---------|-------------|
| `mysql` | Source RDBMS, seed database `classicmodels` |
| `minio` | Object storage để lưu Iceberg warehouse |
| `postgres` | Airflow metadata database |

### Airflow Orchestration

| Service | Description |
|---------|-------------|
| `airflow-init` | Khởi tạo DB và tạo admin user |
| `airflow-webserver` | Web UI monitoring và điều phối DAG (port 8080) |
| `airflow-scheduler` | Lên lịch và trigger DAG runs |

### Spark ETL

| Service | Description |
|---------|-------------|
| `etl-manual` | Chạy một lượt ETL (profile: tools) |
| `query-manual` | Chạy query kiểm tra (profile: tools) |

## Airflow DAG: classicmodels_etl_sync

**Schedule:** `@every 5 minutes` (chạy mỗi 5 phút)

**DAG Structure:**

```
wait_for_mysql >> wait_for_minio >> create_warehouse_bucket >> run_etl >> run_query
```

| Task | Description |
|------|-------------|
| `wait_for_mysql` | Kiểm tra MySQL health qua JDBC connection |
| `wait_for_minio` | Kiểm tra MinIO health qua HTTP endpoint |
| `create_warehouse_bucket` | Tạo bucket `warehouse` trong MinIO (idempotent) |
| `run_etl` | Chạy `build_star_schema.py` sync MySQL → Bronze → Star Schema |
| `run_query` | Chạy `query_star_schema.py` verify kết quả |

**Monitoring Features:**

- Xem DAG runs status (success/failed/running)
- Xem logs từng task (Log tab)
- Retry failed tasks với 1 click
- Gantt chart, task duration, dependencies graph
- Alerting (cấu hình qua email/Slack)

## Hướng dẫn chạy thử

### 1. Khởi động hệ thống

```bash
# Build images
docker compose build airflow-init airflow-webserver airflow-scheduler

# Start all services
docker compose up -d

# Theo dõi logs
docker compose logs -f airflow-init
```

### 2. Truy cập Airflow UI

1. Mở trình duyệt: **http://localhost:8080**
2. Login: `admin` / `admin`
3. Tìm DAG `classicmodels_etl_sync`
4. Click toggle để enable
5. Click **Play button (▶)** để chạy manual

### 3. Monitor pipeline

- Click vào DAG name để xem chi tiết runs
- Click vào task boxes để xem logs
- Xem Graph view để thấy task dependencies

### 4. Kiểm tra kết quả

```bash
# Chạy query manual
docker compose --profile tools up query-manual

# Xem logs scheduler
docker compose logs -f airflow-scheduler
```

### Troubleshooting

```bash
# Xem logs airflow-init
docker compose logs airflow-init

# Restart services
docker compose restart airflow-scheduler airflow-webserver

# Rebuild từ đầu
docker compose down -v
docker compose build --no-cache airflow-init airflow-webserver airflow-scheduler
docker compose up -d
```

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

- `_row_hash`: phát hiện update
- `_is_deleted`: đánh dấu dòng đã bị xóa ở source
- `_synced_at`: thời điểm sync gần nhất

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

### Airflow DAG Code

**File:** `airflow/dags/classicmodels_etl.py`

```python
# DAG definition với 5 tasks
wait_mysql >> wait_minio >> create_bucket >> run_etl >> run_query
```

- Sử dụng `PythonOperator` cho health checks
- Sử dụng `BashOperator` cho spark-submit commands
- Retry logic: 10 retries với 10s delay cho health checks
- `max_active_runs=1`: Prevent concurrent DAG runs

## Project Structure

```
bigdata_week11/
├── docker-compose.yml          # Airflow + MySQL + MinIO
├── .env                        # Airflow config (username, password, fernet key)
├── airflow/
│   ├── Dockerfile              # Custom Airflow image với Spark
│   ├── requirements.txt        # Python dependencies
│   ├── logs/                   # Airflow logs
│   └── dags/
│       └── classicmodels_etl.py    # DAG definition
├── etl/jobs/
│   ├── common.py               # Shared utilities
│   ├── bronze.py               # Bronze layer sync
│   ├── star_schema.py          # Star schema build
│   ├── build_star_schema.py    # Orchestrator
│   └── query_star_schema.py    # Query tool
└── mysql/
    └── init/                   # MySQL init scripts
```

## Kết quả query

![Query Result](Picture1.png)

![Star Schema Tables](Picture2.png)
