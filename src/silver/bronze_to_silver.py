from pathlib import Path
import duckdb


BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def transform_customers(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        COPY (
            SELECT
                TRIM(customer_id) AS customer_id,
                TRIM(customer_unique_id) AS customer_unique_id,
                customer_zip_code_prefix,
                LOWER(TRIM(customer_city)) AS customer_city,
                UPPER(TRIM(customer_state)) AS customer_state,
                _source_file,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
            FROM read_parquet('data/bronze/customers.parquet')
        )
        TO 'data/silver/customers_clean.parquet'
        (FORMAT PARQUET);
    """)


def transform_geolocation(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        COPY (
            SELECT DISTINCT
                geolocation_zip_code_prefix,
                CAST(geolocation_lat AS DOUBLE) AS geolocation_lat,
                CAST(geolocation_lng AS DOUBLE) AS geolocation_lng,
                LOWER(regexp_replace(TRIM(geolocation_city), '^[^[:alnum:]]+|[^[:alnum:]]+$', '')) AS geolocation_city,
                UPPER(regexp_replace(TRIM(geolocation_state), '^[^[:alpha:]]+|[^[:alpha:]]+$', '')) AS geolocation_state,
                _source_file,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
            FROM read_parquet('data/bronze/geolocation.parquet')
        )
        TO 'data/silver/geolocation_clean.parquet'
        (FORMAT PARQUET);
    """)


def transform_order_items(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        COPY (
            SELECT
                TRIM(order_id) AS order_id,
                order_item_id,
                TRIM(product_id) AS product_id,
                TRIM(seller_id) AS seller_id,
                CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
                CAST(price AS DOUBLE) AS price,
                CAST(freight_value AS DOUBLE) AS freight_value,
                _source_file,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
            FROM read_parquet('data/bronze/order_items.parquet')
        )
        TO 'data/silver/order_items_clean.parquet'
        (FORMAT PARQUET);
    """)


def transform_order_payments(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        COPY (
            SELECT
                TRIM(order_id) AS order_id,
                payment_sequential,
                LOWER(TRIM(payment_type)) AS payment_type,
                payment_installments,
                CAST(payment_value AS DOUBLE) AS payment_value,
                _source_file,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
            FROM read_parquet('data/bronze/order_payments.parquet')
        )
        TO 'data/silver/order_payments_clean.parquet'
        (FORMAT PARQUET);
    """)


def transform_order_reviews(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        COPY (
            SELECT
                TRIM(r.review_id) AS review_id,
                TRIM(r.order_id) AS order_id,
                r.review_score,
                TRIM(r.review_comment_title) AS review_comment_title,
                TRIM(r.review_comment_message) AS review_comment_message,
                CASE
                    WHEN TRIM(r.review_comment_title) = '' THEN NULL
                    WHEN regexp_matches(TRIM(r.review_comment_title), '^[[:punct:][:space:]]+$') THEN NULL
                    ELSE TRIM(r.review_comment_title)
                END AS review_comment_title_clean,
                CASE
                    WHEN TRIM(r.review_comment_message) = '' THEN NULL
                    WHEN regexp_matches(TRIM(r.review_comment_message), '^[[:punct:][:space:]]+$') THEN NULL
                    ELSE TRIM(r.review_comment_message)
                END AS review_comment_message_clean,
                TRIM(t.review_comment_title) AS review_comment_title_en,
                TRIM(t.review_comment_message) AS review_comment_message_en,
                CAST(r.review_creation_date AS TIMESTAMP) AS review_creation_date,
                CAST(r.review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp,
                r._source_file,
                CAST(r._ingested_at AS TIMESTAMP) AS _ingested_at
            FROM read_parquet('data/bronze/order_reviews.parquet') r
            LEFT JOIN read_parquet('data/bronze/order_reviews_translation.parquet') t
                ON TRIM(r.review_id) = TRIM(t.review_id)
               AND TRIM(r.order_id) = TRIM(t.order_id)
        )
        TO 'data/silver/order_reviews_clean.parquet'
        (FORMAT PARQUET);
    """)


def transform_orders(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        COPY (
            SELECT
                TRIM(order_id) AS order_id,
                TRIM(customer_id) AS customer_id,
                LOWER(TRIM(order_status)) AS order_status,
                CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
                CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
                CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
                CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
                CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
                _source_file,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
            FROM read_parquet('data/bronze/orders.parquet')
        )
        TO 'data/silver/orders_clean.parquet'
        (FORMAT PARQUET);
    """)


def transform_products(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        COPY (
            SELECT
                TRIM(p.product_id) AS product_id,
                LOWER(TRIM(p.product_category_name)) AS product_category_name,
                LOWER(TRIM(t.product_category_name_english)) AS product_category_name_english,
                p.product_name_lenght,
                p.product_description_lenght,
                p.product_photos_qty,
                p.product_weight_g,
                p.product_length_cm,
                p.product_height_cm,
                p.product_width_cm,
                p._source_file,
                CAST(p._ingested_at AS TIMESTAMP) AS _ingested_at
            FROM read_parquet('data/bronze/products.parquet') p
            LEFT JOIN read_parquet('data/bronze/product_category_name_translation.parquet') t
                ON LOWER(TRIM(p.product_category_name)) = LOWER(TRIM(t.product_category_name))
        )
        TO 'data/silver/products_clean.parquet'
        (FORMAT PARQUET);
    """)


def transform_sellers(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        COPY (
            SELECT
                TRIM(seller_id) AS seller_id,
                seller_zip_code_prefix,
                CASE
                    WHEN TRIM(seller_city) = '' THEN NULL
                    WHEN regexp_matches(TRIM(seller_city), '^[0-9[:space:][:punct:]]+$') THEN NULL
                    ELSE LOWER(TRIM(seller_city))
                END AS seller_city,
                UPPER(TRIM(seller_state)) AS seller_state,
                _source_file,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
            FROM read_parquet('data/bronze/sellers.parquet')
        )
        TO 'data/silver/sellers_clean.parquet'
        (FORMAT PARQUET);
    """)


TRANSFORM_FUNCTIONS: dict[str, callable] = {
    "customers": transform_customers,
    "geolocation": transform_geolocation,
    "order_items": transform_order_items,
    "order_payments": transform_order_payments,
    "order_reviews": transform_order_reviews,
    "orders": transform_orders,
    "products": transform_products,
    "sellers": transform_sellers,
}


def run_transforms(con: duckdb.DuckDBPyConnection, transform_registry: dict[str, callable]) -> tuple[int, int]:
    loaded_count = 0
    failed_count = 0

    for table_name, transform_func in transform_registry.items():
        try:
            print(f"[INFO] Transforming: {table_name}")
            transform_func(con)
            print(f"[OK] Saved: data/silver/{table_name}_clean.parquet")
            loaded_count += 1
        except Exception as e:
            print(f"[ERROR] Failed: {table_name} -> {e}")
            failed_count += 1

    return loaded_count, failed_count


def main() -> None:
    ensure_directory(SILVER_DIR)

    print("[INFO] Starting silver transformation...")

    con = duckdb.connect()
    try:
        loaded_count, failed_count = run_transforms(con, TRANSFORM_FUNCTIONS)
    finally:
        con.close()

    print("-" * 50)
    print("[INFO] Silver transformation finished")
    print(f"[INFO] Transformed tables: {loaded_count}")
    print(f"[INFO] Failed tables: {failed_count}")


if __name__ == "__main__":
    main()