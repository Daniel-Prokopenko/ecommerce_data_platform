from pathlib import Path
from datetime import datetime, UTC

import polars as pl


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SOURCE_DIR = PROJECT_ROOT / "datasets" / "source"
MAPPINGS_DIR = PROJECT_ROOT / "datasets" / "mappings"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

SOURCE_FILES = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
}

MAPPING_FILES = {
    "product_category_name_translation.csv": "product_category_name_translation",
    "olist_order_reviews_dataset_translated.csv": "order_reviews_translation",
}


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_csv(csv_path: Path) -> pl.DataFrame:
    schema_overrides = {
        "olist_customers_dataset.csv": {"customer_zip_code_prefix": pl.Utf8},
        "olist_geolocation_dataset.csv": {"geolocation_zip_code_prefix": pl.Utf8},
        "olist_sellers_dataset.csv": {"seller_zip_code_prefix": pl.Utf8},
    }

    return pl.read_csv(
        csv_path,
        schema_overrides=schema_overrides.get(csv_path.name),
    )


def add_technical_metadata(df: pl.DataFrame, source_file: str) -> pl.DataFrame:
    ingested_at = datetime.now(UTC).isoformat()

    return df.with_columns(
        pl.lit(source_file).alias("_source_file"),
        pl.lit(ingested_at).alias("_ingested_at"),
    )


def write_parquet(df: pl.DataFrame, output_path: Path) -> None:
    df.write_parquet(output_path)


def ingest_group(base_dir: Path, file_registry: dict[str, str]) -> tuple[int, int]:
    loaded_count = 0
    skipped_count = 0

    for source_file, target_table in file_registry.items():
        csv_path = base_dir / source_file
        output_path = BRONZE_DIR / f"{target_table}.parquet"

        if not csv_path.exists():
            print(f"[WARN] File not found: {csv_path}")
            skipped_count += 1
            continue

        print(f"[INFO] Reading: {csv_path.name}")
        df = read_csv(csv_path)
        df = add_technical_metadata(df, source_file)

        print(f"[INFO] Rows: {df.height}, Cols: {df.width}")
        write_parquet(df, output_path)
        print(f"[OK] Saved: {output_path}")

        loaded_count += 1

    return loaded_count, skipped_count


def main() -> None:
    ensure_directory(BRONZE_DIR)

    print("[INFO] Starting bronze ingestion...")

    source_loaded, source_skipped = ingest_group(SOURCE_DIR, SOURCE_FILES)
    mapping_loaded, mapping_skipped = ingest_group(MAPPINGS_DIR, MAPPING_FILES)

    total_loaded = source_loaded + mapping_loaded
    total_skipped = source_skipped + mapping_skipped

    print("-" * 50)
    print("[INFO] Bronze ingestion finished")
    print(f"[INFO] Loaded tables: {total_loaded}")
    print(f"[INFO] Skipped files: {total_skipped}")


if __name__ == "__main__":
    main()