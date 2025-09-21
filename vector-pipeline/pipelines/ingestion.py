from services.utils.logger import setup_logger
from services.utils.config_loader import load_config
from services.utils.pg_handler import PostgresHandler

from services.utils.minio_utils import MinioHandler
from services.utils.csv_parser import CSVParser
from services.utils.data_transform import filter_row_to_columns

logger = setup_logger("ingestion_logger", log_file="logs/ingestion.log", to_console=True)

def run_ingestion():
    config = load_config()
    minio_cfg = config.get("minio", {})
    pg_cfg = config.get("postgres", {})
    bucket_name = minio_cfg.get("bucket_name", "amazonreviews")
    schema_name = "customer_stg"
    table_name = "reviews"

    minio_handler = MinioHandler(
        minio_cfg.get("endpoint"),
        minio_cfg.get("access_key"),
        minio_cfg.get("secret_key"),
        secure=minio_cfg.get("secure", False),
    )

    try:
        pg_handler = PostgresHandler(
            host=pg_cfg.get("host"),
            port=pg_cfg.get("port", 5432),
            dbname=pg_cfg.get("dbname"),
            user=pg_cfg.get("user"),
            password=pg_cfg.get("password"),
        )
        logger.info("PostgreSQL connection established successfully")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return

    valid_columns = pg_handler.get_table_columns(schema_name, table_name)
    logger.info(f"Table {schema_name}.{table_name} columns: {valid_columns}")

    files = minio_handler.list_files(bucket_name)
    if not files:
        logger.info(f"No files found in bucket '{bucket_name}'. Exiting.")
        pg_handler.close()
        return

    logger.info(f"Files found in bucket '{bucket_name}': {len(files)} files")
    for f in files:
        logger.info(f" - {f}")

    sample_file = files[0]
    logger.info(f"Processing file: {sample_file}")

    try:
        content = minio_handler.get_file_content(bucket_name, sample_file)
        logger.info(f"Successfully read file content from {sample_file}")

        parsed_rows = CSVParser.parse_csv_string(content, max_rows=None)
        logger.info(f"Parsed {len(parsed_rows)} rows from CSV")

        transformed_rows = [filter_row_to_columns(row, valid_columns, sample_file) for row in parsed_rows]
        logger.info(f"Prepared {len(transformed_rows)} rows for database insertion")

        if transformed_rows:
            pg_handler.insert_reviews_batch(schema_name, table_name, transformed_rows)
            logger.info(f"Inserted {len(transformed_rows)} rows into {schema_name}.{table_name}")
        else:
            logger.warning("No rows to insert into database")

    except Exception as e:
        logger.error(f"Error processing file {sample_file}: {e}")

    finally:
        pg_handler.close()
        logger.info("PostgreSQL connection closed")

if __name__ == "__main__":
    logger.info("Starting ingestion pipeline")
    run_ingestion()
    logger.info("Ingestion pipeline completed")
