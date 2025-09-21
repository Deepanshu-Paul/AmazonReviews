import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from services.utils.logger import setup_logger
from services.utils.config_loader import load_config
from services.utils.beam_minio import ReadFromMinio
from services.utils.beam_transforms import ParseCSVDoFn, FilterToTableDoFn
from services.utils.beam_postgres import WriteToPostgres

# Setup logger for Beam ingestion
logger = setup_logger("beam_ingestion_logger", log_file="logs/ingestion_beam.log", to_console=False)

def run():
    config = load_config()
    minio_cfg = config["minio"]
    pg_cfg = config["postgres"]
    schema = "customer_stg"
    table = "reviews"

    pipeline_args = ["--runner=DirectRunner", "--save_main_session"]
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        filtered_rows = (
            p
            | "ReadFiles" >> ReadFromMinio(
                    endpoint=minio_cfg["endpoint"],
                    access_key=minio_cfg["access_key"],
                    secret_key=minio_cfg["secret_key"],
                    bucket=minio_cfg["bucket_name"],
                    secure=minio_cfg.get("secure", False)
                )
            | "ParseCSV" >> beam.ParDo(ParseCSVDoFn())
            | "FilterSchema" >> beam.ParDo(FilterToTableDoFn(
                    host=pg_cfg["host"],
                    port=pg_cfg["port"],
                    dbname=pg_cfg["dbname"],
                    user=pg_cfg["user"],
                    password=pg_cfg["password"],
                    schema=schema,
                    table=table
                ))
        )

        # Count total rows before writing
        total_count = filtered_rows | "CountRows" >> beam.combiners.Count.Globally()

        # Write to Postgres
        _ = (
            filtered_rows
            | "WriteToPostgres" >> beam.ParDo(WriteToPostgres(
                    host=pg_cfg["host"],
                    port=pg_cfg["port"],
                    dbname=pg_cfg["dbname"],
                    user=pg_cfg["user"],
                    password=pg_cfg["password"],
                    schema=schema,
                    table=table,
                    batch_size=1000
                ))
        )

        def log_total_rows(count):
            logger.info(f"Total rows to be inserted: {count}")

        # Log the total count after pipeline processing
        total_count | "LogTotalInsertedRows" >> beam.Map(log_total_rows)

if __name__ == "__main__":
    logger.info("=== Beam Ingestion Script Started ===")
    run()
    logger.info("=== Beam Ingestion Script Finished ===")
