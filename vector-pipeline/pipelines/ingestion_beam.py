import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from services.utils.logger import setup_logger
from services.utils.config_loader import load_system_config, load_tables_config
from services.utils.beam_minio import ReadFromMinio
from services.utils.beam_transforms import ParseCSVDoFn, FilterToTableDoFn
from services.utils.beam_postgres import WriteToPostgres

# Setup logger for Beam ingestion
logger = setup_logger("beam_ingestion_logger", log_file="logs/ingestion_beam.log", to_console=False)

from services.utils.config_loader import load_system_config, load_tables_config

def run():
    system_cfg = load_system_config()
    tables = load_tables_config()

    minio_cfg = system_cfg["minio"]
    pg_cfg = system_cfg["postgres"]

    pipeline_args = ["--runner=DirectRunner", "--save_main_session"]
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    for tbl_cfg in tables:
        schema = tbl_cfg["schema"]
        table = tbl_cfg["table"]
        logger.info(f"Starting pipeline for {schema}.{table}")

        with beam.Pipeline(options=options) as p:
            filtered_rows = (
                p
                | f"ReadFiles_{table}" >> ReadFromMinio(
                        endpoint=minio_cfg["endpoint"],
                        access_key=minio_cfg["access_key"],
                        secret_key=minio_cfg["secret_key"],
                        bucket=minio_cfg["bucket_name"],
                        secure=minio_cfg.get("secure", False)
                    )
                | f"ParseCSV_{table}" >> beam.ParDo(ParseCSVDoFn())
                | f"FilterSchema_{table}" >> beam.ParDo(FilterToTableDoFn(
                        host=pg_cfg["host"],
                        port=pg_cfg["port"],
                        dbname=pg_cfg["database"],
                        user=pg_cfg["user"],
                        password=pg_cfg["password"],
                        schema=schema,
                        table=table
                    ))
            )

            total_count = filtered_rows | f"CountRows_{table}" >> beam.combiners.Count.Globally()

            _ = (
                filtered_rows
                | f"WriteToPostgres_{table}" >> beam.ParDo(WriteToPostgres(
                        host=pg_cfg["host"],
                        port=pg_cfg["port"],
                        dbname=pg_cfg["database"],
                        user=pg_cfg["user"],
                        password=pg_cfg["password"],
                        schema=schema,
                        table=table,
                        batch_size=1000
                    ))
            )

            def log_total_rows(count):
                logger.info(f"Total rows to be inserted in {schema}.{table}: {count}")

            total_count | f"LogTotalInsertedRows_{table}" >> beam.Map(log_total_rows)

if __name__ == "__main__":
    logger.info("=== Beam Ingestion Script Started ===")
    run()
    logger.info("=== Beam Ingestion Script Finished ===")
