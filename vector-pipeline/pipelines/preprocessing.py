import re
import datetime
from typing import List, Dict

from services.utils.logger import setup_logger
from services.utils.pg_handler import PostgresHandler
from services.utils.config_loader import load_system_config, load_preprocessing_config

logger = setup_logger("preprocessing_logger", log_file="logs/preprocessing.log", to_console=True)

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"<.*?>", "", text)  # Remove HTML tags
    text = re.sub(r"[^a-z0-9\s]", " ", text)  # Remove special chars
    text = re.sub(r"\s+", " ", text).strip()  # Normalize whitespace
    return text

def parse_date(date_str: str, fmt: str = "%Y-%m-%d") -> datetime.date | None:
    try:
        return datetime.datetime.strptime(date_str, fmt).date()
    except Exception:
        return None

def deduplicate_rows(rows: List[Dict], key_fields: List[str]) -> List[Dict]:
    seen = set()
    deduped = []
    for row in rows:
        key = tuple(row.get(k) for k in key_fields)
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped

def run_preprocessing(config: Dict):
    """
    Preprocess data based on config settings, reading from source table and writing to target.
    Uses load_system_config() for DB connectivity and a per-pipeline config dict for steps.
    """
    system_cfg = load_system_config()
    pg_cfg = system_cfg["postgres"]

    source_schema = config["source_schema"]
    source_table = config["source_table"]
    target_schema = config["target_schema"]
    target_table = config["target_table"]

    logger.info(f"Preprocessing from {source_schema}.{source_table} to {target_schema}.{target_table}")

    pg_handler = PostgresHandler(
        host=pg_cfg["host"],
        port=pg_cfg["port"],
        dbname=pg_cfg["database"],  # loader provides both database and dbname
        user=pg_cfg["user"],
        password=pg_cfg["password"],
    )

    try:
        # Read rows from source table
        with pg_handler.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {source_schema}.{source_table}")
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        logger.info(f"Fetched {len(rows)} rows from source table")

        # Apply preprocessing steps from config
        for step in config.get("steps", []):
            stype = step.get("type")
            if stype == "text_cleaning":
                fields = step.get("fields", [])
                for row in rows:
                    for field in fields:
                        row[field] = clean_text(row.get(field))
                logger.info(f"Applied text cleaning to fields {fields}")

            elif stype == "date_parsing":
                fields = step.get("fields", [])
                fmt = step.get("format", "%Y-%m-%d")
                for row in rows:
                    for field in fields:
                        row[field] = parse_date(row.get(field), fmt)
                logger.info(f"Applied date parsing to fields {fields} with format {fmt}")

            elif stype == "deduplication":
                key_fields = step.get("key_fields", [])
                rows = deduplicate_rows(rows, key_fields)
                logger.info(f"Deduplicated rows based on keys {key_fields}, new count: {len(rows)}")

        # Overwrite target table (optional): clear then insert
        with pg_handler.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {target_schema}.{target_table}")
            logger.info(f"Cleared target table {target_schema}.{target_table} before insert")

        # Insert preprocessed data into target table
        pg_handler.insert_reviews_batch(
            target_schema,
            target_table,
            rows,
            on_conflict="ON CONFLICT DO NOTHING"
        )
        logger.info(f"Inserted {len(rows)} rows into {target_schema}.{target_table}")

    except Exception as e:
        logger.error(f"Error during preprocessing: {e}", exc_info=True)
        raise
    finally:
        pg_handler.close()

def run_all_preprocessing(preprocessing_configs: List[Dict] | None = None):
    """
    Convenience function to run all preprocessing pipelines from YAML or provided list.
    This is safe to call from an Airflow PythonOperator or directly.
    """
    if preprocessing_configs is None:
        preprocessing_configs = load_preprocessing_config()

    if not preprocessing_configs:
        logger.error("No preprocessing pipelines found in config")
        return

    for pipeline_config in preprocessing_configs:
        pipeline_name = pipeline_config.get("name", "unknown")
        logger.info(f"Starting preprocessing pipeline: {pipeline_name}")
        try:
            run_preprocessing(pipeline_config)
            logger.info(f"Completed preprocessing pipeline: {pipeline_name}")
        except Exception as e:
            logger.error(f"Failed preprocessing pipeline {pipeline_name}: {e}", exc_info=True)
            raise

if __name__ == "__main__":
    # Optional CLI entrypoint for local runs; Airflow imports won't execute this block.
    run_all_preprocessing()
