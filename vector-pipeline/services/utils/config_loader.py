import os
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional

# Internal helpers

def _read_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data

def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

def _project_root() -> Path:
    # services/utils/config_loader.py -> project root two levels up
    return Path(__file__).resolve().parents[2]

def _resolve_path(default_rel: str, override: Optional[str]) -> str:
    if override:
        return override
    return str(_project_root() / default_rel)

# Public API

def load_system_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load system-level connections (MinIO, Postgres, Qdrant, etc.) and apply
    environment overrides where present. Returns a dict with canonical keys:
    - minio: endpoint, access_key, secret_key, bucket_name, secure
    - postgres: host, port, database, user, password (and alias dbname)
    - qdrant: url, api_key, prefer_grpc
    """
    path = _resolve_path("configs/system_config.yml", config_path)
    config = _read_yaml(path)

    # Ensure sub-dicts exist
    config.setdefault("minio", {})
    config.setdefault("postgres", {})
    config.setdefault("qdrant", {})

    # MinIO overrides
    mi = config["minio"]
    mi["endpoint"] = os.getenv("MINIO_ENDPOINT", mi.get("endpoint"))
    mi["access_key"] = os.getenv("MINIO_ACCESS_KEY", mi.get("access_key"))
    mi["secret_key"] = os.getenv("MINIO_SECRET_KEY", mi.get("secret_key"))
    mi["bucket_name"] = os.getenv("MINIO_BUCKET_NAME", mi.get("bucket_name"))
    mi["secure"] = _to_bool(os.getenv("MINIO_SECURE", mi.get("secure", False)))

    # Postgres overrides
    pg = config["postgres"]
    pg["host"] = os.getenv("POSTGRES_HOST", pg.get("host"))
    pg["port"] = int(os.getenv("POSTGRES_PORT", pg.get("port", 5432)))
    # Prefer POSTGRES_DB then POSTGRES_DBNAME then config value
    db_env = os.getenv("POSTGRES_DB") or os.getenv("POSTGRES_DBNAME")
    pg["database"] = db_env if db_env is not None else pg.get("database") or pg.get("dbname")
    pg["user"] = os.getenv("POSTGRES_USER", pg.get("user"))
    pg["password"] = os.getenv("POSTGRES_PASSWORD", pg.get("password"))
    # Back-compat alias for code that expects 'dbname'
    pg["dbname"] = pg.get("database")

    # Qdrant overrides (token-based cloud or local)
    qd = config["qdrant"]
    qd["url"] = os.getenv("QDRANT_URL", qd.get("url"))
    qd["api_key"] = os.getenv("QDRANT_API_KEY", qd.get("api_key"))
    qd["prefer_grpc"] = _to_bool(os.getenv("QDRANT_PREFER_GRPC", qd.get("prefer_grpc", False)))

    return config

def load_tables_config(config_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load table routing without coupling to preprocessing/embedding targets.
    Adds default target_schema/table mirroring source schema/table when missing.
    """
    path = _resolve_path("configs/tables_config.yml", config_path)
    config = _read_yaml(path)
    tables = config.get("tables", [])
    for tbl in tables:
        tbl.setdefault("schema", tbl.get("source_schema", tbl.get("schema")))
        tbl.setdefault("table", tbl.get("source_table", tbl.get("table")))
        tbl.setdefault("target_schema", tbl.get("schema"))
        tbl.setdefault("target_table", tbl.get("table"))
    return tables

def load_preprocessing_config(config_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load preprocessing pipelines as a list from YAML. Each entry may define
    source_schema/table and target_schema/table along with steps.
    """
    path = _resolve_path("configs/preprocessing_config.yml", config_path)
    cfg = _read_yaml(path)
    return cfg.get("preprocessing_pipelines", [])

def load_embedding_config(config_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load embedding pipelines as a list from YAML. Each entry may define
    source table, text fields, fields_map, model settings, and Qdrant config.
    """
    path = _resolve_path("configs/embedding_config.yml", config_path)
    cfg = _read_yaml(path)
    return cfg.get("embedding_pipelines", [])

def load_clustering_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load clustering configuration (algorithm, params, I/O batch sizes, results table).
    Defaults to MiniBatchKMeans with streaming-friendly batch sizes if file is missing.
    """
    path = _resolve_path("configs/clustering_config.yml", config_path)
    if not Path(path).exists():
        return {
            "clustering": {
                "collection_name": "amazon_reviews",
                "algorithm": "minibatchkmeans",
                "params": {
                    "n_clusters": 10,
                    "n_init": "auto",
                    "max_iter": 100,
                    "random_state": 42,
                    "batch_size": 10000,
                },
                "io": {
                    "scroll_batch": 5000,
                    "predict_batch": 10000,
                    "payload_batch": 2000,
                    "db_batch": 5000,
                },
                "results_table": {"schema": "analytics", "table": "reviews_with_clusters"},
            }
        }
    return _read_yaml(path)

def load_payload_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load payload include_fields mapping for Qdrant payloads and analytics tables.
    """
    path = _resolve_path("configs/payload_config.yml", config_path)
    return _read_yaml(path)
