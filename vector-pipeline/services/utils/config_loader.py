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
    """
    path = _resolve_path("configs/system_config.yml", config_path)
    config = _read_yaml(path)

    # Ensure sub-dicts exist
    config.setdefault("minio", {})
    config.setdefault("postgres", {})

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
    source table, text fields, model settings, and Qdrant collection config.
    """
    path = _resolve_path("configs/embedding_config.yml", config_path)
    cfg = _read_yaml(path)
    return cfg.get("embedding_pipelines", [])
