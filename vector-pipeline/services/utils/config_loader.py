import os
import yaml

def load_system_config(config_path=None):
    path = config_path or "configs/system_config.yml"
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config

def load_tables_config(config_path=None):
    path = config_path or "configs/tables_config.yml"
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config.get("tables", [])

    # Override config with environment variables if available
    config.setdefault("minio", {})
    config["minio"]["endpoint"] = os.getenv("MINIO_ENDPOINT", config["minio"].get("endpoint"))
    config["minio"]["access_key"] = os.getenv("MINIO_ACCESS_KEY", config["minio"].get("access_key"))
    config["minio"]["secret_key"] = os.getenv("MINIO_SECRET_KEY", config["minio"].get("secret_key"))
    config["minio"]["bucket_name"] = os.getenv("MINIO_BUCKET_NAME", config["minio"].get("bucket_name"))
    config["minio"]["secure"] = os.getenv("MINIO_SECURE", "False").lower() == "true"

    config.setdefault("postgres", {})
    config["postgres"]["host"]     = os.getenv("POSTGRES_HOST", config["postgres"].get("host"))
    config["postgres"]["port"]     = int(os.getenv("POSTGRES_PORT", config["postgres"].get("port", 5432)))
    config["postgres"]["dbname"]   = os.getenv("POSTGRES_DBNAME", config["postgres"].get("dbname"))
    config["postgres"]["user"]     = os.getenv("POSTGRES_USER", config["postgres"].get("user"))
    config["postgres"]["password"] = os.getenv("POSTGRES_PASSWORD", config["postgres"].get("password"))


    return config
