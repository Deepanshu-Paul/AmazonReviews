import os
import json

def load_config():
    """
    Loads configuration from a JSON file at configs/pipeline_config.json or environment overrides.
    """
    config_path = os.getenv("PIPELINE_CONFIG_PATH", "configs/pipeline_config.json")
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Config file {config_path} not found, using empty config.")
        config = {}

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
