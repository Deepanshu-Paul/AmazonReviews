import logging
from typing import List, Dict
from transformers import AutoTokenizer, AutoModel
import torch
import numpy as np
from services.utils.logger import setup_logger
from services.utils.config_loader import load_config
from services.utils.pg_handler import PostgresHandler
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct

logger = setup_logger("embedding_logger", log_file="logs/embedding.log", to_console=True)


class Embedder:
    def __init__(self, model_name: str, device: str = 'cpu'):
        logger.info(f"Loading embedding model: {model_name}")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.device = torch.device(device)
        self.model.to(self.device)
        self.model.eval()

    def encode(self, texts: List[str], max_length=512) -> List[List[float]]:
        with torch.no_grad():
            tokens = self.tokenizer(texts, padding=True, truncation=True, max_length=max_length, return_tensors='pt')
            tokens = {k: v.to(self.device) for k, v in tokens.items()}
            out = self.model(**tokens)
            embeddings = out.last_hidden_state[:,0,:].cpu().numpy()  # CLS token embedding
            return embeddings.tolist()


def run_embedding_generation(config: Dict):
    system_cfg = load_config("system_config")
    pg_cfg = system_cfg["postgres"]
    qdrant_cfg = system_cfg.get("qdrant", {})

    source_schema = config["source_schema"]
    source_table = config["source_table"]
    text_fields = config["text_fields"]
    model_name = config["model"]["name"]
    batch_size = config["model"].get("batch_size", 32)
    max_length = config["model"].get("max_length", 512)
    collection_name = config["qdrant"]["collection_name"]
    vector_size = config["qdrant"]["vector_size"]
    distance = config["qdrant"]["distance_metric"]
    metadata_fields = config["metadata_fields"]

    logger.info(f"Generating embeddings for {source_schema}.{source_table} using model {model_name}")

    # Initialize Postgres and Qdrant clients
    pg_handler = PostgresHandler(
        host=pg_cfg["host"],
        port=pg_cfg["port"],
        dbname=pg_cfg["database"],
        user=pg_cfg["user"],
        password=pg_cfg["password"],
    )

    qdrant_client = QdrantClient(host=qdrant_cfg.get("host", "localhost"), port=qdrant_cfg.get("port", 6333))

    # Create or recreate Qdrant collection
    qdrant_client.recreate_collection(
        collection_name=collection_name,
        vectors_config={"size": vector_size, "distance": distance}
    )

    embedder = Embedder(model_name=model_name, device='cuda' if torch.cuda.is_available() else 'cpu')

    try:
        with pg_handler.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {source_schema}.{source_table}")
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        logger.info(f"Fetched {len(rows)} rows from source for embedding")

        # Prepare embeddings and points for Qdrant
        points = []
        for i in range(0, len(rows), batch_size):
            batch_rows = rows[i:i+batch_size]
            texts = [
                " ".join([str(row.get(f, "") or "") for f in text_fields])
                for row in batch_rows
            ]
            vectors = embedder.encode(texts, max_length=max_length)

            for row, vector in zip(batch_rows, vectors):
                payload = {field: row.get(field) for field in metadata_fields}
                point = PointStruct(
                    id=row.get("id"),  # Ensure 'id' exists in source table as unique identifier
                    vector=vector,
                    payload=payload
                )
                points.append(point)

            # Upsert points to Qdrant batch-wise
            qdrant_client.upsert(collection_name=collection_name, points=points)
            logger.info(f"Upserted batch of {len(points)} vectors to Qdrant")
            points.clear()

    except Exception as e:
        logger.error(f"Error during embedding generation: {e}", exc_info=True)
        raise

    finally:
        pg_handler.close()
