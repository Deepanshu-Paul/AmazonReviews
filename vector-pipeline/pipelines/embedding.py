# pipelines/embedding.py
from typing import Dict, List, Iterable, Tuple
import numpy as np
import yaml
from sentence_transformers import SentenceTransformer
from qdrant_client.http.models import PointStruct

from services.utils.logger import setup_logger
from services.utils.config_loader import load_system_config, load_embedding_config
from services.utils.pg_handler import PostgresHandler

logger = setup_logger("embedding_logger", log_file="logs/embedding.log", to_console=True)

def _resolve_text_columns(text_fields: List[str], fields_map: Dict[str, str]) -> List[str]:
    return [fields_map.get(f, f.lower().replace(" ", "_")) for f in text_fields]

def _concat_text(row: Dict, text_cols: List[str]) -> str:
    parts = [str(row.get(c) or "") for c in text_cols]
    return " ".join(p for p in parts if p).strip()

def _load_payload_map(path="configs/payload_config.yml") -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("payload", {}).get("include_fields", {})

def _batched(seq, n):
    batch = []
    for item in seq:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch

def generate_embeddings_batches(pipeline_cfg: Dict, system_cfg: Dict) -> Iterable[Tuple[List[PointStruct], int]]:
    pg_cfg = system_cfg["postgres"]
    text_fields = pipeline_cfg.get("text_fields", [])
    fields_map = pipeline_cfg.get("fields_map", {})
    text_cols = _resolve_text_columns(text_fields, fields_map)

    model = SentenceTransformer(pipeline_cfg["model"]["name"])
    batch_size = pipeline_cfg["model"].get("batch_size", 64)
    normalize = pipeline_cfg["model"].get("normalize", True)

    payload_map = _load_payload_map()

    source_schema = pipeline_cfg["source_schema"]
    source_table = pipeline_cfg["source_table"]

    pg = PostgresHandler(
        host=pg_cfg["host"], port=pg_cfg["port"], dbname=pg_cfg["database"],
        user=pg_cfg["user"], password=pg_cfg["password"],
    )
    try:
        with pg.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {source_schema}.{source_table}")
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        logger.info(f"Fetched {len(rows)} rows from {source_schema}.{source_table}")

        for batch_rows in _batched(rows, batch_size):
            texts = [_concat_text(r, text_cols) for r in batch_rows]

            # Ensure ndarray output; never use ndarray directly in boolean context
            vectors = model.encode(
                texts,
                batch_size=batch_size,
                show_progress_bar=False,
                normalize_embeddings=normalize,
                convert_to_numpy=True
            )

            # Use explicit size/shape checks to avoid ambiguous truth-value on ndarrays
            if not isinstance(vectors, np.ndarray) or vectors.size == 0:
                continue

            # Compute embedding dimension from shape
            dim = vectors.shape[1] if vectors.ndim == 2 else vectors.shape[0]

            points = []
            for r, vec in zip(batch_rows, vectors):
                payload = {k: r.get(v) for k, v in payload_map.items()}
                if "title" not in payload and "review_title" in r:
                    payload["title"] = r.get("review_title")
                points.append(PointStruct(
                    id=r.get("id"),
                    vector=vec.tolist(),
                    payload=payload
                ))

            if points:
                yield points, dim
    finally:
        pg.close()

def run_embedding_pipelines():
    system_cfg = load_system_config()
    for pipe in load_embedding_config():
        logger.info(f"Preparing embeddings for pipeline: {pipe['name']}")
        yield pipe, system_cfg, generate_embeddings_batches(pipe, system_cfg)
