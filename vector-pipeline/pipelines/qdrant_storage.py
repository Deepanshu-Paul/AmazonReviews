# pipelines/qdrant_storage.py
from typing import Dict, Iterable, Tuple, List
from services.utils.logger import setup_logger
from services.utils.qdrant_utils import get_qdrant_client, ensure_collection, upsert_points

logger = setup_logger("qdrant_storage_logger", log_file="logs/qdrant_storage.log", to_console=True)

def store_embeddings_in_qdrant(pipeline_cfg: Dict, system_cfg: Dict, batches: Iterable[Tuple[List, int]]):
    q_cfg = system_cfg.get("qdrant", {})
    client = get_qdrant_client(q_cfg)
    collection = pipeline_cfg["qdrant"]["collection_name"]
    distance = pipeline_cfg["qdrant"].get("distance_metric", "Cosine")

    created = False
    total = 0
    for points, dim in batches:
        if not created:
            ensure_collection(client, collection, vector_size=dim, distance=distance)
            created = True
            logger.info(f"Ensured collection {collection} (dim={dim}, distance={distance})")
        if points:
            upsert_points(client, collection, points, wait=True)
            total += len(points)
            logger.info(f"Upserted batch of {len(points)} into {collection}")
    logger.info(f"Total upserted to {collection}: {total}")
