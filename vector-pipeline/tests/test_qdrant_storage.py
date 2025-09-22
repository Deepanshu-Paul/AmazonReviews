# tests/test_qdrant_storage.py
from services.utils.config_loader import load_system_config
from pipelines.embedding import generate_embeddings_batches
from pipelines.qdrant_storage import store_embeddings_in_qdrant
from qdrant_client import QdrantClient

def test_qdrant_upsert_and_scroll(pg_container, monkeypatch):
    sys_cfg = load_system_config()
    from tests.utils import seed_reviews
    seed_reviews(sys_cfg["postgres"])

    # Point to a disposable collection; use cloud creds from system_config.yml
    pipe_cfg = {
        "name":"reviews_embeddings",
        "source_schema":"customer_clean","source_table":"reviews",
        "text_fields":["Review Title","Review Text"],
        "fields_map":{"Review Title":"review_title","Review Text":"review_text"},
        "model":{"name":"sentence-transformers/all-MiniLM-L6-v2","batch_size":8,"normalize":True},
        "qdrant":{"collection_name":"test_reviews_tmp","distance_metric":"Cosine"},
    }

    batches = generate_embeddings_batches(pipe_cfg, sys_cfg)
    store_embeddings_in_qdrant(pipe_cfg, sys_cfg, batches)

    client = QdrantClient(url=sys_cfg["qdrant"]["url"], api_key=sys_cfg["qdrant"]["api_key"])
    # scroll back a few points
    result, _ = client.scroll(collection_name=pipe_cfg["qdrant"]["collection_name"], limit=10, with_vectors=False, with_payload=True)
    # Assert 2 points upserted and payload present
    assert len(result) >= 2
    assert all(pt.payload is not None for pt in result)
