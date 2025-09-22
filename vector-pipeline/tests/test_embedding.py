# tests/test_embedding.py
from services.utils.config_loader import load_system_config
from pipelines.embedding import generate_embeddings_batches

def test_embedding_generates_points(pg_container):
    # system config picks up env from container fixture
    sys_cfg = load_system_config()
    # seed data
    from tests.utils import seed_reviews
    seed_reviews(sys_cfg["postgres"])
    # minimal pipeline cfg mirrors your YAML
    pipe_cfg = {
        "name": "reviews_embeddings",
        "source_schema": "customer_curation",
        "source_table": "reviews",
        "text_fields": ["Review Title","Review Text"],
        "fields_map": {"Review Title":"review_title","Review Text":"review_text"},
        "model": {"name":"sentence-transformers/all-MiniLM-L6-v2","batch_size": 8,"normalize": True},
        "qdrant": {"collection_name":"test_reviews","distance_metric":"Cosine"},
    }

    batches = list(generate_embeddings_batches(pipe_cfg, sys_cfg))
    # Assert at least one batch and correct vector dimension across points
    assert len(batches) >= 1
    total = sum(len(points) for points, _ in batches)
    assert total == 2  # we seeded 2 rows
    # vectors present and payload has metadata fields (not embedded)
    for points, dim in batches:
        assert dim in (384, 768, 1024)  # model dependent
        for p in points:
            assert isinstance(p.vector, list) and len(p.vector) == dim
            assert "rating" in (p.payload or {})  # comes from payload_config.yml
