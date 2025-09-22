# tests/test_clustering.py
from services.utils.config_loader import load_system_config
from tests.utils import seed_reviews
from pipelines.embedding import generate_embeddings_batches
from pipelines.qdrant_storage import store_embeddings_in_qdrant
from pipelines.clustering import run_clustering_and_persist
from qdrant_client import QdrantClient
from services.utils.pg_handler import PostgresHandler

def test_clustering_updates_payload_and_table(pg_container):
    sys_cfg = load_system_config()
    seed_reviews(sys_cfg["postgres"])

    pipe_cfg = {
        "name":"reviews_embeddings",
        "source_schema":"customer_clean","source_table":"reviews",
        "text_fields":["Review Title","Review Text"],
        "fields_map":{"Review Title":"review_title","Review Text":"review_text"},
        "model":{"name":"sentence-transformers/all-MiniLM-L6-v2","batch_size":8,"normalize":True},
        "qdrant":{"collection_name":"test_reviews_cluster","distance_metric":"Cosine"},
    }
    # upsert points
    store_embeddings_in_qdrant(pipe_cfg, sys_cfg, generate_embeddings_batches(pipe_cfg, sys_cfg))
    # run clustering (kmeans=2) and persist
    run_clustering_and_persist(
        system_cfg=sys_cfg,
        collection=pipe_cfg["qdrant"]["collection_name"],
        algo="kmeans",
        n_clusters=2,
        results_schema="analytics",
        results_table="reviews_with_clusters"
    )

    # Verify payload updated with cluster_label
    client = QdrantClient(url=sys_cfg["qdrant"]["url"], api_key=sys_cfg["qdrant"]["api_key"])
    items, _ = client.scroll(collection_name=pipe_cfg["qdrant"]["collection_name"], limit=100, with_vectors=False, with_payload=True)
    assert len(items) >= 2
    assert all("cluster_label" in (pt.payload or {}) for pt in items)

    # Verify persisted table has 2 rows and cluster_label column
    pg_cfg = sys_cfg["postgres"]
    pg = PostgresHandler(host=pg_cfg["host"], port=pg_cfg["port"], dbname=pg_cfg["database"], user=pg_cfg["user"], password=pg_cfg["password"])
    try:
        with pg.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS analytics.reviews_with_clusters (
                    id TEXT PRIMARY KEY,
                    cluster_label INTEGER,
                    reviewer_name TEXT,
                    country TEXT,
                    rating INTEGER,
                    review_date DATE,
                    experience_date DATE,
                    title TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("SELECT COUNT(*) FROM analytics.reviews_with_clusters")
            count = cur.fetchone()[0]
        assert count >= 2
    finally:
        pg.close()
