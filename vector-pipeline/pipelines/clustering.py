# pipelines/clustering.py
from typing import Dict, List, Union
import yaml
import numpy as np
from sklearn.cluster import MiniBatchKMeans, KMeans

from services.utils.logger import setup_logger
from services.utils.qdrant_utils import get_qdrant_client, scroll_points_iter, set_payload_bulk
from services.utils.config_loader import load_system_config, load_clustering_config
from services.utils.pg_handler import PostgresHandler

logger = setup_logger("clustering_logger", log_file="logs/clustering.log", to_console=True)

def _load_payload_map(path="configs/payload_config.yml") -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("payload", {}).get("include_fields", {})

def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _build_model(algorithm: str, params: Dict):
    algo = algorithm.lower()
    if algo == "minibatchkmeans":
        return MiniBatchKMeans(
            n_clusters=int(params.get("n_clusters", 10)),
            batch_size=int(params.get("batch_size", 10000)),
            n_init=params.get("n_init", "auto"),
            max_iter=int(params.get("max_iter", 100)),
            random_state=int(params.get("random_state", 42)),
            verbose=0,
        )
    if algo == "kmeans":
        return KMeans(
            n_clusters=int(params.get("n_clusters", 10)),
            n_init=params.get("n_init", "auto"),
            max_iter=int(params.get("max_iter", 300)),
            random_state=int(params.get("random_state", 42)),
        )
    raise ValueError("Supported algorithms: minibatchkmeans, kmeans")

def run_clustering_from_config(config_path: str | None = None):
    system_cfg = load_system_config()
    cfg = load_clustering_config(config_path)
    cluster_cfg = cfg.get("clustering", {})

    collection = cluster_cfg.get("collection_name")
    algorithm = cluster_cfg.get("algorithm", "minibatchkmeans")
    params = cluster_cfg.get("params", {})
    io_cfg = cluster_cfg.get("io", {})
    results = cluster_cfg.get("results_table", {})

    run_clustering_and_persist(
        system_cfg=system_cfg,
        collection=collection,
        algorithm=algorithm,
        params=params,
        io_cfg=io_cfg,
        results_schema=results.get("schema", "analytics"),
        results_table=results.get("table", "reviews_with_clusters"),
    )

def run_clustering_and_persist(
    system_cfg: Dict,
    collection: str,
    algorithm: str,
    params: Dict,
    io_cfg: Dict,
    results_schema: str,
    results_table: str,
):
    qclient = get_qdrant_client(system_cfg.get("qdrant", {}))
    model = _build_model(algorithm, params)

    scroll_batch = int(io_cfg.get("scroll_batch", 5000))
    predict_batch = int(io_cfg.get("predict_batch", 10000))
    payload_batch = int(io_cfg.get("payload_batch", 2000))
    db_batch = int(io_cfg.get("db_batch", 5000))

    # 1) Train model with streaming and logs
    total_read = 0
    buffers: List[np.ndarray] = []
    for page in scroll_points_iter(
        qclient, collection=collection, batch_size=scroll_batch, with_vectors=True, with_payload=False
    ):
        X = np.array([pt.vector for pt in page], dtype=np.float32)
        total_read += len(X)
        logger.info(f"[CLUSTER] Read {total_read} vectors from Qdrant")

        if isinstance(model, MiniBatchKMeans):
            buffers.append(X)
            joined = np.vstack(buffers) if len(buffers) > 1 else X
            bs = int(params.get("batch_size", 10000))
            while len(joined) >= bs:
                model.partial_fit(joined[:bs])
                logger.info(f"[CLUSTER] partial_fit on {bs} samples (k={params.get('n_clusters', 10)})")
                joined = joined[bs:]
            buffers = [joined] if len(joined) else []
        else:
            buffers.append(X)

    if isinstance(model, MiniBatchKMeans):
        if buffers and len(buffers[0]) > 0:
            model.partial_fit(buffers[0])
            logger.info(f"[CLUSTER] partial_fit final on {len(buffers[0])} samples")
    else:
        if buffers:
            X_all = np.vstack(buffers)
            logger.info(f"[CLUSTER] Fitting full KMeans on {len(X_all)} samples")
            model.fit(X_all)

    # 2) Predict labels with payloads and logs
    ids_all: List[Union[int, str]] = []
    payloads_all: List[Dict] = []
    labels_all: List[int] = []
    total_pred = 0

    for page in scroll_points_iter(
        qclient, collection=collection, batch_size=scroll_batch, with_vectors=True, with_payload=True
    ):
        vecs = np.array([pt.vector for pt in page], dtype=np.float32)
        ids = [pt.id for pt in page]
        pays = [pt.payload or {} for pt in page]

        for idxs in _chunks(list(range(len(ids))), predict_batch):
            start, end = idxs[0], idxs[-1] + 1
            preds = model.predict(vecs[start:end]).tolist()
            ids_all.extend(ids[start:end])
            payloads_all.extend(pays[start:end])
            labels_all.extend(preds)
            total_pred += len(preds)
            logger.info(f"[CLUSTER] Predicted labels for {total_pred} vectors total")

    if not ids_all:
        logger.info(f"[CLUSTER] No vectors found to label in {collection}")
        return

    # 3) Batch update Qdrant payloads (cluster_label) using grouped updates
    # Group ids by label so we can set {"cluster_label": k} once per group (fast path)
    label_to_indices: Dict[int, List[int]] = {}
    for i, lbl in enumerate(labels_all):
        label_to_indices.setdefault(int(lbl), []).append(i)

    total_updated = 0
    for lbl, idxs in label_to_indices.items():
        # Further chunk ids to respect payload_batch and avoid oversized requests
        for sub in _chunks(idxs, payload_batch):
            batch_ids = [ids_all[i] for i in sub]
            set_payload_bulk(
                qclient,
                collection=collection,
                ids=batch_ids,
                payloads={"cluster_label": int(lbl)},  # same payload for the whole batch
                wait=True,
            )
            total_updated += len(batch_ids)
            logger.info(f"[CLUSTER] Updated payloads (cluster_label={lbl}) for {len(batch_ids)} points; total {total_updated}")

    # 4) Upsert results into Postgres (id + preexisting payload fields + cluster_label)
    pg_cfg = system_cfg["postgres"]
    pg = PostgresHandler(
        host=pg_cfg["host"], port=pg_cfg["port"], dbname=pg_cfg["database"],
        user=pg_cfg["user"], password=pg_cfg["password"],
    )
    try:
        payload_map = _load_payload_map()
        select_keys = list(payload_map.keys())

        rows = []
        for pid, lbl, pay in zip(ids_all, labels_all, payloads_all):
            row = {"id": pid, "cluster_label": int(lbl)}
            for k in select_keys:
                row[k] = pay.get(k)
            rows.append(row)

        total_db = 0
        for chunk in _chunks(rows, db_batch):
            pg.insert_reviews_batch(
                results_schema,
                results_table,
                chunk,
                on_conflict="ON CONFLICT (id) DO UPDATE SET cluster_label = EXCLUDED.cluster_label"
            )
            total_db += len(chunk)
            logger.info(f"[CLUSTER] Upserted {total_db} rows into {results_schema}.{results_table}")
    finally:
        pg.close()
