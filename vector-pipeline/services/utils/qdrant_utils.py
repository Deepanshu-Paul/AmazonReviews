# services/utils/qdrant_utils.py
from typing import List, Dict, Iterable, Tuple, Optional, Union
from collections import defaultdict
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, Distance, VectorParams

def get_qdrant_client(qdrant_cfg: Dict) -> QdrantClient:
    """
    Build a Qdrant client with Cloud-safe defaults:
    - timeout: increase default REST timeout for long operations
    - port: allow explicit None to avoid appending :6333 to Cloud URLs
    """
    return QdrantClient(
        url=qdrant_cfg.get("url"),
        api_key=qdrant_cfg.get("api_key"),
        prefer_grpc=qdrant_cfg.get("prefer_grpc", False),
        port=qdrant_cfg.get("port", None),          # avoid :6333 on cloud URLs
        timeout=qdrant_cfg.get("timeout", 60),      # raise if needed for big batches
    )  # timeout/port are supported client params [web:242][web:384]

def _distance_matches(current_distance: Union[str, Distance, None], desired: str) -> bool:
    # Distance may be an enum; compare by normalized string suffix
    if current_distance is None:
        return False
    s = str(current_distance)
    return s.lower().endswith(desired.lower())

def ensure_collection(client: QdrantClient, collection: str, vector_size: int, distance: str = "Cosine"):
    """
    Idempotently ensure a collection exists with the expected vector size and distance.
    If it exists with a mismatched schema, drop and recreate it.
    """
    try:
        exists = client.collection_exists(collection_name=collection)
    except AttributeError:
        try:
            client.get_collection(collection_name=collection)
            exists = True
        except Exception:
            exists = False  # [web:245][web:329]

    if exists:
        try:
            info = client.get_collection(collection_name=collection)
            current = info.result.config.params.vectors
            if hasattr(current, "size") and hasattr(current, "distance"):
                same_size = (current.size == vector_size)
                same_dist = _distance_matches(current.distance, distance)
                if same_size and same_dist:
                    return
            client.delete_collection(collection_name=collection)
        except Exception:
            client.delete_collection(collection_name=collection)

    client.create_collection(
        collection_name=collection,
        vectors_config=VectorParams(size=vector_size, distance=Distance(distance.capitalize())),
        timeout=60,
    )  # create/exists/delete semantics per collections API [web:245][web:323]

def recreate_collection(client: QdrantClient, collection: str, vector_size: int, distance: str = "Cosine"):
    client.recreate_collection(
        collection_name=collection,
        vectors_config=VectorParams(size=vector_size, distance=Distance(distance.capitalize())),
        timeout=60,
    )  # convenience; not used in idempotent flow [web:245]

def upsert_points(client: QdrantClient, collection: str, points: List[PointStruct], wait: bool = True):
    return client.upsert(collection_name=collection, points=points, wait=wait)  # idempotent by id [web:219]

def set_payload_bulk(
    client: QdrantClient,
    collection: str,
    ids: List[Union[int, str]],
    payloads: Union[Dict, List[Dict]],
    wait: bool = True,
):
    """
    - If 'payloads' is a dict, set the same payload for all ids in one call.
    - If 'payloads' is a list of dicts (per-id payload), apply per-point updates (slower).
    Prefer grouping many ids under the same payload (see helper below).
    """
    if isinstance(payloads, Dict):
        client.set_payload(collection_name=collection, points=ids, payload=payloads, wait=wait)
        return
    for pid, pl in zip(ids, payloads):
        client.set_payload(collection_name=collection, points=[pid], payload=pl, wait=wait)  # set payload API [web:244]

def scroll_points_iter(
    client: QdrantClient,
    collection: str,
    batch_size: int = 2000,
    with_vectors: bool = True,
    with_payload: bool = True,
    offset: Optional[Union[int, str]] = None,
):
    next_page = offset
    while True:
        result, next_page = client.scroll(
            collection_name=collection,
            limit=batch_size,
            with_vectors=with_vectors,
            with_payload=with_payload,
            offset=next_page,
        )
        if not result:
            break
        yield result
        if next_page is None:
            break  # scroll API returns pages + next offset [web:238]

# Optional helper (keeps existing API intact) to avoid timeouts:
def set_cluster_label_grouped(
    client: QdrantClient,
    collection: str,
    ids: List[Union[int, str]],
    labels: List[int],
    wait: bool = True,
):
    """
    Efficiently set {"cluster_label": k} once per cluster:
    groups ids by label and calls set_payload once per group.
    """
    groups: Dict[int, List[Union[int, str]]] = defaultdict(list)
    for pid, lbl in zip(ids, labels):
        groups[int(lbl)].append(pid)
    for lbl, grp_ids in groups.items():
        client.set_payload(
            collection_name=collection,
            payload={"cluster_label": int(lbl)},
            points=grp_ids,
            wait=wait,
        )  # single payload for many ids per docs [web:244][web:220]
