# scripts/test_pipeline_run.py
from argparse import ArgumentParser
from services.utils.logger import setup_logger
from pipelines.embedding import run_embedding_pipelines
from pipelines.qdrant_storage import store_embeddings_in_qdrant
from pipelines.clustering import run_clustering_from_config

logger = setup_logger("test_runner_logger", log_file="logs/test_runner.log", to_console=True)

def main():
    parser = ArgumentParser(description="End-to-end test: embeddings -> Qdrant upsert -> clustering")
    parser.add_argument(
        "--clustering-config",
        help="Optional path to configs/clustering_config.yml; defaults to project config if omitted",
        default=None,
    )
    args = parser.parse_args()

    # 1) Embedding + Qdrant upsert (idempotent upsert by id)
    for pipe_cfg, system_cfg, batches in run_embedding_pipelines():
        logger.info(f"Running embedding and storage for pipeline: {pipe_cfg['name']}")
        store_embeddings_in_qdrant(pipe_cfg, system_cfg, batches)

    # 2) Clustering driven by YAML (MiniBatchKMeans and all batch sizes come from YAML)
    # If a custom config path is provided, pass it through; otherwise the function loads the default YAML.
    if args.clustering_config:
        run_clustering_from_config(args.clustering_config)
    else:
        run_clustering_from_config()

    logger.info("Test run completed successfully")

if __name__ == "__main__":
    # Standard Python entrypoint pattern to avoid side effects when imported
    main()
