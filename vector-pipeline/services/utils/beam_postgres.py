import logging
import apache_beam as beam
from services.utils.pg_handler import PostgresHandler

# Use the same logger name as the rest of your pipeline
logger = logging.getLogger("beam_ingestion_logger")

class WriteToPostgres(beam.DoFn):
    def __init__(self, host, port, dbname, user, password, schema, table, batch_size=1000):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.schema = schema
        self.table = table
        self.batch_size = batch_size
        self.pg_handler = None
        self.batch = []

    def setup(self):
        # Initialize Postgres handler once per worker
        self.pg_handler = PostgresHandler(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        logger.info("PostgresHandler initialized in WriteToPostgres DoFn")

    def process(self, element):
        # Collect rows into the batch
        self.batch.append(element)
        if len(self.batch) >= self.batch_size:
            self._flush_batch()

    def finish_bundle(self):
        # Flush any remaining rows at the end of bundle
        if self.batch:
            self._flush_batch()

    def _flush_batch(self):
        count = len(self.batch)
        try:
            self.pg_handler.insert_reviews_batch(self.schema, self.table, self.batch)
            logger.info(f"[WriteToPostgres] Flushed batch of {count} rows into {self.schema}.{self.table}")
        except Exception as e:
            logger.error(f"[WriteToPostgres] Failed to insert batch of {count} rows: {e}", exc_info=True)
        finally:
            self.batch = []

    def teardown(self):
        # Clean up connection
        if self.pg_handler:
            self.pg_handler.close()
            logger.info("PostgresHandler connection closed in WriteToPostgres DoFn")
