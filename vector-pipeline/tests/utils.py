# tests/utils.py
from services.utils.pg_handler import PostgresHandler

def seed_reviews(pg_cfg, schema="customer_curation", table="reviews_curated"):
    pg = PostgresHandler(
        host=pg_cfg["host"], port=pg_cfg["port"],
        dbname=pg_cfg["database"], user=pg_cfg["user"], password=pg_cfg["password"]
    )
    with pg.conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                id TEXT PRIMARY KEY,
                review_title TEXT,
                review_text  TEXT,
                reviewer_name TEXT,
                country TEXT,
                rating INTEGER,
                review_date DATE,
                date_of_experience DATE
            )
        """)
        cur.execute(f"DELETE FROM {schema}.{table}")
        cur.execute(f"""
            INSERT INTO {schema}.{table} (id, review_title, review_text, reviewer_name, country, rating, review_date, date_of_experience)
            VALUES
            ('r1','Great','Loved it','Alice','US',5,'2024-01-10','2024-01-09'),
            ('r2','Bad','Hated it','Bob','GB',1,'2024-02-11','2024-02-11')
        """)
    pg.close()
