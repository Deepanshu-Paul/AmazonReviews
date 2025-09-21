import psycopg2
from psycopg2.extras import execute_values

class PostgresHandler:
    def __init__(self, host, port, dbname, user, password):
        """
        Create a PostgreSQL connection using psycopg2 and enable autocommit.
        """
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self.conn.autocommit = True

    def get_table_columns(self, schema: str, table: str):
        """
        Return a list of column names for the given schema.table using information_schema.columns.
        """
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return [r[0] for r in cur.fetchall()]

    def insert_reviews_batch(self, schema: str, table: str, rows: list[dict], on_conflict: str | None = None):
        """
        Bulk insert rows (list of dicts) into schema.table using execute_values.
        Keys of the first dict determine the column order.
        Optionally pass an ON CONFLICT clause as raw SQL (e.g., 'ON CONFLICT DO NOTHING').
        """
        if not rows:
            return

        columns = list(rows[0].keys())
        values = [[row.get(col, None) for col in columns] for row in rows]
        cols_sql = ",".join(columns)

        conflict_sql = f" {on_conflict}" if on_conflict else ""

        sql = f"INSERT INTO {schema}.{table} ({cols_sql}) VALUES %s{conflict_sql};"

        with self.conn.cursor() as cur:
            execute_values(cur, sql, values)

    def close(self):
        self.conn.close()
