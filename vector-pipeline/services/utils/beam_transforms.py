import csv
import io
import datetime
import apache_beam as beam
from services.utils.pg_handler import PostgresHandler

class ParseCSVDoFn(beam.DoFn):
    def process(self, element):
        filename, csv_content = element
        reader = csv.DictReader(io.StringIO(csv_content, newline=''))
        for row in reader:
            yield (filename, row)

class FilterToTableDoFn(beam.DoFn):
    def __init__(self, host, port, dbname, user, password, schema, table):
        # Store connection params, not the connection itself
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.schema = schema
        self.table = table
        self.valid_columns = None

    def setup(self):
        # Create connection per worker
        pg = PostgresHandler(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.valid_columns = pg.get_table_columns(self.schema, self.table)
        pg.close()

    def process(self, element):
        filename, row = element
        now = datetime.datetime.utcnow()
        filtered = {}
        for col in self.valid_columns:
            if col == "id":
                continue
            if col == "filename":
                filtered[col] = filename
            elif col in ("loaddate", "created_at"):
                filtered[col] = now
            else:
                key = next((k for k in row if k.lower().replace(" ", "_") == col.lower()), None)
                filtered[col] = row.get(key) if key else None
        yield filtered
