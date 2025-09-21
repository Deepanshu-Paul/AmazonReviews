import io
import csv

class CSVParser:
    @staticmethod
    def parse_csv_string(csv_content, max_rows=None):
        reader = csv.DictReader(io.StringIO(csv_content, newline=''))
        rows = []
        for i, row in enumerate(reader):
            if max_rows and i >= max_rows:
                break
            rows.append(row)
        return rows
