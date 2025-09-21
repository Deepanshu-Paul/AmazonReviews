import datetime

def filter_row_to_columns(row, valid_columns, filename):
    """
    Filter dict to include only table columns except SERIAL column (id),
    add filename and loaddate fields dynamically.
    """
    filtered_row = {}
    current_time = datetime.datetime.now()
    for col in valid_columns:
        if col == "id":
            continue  # SERIAL primary key, auto-generated
        elif col == "filename":
            filtered_row[col] = filename
        elif col in ("loaddate", "created_at"):
            filtered_row[col] = current_time
        else:
            matched_key = None
            for key in row.keys():
                if key.lower().replace(" ", "_") == col.lower():
                    matched_key = key
                    break
            filtered_row[col] = row.get(matched_key, None)
    return filtered_row
