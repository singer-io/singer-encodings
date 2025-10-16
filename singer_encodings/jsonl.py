import json

def get_row_iterator(file_like_handle):
    for row in file_like_handle:
        if isinstance(row, bytes):
            row = row.decode('utf-8')
        if row.strip():
            parsed_row = json.loads(row)
            if len(parsed_row):
                yield parsed_row
