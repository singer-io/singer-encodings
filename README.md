# Singer Encodings

A Python library for reading various file formats (CSV, JSON, JSONL, Avro, Parquet, and Excel) commonly used in Singer taps and targets.

## Installation

```bash
pip install singer-encodings
```

## Supported Formats

- CSV
- JSON
- JSONL (JSON Lines)
- Avro
- Parquet
- Excel (.xlsx, .xls)

## Usage

### Excel Files

The Excel module supports reading `.xlsx` and `.xls` files with advanced features including hyperlink preservation, comment extraction, and automatic date/time normalization.

#### Basic Usage

```python
from singer_encodings.excel_reader import get_excel_row_iterator

# Open and read an Excel file
with open('data.xlsx', 'rb') as excel_file:
    row_iterator = get_excel_row_iterator(excel_file)

    if row_iterator:
        for sheet_name, row_dict in row_iterator:
            print(f"Sheet: {sheet_name}")
            print(f"Row: {row_dict}")
```

#### Read Specific Sheet

```python
from singer_encodings.excel_reader import get_excel_row_iterator

with open('data.xlsx', 'rb') as excel_file:
    options = {'sheet_name': 'Sales Data'}
    row_iterator = get_excel_row_iterator(excel_file, options=options)

    if row_iterator:
        for sheet_name, row_dict in row_iterator:
            print(f"Row from {sheet_name}: {row_dict}")
```

#### Validate Required Headers

```python
from singer_encodings.excel_reader import get_excel_row_iterator

with open('data.xlsx', 'rb') as excel_file:
    options = {
        'key_properties': ['id', 'email'],  # Required columns
        'date_overrides': ['created_at', 'updated_at']  # Expected date columns
    }

    try:
        row_iterator = get_excel_row_iterator(excel_file, options=options)
        if row_iterator:
            for sheet_name, row_dict in row_iterator:
                print(row_dict)
    except Exception as e:
        print(f"Validation error: {e}")
```

#### Filter by Catalog Headers

```python
from singer_encodings.excel_reader import get_excel_row_iterator

# Only include specific columns; others go to _sdc_extra
headers_in_catalog = ['id', 'name', 'email', 'created_at']

with open('data.xlsx', 'rb') as excel_file:
    row_iterator = get_excel_row_iterator(
        excel_file,
        headers_in_catalog=headers_in_catalog
    )

    if row_iterator:
        for sheet_name, row_dict in row_iterator:
            # Columns not in catalog are stored in _sdc_extra
            if '_sdc_extra' in row_dict:
                print(f"Extra data: {row_dict['_sdc_extra']}")
```

### Excel Features

#### Hyperlink Preservation

Cells with hyperlinks are represented as lists with structured data:

```python
# Cell with hyperlink returns:
{
    "website": [{"text": "Visit Site", "url": "https://example.com"}]
}
```

#### Comment Extraction

Cell comments (both legacy and threaded) are preserved:

```python
# Cell with comment returns:
{
    "notes": [{"text": "Important note", "comment": {"text": "Review this", "author": "John Doe"}}]
}

# Cell with hyperlink and comment:
{
    "link": [{"text": "Click here", "url": "https://example.com", "comment": {"text": "Updated link"}}]
}
```

#### Date/Time Normalization

All date and datetime values are automatically normalized to ISO-8601 format:

```python
# Datetime values: 'YYYY-MM-DDTHH:MM:SSZ'
# Date values: 'YYYY-MM-DDT00:00:00Z'
# Time values: 'HH:MM:SS'

# Example output:
{
    "created_at": "2024-01-15T14:30:00Z",  # From Excel datetime
    "birth_date": "1990-05-20T00:00:00Z",  # From Excel date
    "start_time": "09:30:00"                # From Excel time
}
```

#### Duplicate Headers

Duplicate headers are automatically detected and stored in `_sdc_extra`:

```python
# Excel with headers: id, name, email, name
# Returns:
{
    "id": "123",
    "name": "John",
    "email": "john@example.com",
    "_sdc_extra": [{"name": "Additional Name"}]
}
```

### CSV Files

```python
from singer_encodings.csv import get_row_iterator

csv_data = [b"name,email,age", b"John,john@example.com,30"]
row_iterator = get_row_iterator(csv_data)

for row in row_iterator:
    print(row)
# Output: {'name': 'John', 'email': 'john@example.com', 'age': '30'}
```

### JSONL Files

```python
from singer_encodings.jsonl import get_row_iterator

with open('data.jsonl', 'r') as jsonl_file:
    row_iterator = get_row_iterator(jsonl_file)

    for row in row_iterator:
        print(row)
```

### Parquet Files

```python
from singer_encodings.parquet import get_row_iterator

with open('data.parquet', 'rb') as parquet_file:
    row_iterator = get_row_iterator(parquet_file)

    for row in row_iterator:
        print(row)
```

## Development

### Running Tests

```bash
pytest tests/
```

### Running Specific Test

```bash
pytest tests/test_excel.py
```

## License

See LICENSE file for details.

## Contributing

Contributions are welcome! Please ensure:
- All tests pass
- New features include tests
- Update CHANGELOG.md with your changes
