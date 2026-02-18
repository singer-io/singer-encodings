# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1]
* Add Excel file support [#30](https://github.com/singer-io/singer-encodings/pull/30)

### Added
- Excel file support (.xlsx, .xls) via new `excel_reader` module
- `ExcelHelper` class for reading and processing Excel workbooks
- `get_excel_row_iterator()` function to iterate over Excel rows with sheet support
- Hyperlink preservation in Excel cells as structured data `[{"text": "...", "url": "..."}]`
- Cell comment extraction with support for both legacy and threaded comment formats
- Automatic ISO-8601 datetime normalization for date, time, and datetime values
- Duplicate header detection and handling via `_sdc_extra` column
- Support for multiple sheets with sheet-specific row iteration
- Validation for `key_properties` and `date_overrides` against Excel headers
- `openpyxl==3.1.5` dependency for Excel file parsing
- Comprehensive test suite with 19 tests for Excel functionality
- README documentation with usage examples for Excel files
- Example scripts demonstrating Excel reader capabilities

## [0.3.0] - Previous Release
- Existing CSV, JSON, JSONL, Avro, and Parquet support
