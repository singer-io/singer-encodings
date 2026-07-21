# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.1]
* Fix `get_row_iterator()` in `parquet` module to decode via bounded batches instead of whole row groups, reducing peak memory usage on files with large row groups [#33](https://github.com/singer-io/singer-encodings/pull/33)

### Fixed
- `get_row_iterator(file_like_handle)` in `singer_encodings.parquet` - now uses `ParquetFile.iter_batches(batch_size=65536)` instead of `read_row_group().to_pylist()`, so peak memory stays proportional to one batch rather than to the size of the largest row group. Output (row order and content) is unchanged.

## [0.6.0]
* Add `sample_row_iterator()` and `is_empty()` to `parquet` module for memory-efficient discovery-time schema sampling [#32](https://github.com/singer-io/singer-encodings/pull/32)

### Added
- `sample_row_iterator(file_like_handle, sample_rate=5, max_records=1000)` in `singer_encodings.parquet` - filters rows at the Arrow level (via `Table.take()`) *before* converting them to Python objects, unlike `get_row_iterator()` combined with post-hoc filtering, which always converted an entire row group to Python objects regardless of `sample_rate`. Skips decompressing row groups with no sampled rows, and stops reading further row groups once `max_records` is reached.
- `is_empty(file_like_handle)` in `singer_encodings.parquet` - returns `True` if the Parquet file has no rows, reading only the footer metadata (no row group decompression).
- `get_row_iterator()` is unchanged and should still be used for full syncs, which need every row.

## [0.5.0]
* Bump `pyarrow` dependency to improve compatibility and performance [#31](https://github.com/singer-io/singer-encodings/pull/31)

## [0.4.0]
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

