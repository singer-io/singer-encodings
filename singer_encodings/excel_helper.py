"""Excel reading utilities for Singer encodings.

This module provides helpers to read Excel workbooks in read-only mode and
produce JSON-friendly row dictionaries:

- Iterates all sheets (or a specific sheet) and yields `(sheet_name, row_dict)`.
- Builds row dicts keyed by headers; duplicate headers are captured under
    the special `_sdc_extra` field.
- Preserves cell hyperlinks as a list of objects with the shape
    `[{"text": "...", "url": "..."}]`.
- Normalizes `datetime`, `date`, `time`, and common date-like strings to
    ISO-8601 using `convert_for_json`.

These conventions ensure downstream JSON serialization is stable and that
metadata like hyperlinks is not lost.
"""
from openpyxl import load_workbook
import logging
from datetime import datetime, date, time

# ----------------------------
# Constants
# ----------------------------
SDC_EXTRA_COLUMN = "_sdc_extra"
NO_HEADERS = "no_headers"

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# ----------------------------
# ExcelHelper
# ----------------------------
class ExcelHelper:
    """Helper for reading Excel sheets and producing JSON-friendly rows.

    Responsibilities:
    - Track headers and detect duplicates per sheet.
    - Convert rows into dictionaries keyed by headers.
    - Capture duplicate or non-catalog headers in `_sdc_extra`.
    - Preserve hyperlinks and normalize date-like values to ISO strings.

    Attributes are reset per sheet when iterating a workbook.
    """
    def __init__(self):
        self.all_headers = []
        self.unique_headers = []
        self.unique_headers_idxs = []
        self.duplicate_headers = []
        self.dup_headers_idxs = []

    @staticmethod
    def generate_dict_from_zipped_data(zipped_data):
        dup_dictionary = {}
        for key, value in zipped_data:
            if key in dup_dictionary:
                if not isinstance(dup_dictionary[key], list):
                    dup_dictionary[key] = [dup_dictionary[key], value]
                else:
                    dup_dictionary[key].append(value)
            else:
                dup_dictionary[key] = value
        return dup_dictionary

    @staticmethod
    def convert_for_json(value):
        """Convert datetime/date/time to JSON-serializable string"""
        if isinstance(value, (datetime, date, time)):
            return value.isoformat()

        # Try to parse common date/datetime string formats to ISO
        if isinstance(value, str):
            s = value.strip()
            # Datetime patterns (with time component)
            datetime_patterns = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%d-%b-%y %H:%M:%S",
                "%d-%b-%y %H:%M",
                "%d-%b-%Y %H:%M:%S",
                "%d-%b-%Y %H:%M",
                "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
            ]
            date_patterns = [
                "%Y-%m-%d",
                "%d-%b-%y",   # e.g., 15-Aug-25
                "%d-%b-%Y",
                "%d %b %Y",
                "%b %d, %Y",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%m/%d/%y",
                "%d/%m/%y",
                "%d-%m-%Y",
                "%d-%m-%y",
                "%Y/%m/%d",
            ]

            # Try datetime patterns first
            for fmt in datetime_patterns:
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.isoformat()
                except Exception:
                    pass

            # Then date-only patterns
            for fmt in date_patterns:
                try:
                    d = datetime.strptime(s, fmt).date()
                    return d.isoformat()
                except Exception:
                    pass

        return value

    def _generate_dict_reader(self, rows, skip_empty=True):
        """Convert rows to dictionaries, handle duplicates and `_sdc_extra`.

        - Rows are lists of cell values (already processed for hyperlinks).
        - Values are recursively converted to ISO where applicable.
        - Excess values (when row has more items than headers) are stored
            under `_sdc_extra` with the `no_headers` key.
        """
        for row in rows:
            row = list(row)

            if skip_empty and all(v is None for v in row):
                continue  # skip completely empty rows

            row_length = len(row)
            header_length = len(self.all_headers)
            row_dict = {}
            sdc_extra_values = []

            if self.dup_headers_idxs:
                uniq_idxs = [i for i in self.unique_headers_idxs if i < row_length]
                dup_idxs = [i for i in self.dup_headers_idxs if i < row_length]

                row_dict = dict(zip(self.unique_headers, map(row.__getitem__, uniq_idxs)))

                dup_values = list(map(row.__getitem__, dup_idxs))
                dup_zipped = zip(self.duplicate_headers, dup_values)
                dup_dictionary = self.generate_dict_from_zipped_data(dup_zipped)

                sdc_extra_values.extend([{k: v} for k, v in dup_dictionary.items()])
            else:
                if row_length > header_length:
                    row_dict = dict(zip(self.all_headers, row[:header_length]))
                    sdc_extra_values.append({NO_HEADERS: row[header_length:]})
                else:
                    row_dict = dict(zip(self.all_headers, row))

            if sdc_extra_values:
                row_dict[SDC_EXTRA_COLUMN] = sdc_extra_values

            # Recursively convert all values to JSON-serializable
            def convert_recursive(val):
                if isinstance(val, (datetime, date, time)):
                    return val.isoformat()
                elif isinstance(val, list):
                    return [convert_recursive(x) for x in val]
                elif isinstance(val, dict):
                    return {k: convert_recursive(v) for k, v in val.items()}
                return val

            yield convert_recursive(row_dict)


    def get_all_sheets_iterator(self, workbook_stream, headers_in_catalog=None, sheet_name=None, skip_empty=True):
        """Yield `(sheet_name, row_dict)` for all sheets (or a specific sheet).

        - Reads cell objects to preserve hyperlinks.
        - Skips fully empty rows when `skip_empty=True`.
        - Duplicate headers and non-catalog fields are captured in `_sdc_extra`.
        - Hyperlinked cells are represented as `[{"text": "...", "url": "..."}]`.
        - Date-like values and datetimes are normalized to ISO strings.
        """
        wb = load_workbook(workbook_stream, read_only=True, data_only=True)
        sheetnames = [sheet_name] if sheet_name else wb.sheetnames

        for sn in sheetnames:
            LOGGER.info("Reading sheet: %s", sn)
            ws = wb[sn]
            # Read cell objects to preserve hyperlink info
            rows_cells = ws.iter_rows(values_only=False)

            # Reset header tracking per sheet
            self.all_headers = []
            self.unique_headers = []
            self.unique_headers_idxs = []
            self.duplicate_headers = []
            self.dup_headers_idxs = []

            try:
                header_cells = next(rows_cells)
                self.all_headers = [str(h.value) if getattr(h, "value", None) is not None else "" for h in header_cells]
            except StopIteration:
                LOGGER.warning("Sheet '%s' is empty, skipping", sn)
                continue

            for idx, header in enumerate(self.all_headers):
                not_in_catalog = headers_in_catalog and header not in headers_in_catalog
                if header in self.unique_headers or not_in_catalog:
                    if not_in_catalog and header not in self.duplicate_headers:
                        LOGGER.warning("\"%s\" field not in catalog, stored in _sdc_extra", header)
                    self.duplicate_headers.append(header)
                    self.dup_headers_idxs.append(idx)
                else:
                    self.unique_headers.append(header)
                    self.unique_headers_idxs.append(idx)

            if self.dup_headers_idxs:
                LOGGER.warning(
                    "Duplicate Header(s) %s found in sheet '%s', stored in _sdc_extra",
                    set(self.duplicate_headers),
                    sn,
                )

            # Build a generator of processed row values, retaining hyperlinks
            def processed_rows():
                for r in rows_cells:
                    processed = []
                    for c in r:
                        # Extract hyperlink target if present
                        url = None
                        try:
                            if c.hyperlink:
                                url = getattr(c.hyperlink, "target", None) or getattr(c.hyperlink, "location", None)
                        except AttributeError:
                            url = None

                        val = c.value
                        if url:
                            # Preserve both displayed text and hyperlink URL, as a list of objects
                            processed.append([{"text": self.convert_for_json(val), "url": url}])
                        else:
                            # Convert date-like values and datetimes
                            processed.append(self.convert_for_json(val))
                    yield processed

            for row_dict in self._generate_dict_reader(processed_rows(), skip_empty=skip_empty):
                yield sn, row_dict
