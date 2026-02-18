"""Excel reading utilities for Singer encodings.

This module provides helpers to read Excel workbooks in read-only mode and
produce JSON-friendly row dictionaries:

- Iterates all sheets (or a specific sheet) and yields `(sheet_name, row_dict)`.
- Builds row dicts keyed by headers; duplicate headers are captured under
    the special `_sdc_extra` field.
- Preserves cell hyperlinks as a list of objects with the shape
    `[{"text": "...", "url": "..."}]`.
- Preserves cell comments by adding a `comment` field in the same object,
    e.g. `[{"text": "...", "url": "...", "comment": "..."}]` or
    `[{"text": "...", "comment": "..."}]` when no hyperlink exists.
- Normalizes `datetime`, `date`, `time`, and common date-like strings to
    ISO-8601 using `convert_for_json`.

These conventions ensure downstream JSON serialization is stable and that
metadata like hyperlinks is not lost.
"""
from openpyxl import load_workbook
import singer
from datetime import datetime, date, time

# ----------------------------
# Constants
# ----------------------------
SDC_EXTRA_COLUMN = "_sdc_extra"
NO_HEADERS = "no_headers"

LOGGER = singer.get_logger()

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

    def convert_for_json(self, value):
        """Convert datetimes, dates, and common date-like strings to ISO-8601 with Z.

        - Datetime values: 'YYYY-MM-DDTHH:MM:SSZ' (assumes naive as UTC)
        - Date-only values: coerced to 'YYYY-MM-DDT00:00:00Z'
        - Time-only values: 'HH:MM:SS'
        """
        if isinstance(value, (datetime, date, time)):
            if isinstance(value, datetime):
                return value.strftime('%Y-%m-%dT%H:%M:%SZ')
            if isinstance(value, date):
                return datetime.combine(value, time.min).strftime('%Y-%m-%dT%H:%M:%SZ')
            return value.strftime('%H:%M:%S')

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

            # Try datetime patterns first (keep time component if present)
            for fmt in datetime_patterns:
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                except Exception:
                    pass

            # Then date-only patterns
            for fmt in date_patterns:
                try:
                    d = datetime.strptime(s, fmt).date()
                    return datetime.combine(d, time.min).strftime('%Y-%m-%dT%H:%M:%SZ')
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
                if isinstance(val, list):
                    return [convert_recursive(x) for x in val]
                elif isinstance(val, dict):
                    return {k: convert_recursive(v) for k, v in val.items()}
                return self.convert_for_json(val)

            yield convert_recursive(row_dict)


    def get_all_sheets_iterator(self, workbook_stream, headers_in_catalog=None, sheet_name=None, skip_empty=True):
        """Yield `(sheet_name, row_dict)` for all sheets (or a specific sheet).

        - Reads cell objects to preserve hyperlinks.
        - Skips fully empty rows when `skip_empty=True`.
        - Duplicate headers and non-catalog fields are captured in `_sdc_extra`.
        - Hyperlinked cells are represented as `[{"text": "...", "url": "..."}]`.
        - Cells with comments include a `comment` field in the same object.
        - Date-only values are coerced to 'YYYY-MM-DDT00:00:00'; datetimes keep time.
        """
        # Use non-read-only mode so hyperlink metadata is available on cells
        wb = load_workbook(workbook_stream, read_only=False, data_only=True)
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

            # Build a generator of processed row values, retaining hyperlinks and comments
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
                        # Extract comment data (text and author) if present
                        comment_data = None
                        try:
                            if c.comment:
                                comment_obj = {}
                                comment_text = getattr(c.comment, "text", None)

                                if comment_text:
                                    # Check if this is a threaded comment (modern Excel format)
                                    is_threaded = comment_text.startswith("[Threaded comment]")

                                    if is_threaded:
                                        # Parse threaded comments format
                                        # Format: [Threaded comment]\n\n...\nComment:\n    text\nReply:\n    reply text
                                        sections = []
                                        lines = comment_text.split('\n')

                                        i = 0
                                        while i < len(lines):
                                            line = lines[i].strip()

                                            # Look for "Comment:" or "Reply:" markers
                                            if line == "Comment:" or line == "Reply:":
                                                # Collect text until next marker or end
                                                comment_lines = []
                                                i += 1
                                                while i < len(lines):
                                                    next_line = lines[i].strip()
                                                    if next_line in ["Comment:", "Reply:", "Comment author:"]:
                                                        break
                                                    if next_line and not next_line.startswith("Your version of Excel"):
                                                        comment_lines.append(next_line)
                                                    i += 1

                                                if comment_lines:
                                                    sections.append({
                                                        "text": '\n'.join(comment_lines).strip(),
                                                        "author": None
                                                    })
                                                continue
                                            i += 1

                                        # Build comment object for threaded comments
                                        if sections:
                                            if len(sections) == 1:
                                                comment_obj["text"] = sections[0]["text"]
                                            else:
                                                comment_obj["text"] = sections[0]["text"]
                                                comment_obj["replies"] = [
                                                    {"text": s["text"], "author": s["author"]}
                                                    for s in sections[1:]
                                                ]
                                    else:
                                        # Parse old-style comments format
                                        # Pattern: any text followed by "\n\t-AuthorName"
                                        sections = []
                                        current_text = []
                                        lines = comment_text.split('\n')

                                        for line in lines:
                                            # Check if line is a signature (starts with whitespace and dash)
                                            stripped = line.lstrip()
                                            if stripped.startswith('-') and current_text:
                                                # Extract author name after the dash
                                                author_name = stripped[1:].strip()
                                                text_content = '\n'.join(current_text).strip()
                                                sections.append({
                                                    "text": text_content,
                                                    "author": author_name
                                                })
                                                current_text = []
                                            else:
                                                current_text.append(line)

                                        # Handle any remaining text without signature
                                        if current_text:
                                            remaining = '\n'.join(current_text).strip()
                                            if remaining:
                                                sections.append({
                                                    "text": remaining,
                                                    "author": None
                                                })

                                        # Get Excel metadata author
                                        excel_author = getattr(c.comment, "author", None)
                                        if excel_author and excel_author.lower() != "none":
                                            comment_obj["excel_author"] = excel_author

                                        # If we have parsed sections, use them
                                        if sections:
                                            if len(sections) == 1:
                                                # Single comment
                                                comment_obj["text"] = sections[0]["text"]
                                                if sections[0]["author"]:
                                                    comment_obj["author"] = sections[0]["author"]
                                            else:
                                                # Multiple comments/replies
                                                # First section is the main comment
                                                comment_obj["text"] = sections[0]["text"]
                                                if sections[0]["author"]:
                                                    comment_obj["author"] = sections[0]["author"]

                                                # Rest are replies
                                                if len(sections) > 1:
                                                    comment_obj["replies"] = [
                                                        {
                                                            "text": s["text"],
                                                            "author": s["author"]
                                                        } for s in sections[1:] if s["author"] or s["text"]
                                                    ]
                                        else:
                                            # No sections parsed, use full text
                                            comment_obj["text"] = comment_text

                                if comment_obj:
                                    comment_data = comment_obj
                        except (AttributeError, ImportError):
                            comment_data = None

                        val = c.value
                        if url:
                            # Preserve displayed text, hyperlink URL and optional comment, as a list of one object
                            obj = {"text": self.convert_for_json(val), "url": url}
                            if comment_data is not None:
                                obj["comment"] = comment_data
                            processed.append([obj])
                        elif comment_data is not None:
                            # Preserve text with comment when no hyperlink exists, as a list of one object
                            processed.append([{ "text": self.convert_for_json(val), "comment": comment_data }])
                        else:
                            # Convert date-like values and datetimes
                            processed.append(self.convert_for_json(val))
                    yield processed

            for row_dict in self._generate_dict_reader(processed_rows(), skip_empty=skip_empty):
                yield sn, row_dict
