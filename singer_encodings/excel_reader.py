"""Public API for iterating Excel rows via ExcelHelper.

Produces a generator over `(sheet_name, row_dict)` with:
- ISO-normalized date/datetime/time and common date-like strings.
- Hyperlink preservation per cell as `[{"text": "...", "url": "..."}]`.
- Optional validation of `key_properties` and `date_overrides` against headers.

Returns `None` when the Excel file contains no rows.
"""
from .excel_helper import ExcelHelper

def get_excel_row_iterator(
    iterable,
    options=None,
    headers_in_catalog=None,
):
    """Return a generator over `(sheet_name, row_dict)` for Excel rows.

    Options:
    - `sheet_name`: read only the named sheet.
    - `key_properties`: set of required headers; raises if missing.
    - `date_overrides`: headers expected to be dates; raises if missing.

    Behavior:
    - Values are normalized to ISO (dates/times and common date strings).
    - Hyperlinks are preserved as `[{"text": "...", "url": "..."}]`.
    - Returns `None` if the workbook is empty.
    """
    options = options or {}

    excel_helper = ExcelHelper()
    reader = excel_helper.get_all_sheets_iterator(
        workbook_stream=iterable,
        sheet_name=options.get("sheet_name"),
        headers_in_catalog=headers_in_catalog,
    )

    try:
        first_sheet_name, first_row = next(reader)
    except StopIteration:
        return None  # Excel file is empty

    headers = set(excel_helper.unique_headers)

    if options.get("key_properties"):
        key_props = set(options["key_properties"])
        if not key_props.issubset(headers):
            raise Exception(
                "Excel file missing required headers: {}".format(
                    key_props - headers
                )
            )

    if options.get("date_overrides"):
        date_overrides = set(options["date_overrides"])
        if not date_overrides.issubset(headers):
            raise Exception(
                "Excel file missing date_overrides headers: {}".format(
                    date_overrides - headers
                )
            )

    def generator():
        yield first_sheet_name, first_row
        for item in reader:
            yield item

    return generator()
