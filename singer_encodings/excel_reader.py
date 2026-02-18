"""Public API for iterating Excel rows via ExcelHelper.

Produces a generator over `(sheet_name, row_dict)` with:
- ISO-normalized date/datetime/time and common date-like strings.
- Hyperlink preservation per cell as `[{"text": "...", "url": "..."}]`.
- Optional validation of `key_properties` and `date_overrides` against headers.

Returns `None` when the Excel file contains no rows.
"""
from typing import Generator, Tuple, Dict, Any, Optional, List
from .excel_helper import ExcelHelper

def get_excel_row_iterator(
    iterable,
    options: Optional[Dict[str, Any]] = None,
    headers_in_catalog: Optional[List[str]] = None,
) -> Optional[Generator[Tuple[str, dict], None, None]]:
    """Return a generator over (sheet_name, row_dict) for Excel rows.

    Args:
        iterable: File-like object or path to Excel file (.xlsx, .xls)
        options: Optional configuration dict with keys:
            - sheet_name (str): Read only the named sheet
            - key_properties (list): Required headers; raises ValueError if missing
            - date_overrides (list): Headers expected to be dates; raises ValueError if missing
        headers_in_catalog: List of headers to include in main dict; others go to _sdc_extra

    Returns:
        Generator yielding (sheet_name, row_dict) tuples, or None if workbook is empty

    Raises:
        ValueError: If key_properties or date_overrides validation fails, or sheet_name not found
        IOError: If Excel file cannot be read

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
            missing = sorted(key_props - headers)
            available = sorted(headers)
            raise ValueError(
                f"Excel file missing required headers: {missing}. "
                f"Available headers: {available}"
            )

    if options.get("date_overrides"):
        date_overrides = set(options["date_overrides"])
        if not date_overrides.issubset(headers):
            missing = sorted(date_overrides - headers)
            available = sorted(headers)
            raise ValueError(
                f"Excel file missing date_overrides headers: {missing}. "
                f"Available headers: {available}"
            )

    def generator():
        yield first_sheet_name, first_row
        for item in reader:
            yield item

    return generator()
