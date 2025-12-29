from excel_helper import ExcelHelper

def get_excel_row_iterator(
    iterable,
    options=None,
    headers_in_catalog=None,
):
    """
    Returns an iterator over all rows in Excel.
    If sheet_name is provided in options, returns only that sheet.
    Converts datetime/date values automatically for JSON.
    """
    options = options or {}

    excel_helper = ExcelHelper()
    reader = excel_helper.get_all_sheets_iterator(
        workbook_stream=iterable,
        sheet_name=options.get("sheet_name"),
        headers_in_catalog=headers_in_catalog
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
