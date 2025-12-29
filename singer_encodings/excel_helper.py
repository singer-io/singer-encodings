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
        return value

    def _generate_dict_reader(self, rows, skip_empty=True):
        """Convert rows to dictionaries, handle duplicates and _sdc_extra"""
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
        """
        Read all sheets from Excel (or a specific sheet) and yield tuples (sheet_name, row_dict)
        """
        wb = load_workbook(workbook_stream, read_only=True, data_only=True)
        sheetnames = [sheet_name] if sheet_name else wb.sheetnames

        for sn in sheetnames:
            LOGGER.info("Reading sheet: %s", sn)
            ws = wb[sn]
            rows = ws.iter_rows(values_only=True)

            # Reset header tracking per sheet
            self.all_headers = []
            self.unique_headers = []
            self.unique_headers_idxs = []
            self.duplicate_headers = []
            self.dup_headers_idxs = []

            try:
                self.all_headers = [str(h) if h is not None else "" for h in next(rows)]
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

            for row_dict in self._generate_dict_reader(rows, skip_empty=skip_empty):
                yield sn, row_dict
