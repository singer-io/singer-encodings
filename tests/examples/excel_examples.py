"""
Example usage of singer-encodings Excel reader.

This example demonstrates how to read Excel files with various features:
- Reading all sheets or specific sheets
- Handling hyperlinks and comments
- Date/time normalization
- Duplicate header handling
- Key properties validation
"""

from singer_encodings.excel_reader import get_excel_row_iterator
from openpyxl import Workbook
from openpyxl.comments import Comment
from datetime import datetime, date
import tempfile
import os


def create_sample_excel():
    """Create a sample Excel file with multiple features."""
    temp_file = tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False)
    temp_file.close()

    wb = Workbook()

    # Sheet 1: Employee data
    ws1 = wb.active
    ws1.title = "Employees"
    ws1.append(["id", "name", "email", "hire_date", "department"])

    ws1.append([1, "John Doe", "john@example.com", datetime(2020, 1, 15), "Engineering"])
    ws1.append([2, "Jane Smith", "jane@example.com", datetime(2021, 3, 20), "Sales"])
    ws1.append([3, "Bob Johnson", "bob@example.com", datetime(2019, 11, 5), "Marketing"])

    # Add a comment to one cell
    comment = Comment("Star performer", "Manager")
    ws1['B2'].comment = comment

    # Add a hyperlink to email
    ws1['C2'].hyperlink = "mailto:john@example.com"

    # Sheet 2: Sales data
    ws2 = wb.create_sheet("Sales")
    ws2.append(["sale_id", "employee_id", "amount", "sale_date"])
    ws2.append([101, 1, 1500.00, datetime(2024, 1, 10)])
    ws2.append([102, 2, 2300.50, datetime(2024, 1, 15)])
    ws2.append([103, 1, 1800.75, datetime(2024, 1, 20)])

    wb.save(temp_file.name)
    wb.close()

    return temp_file.name


def example_1_basic_reading():
    """Example 1: Basic Excel reading - all sheets."""
    print("=" * 60)
    print("Example 1: Reading all sheets from Excel file")
    print("=" * 60)

    excel_file = create_sample_excel()

    try:
        with open(excel_file, 'rb') as f:
            row_iterator = get_excel_row_iterator(f)

            if row_iterator:
                for sheet_name, row_dict in row_iterator:
                    print(f"\nSheet: {sheet_name}")
                    print(f"Row: {row_dict}")
            else:
                print("Excel file is empty")
    finally:
        os.unlink(excel_file)


def example_2_specific_sheet():
    """Example 2: Reading a specific sheet only."""
    print("\n" + "=" * 60)
    print("Example 2: Reading specific sheet (Sales)")
    print("=" * 60)

    excel_file = create_sample_excel()

    try:
        with open(excel_file, 'rb') as f:
            options = {'sheet_name': 'Sales'}
            row_iterator = get_excel_row_iterator(f, options=options)

            if row_iterator:
                for sheet_name, row_dict in row_iterator:
                    print(f"\nSheet: {sheet_name}")
                    print(f"Sale ID: {row_dict.get('sale_id')}")
                    print(f"Amount: ${row_dict.get('amount')}")
                    print(f"Date: {row_dict.get('sale_date')}")
    finally:
        os.unlink(excel_file)


def example_3_key_properties():
    """Example 3: Validating required key properties."""
    print("\n" + "=" * 60)
    print("Example 3: Validating key properties")
    print("=" * 60)

    excel_file = create_sample_excel()

    try:
        with open(excel_file, 'rb') as f:
            # Require 'id' and 'email' columns
            options = {
                'sheet_name': 'Employees',
                'key_properties': ['id', 'email']
            }

            row_iterator = get_excel_row_iterator(f, options=options)

            if row_iterator:
                print("\n✓ Key properties validation passed!")
                for sheet_name, row_dict in row_iterator:
                    print(f"ID: {row_dict['id']}, Email: {row_dict['email']}")
    except Exception as e:
        print(f"\n✗ Validation failed: {e}")
    finally:
        os.unlink(excel_file)


def example_4_date_handling():
    """Example 4: Date/time normalization."""
    print("\n" + "=" * 60)
    print("Example 4: Date/Time normalization to ISO-8601")
    print("=" * 60)

    excel_file = create_sample_excel()

    try:
        with open(excel_file, 'rb') as f:
            options = {
                'sheet_name': 'Employees',
                'date_overrides': ['hire_date']
            }

            row_iterator = get_excel_row_iterator(f, options=options)

            if row_iterator:
                for sheet_name, row_dict in row_iterator:
                    print(f"\nEmployee: {row_dict['name']}")
                    print(f"Hire Date (ISO): {row_dict['hire_date']}")
    finally:
        os.unlink(excel_file)


def example_5_hyperlinks_and_comments():
    """Example 5: Accessing hyperlinks and comments."""
    print("\n" + "=" * 60)
    print("Example 5: Hyperlinks and Comments")
    print("=" * 60)

    excel_file = create_sample_excel()

    try:
        with open(excel_file, 'rb') as f:
            options = {'sheet_name': 'Employees'}
            row_iterator = get_excel_row_iterator(f, options=options)

            if row_iterator:
                for sheet_name, row_dict in row_iterator:
                    print(f"\n--- Employee: {row_dict.get('name', 'N/A')} ---")

                    # Check for hyperlinks in email
                    email = row_dict.get('email')
                    if isinstance(email, list) and email:
                        print(f"Email (with hyperlink):")
                        print(f"  Text: {email[0].get('text')}")
                        print(f"  URL: {email[0].get('url')}")
                    else:
                        print(f"Email: {email}")

                    # Check for comments in name
                    name = row_dict.get('name')
                    if isinstance(name, list) and name:
                        print(f"Name has comment:")
                        if 'comment' in name[0]:
                            comment = name[0]['comment']
                            print(f"  Comment text: {comment.get('text')}")
                            print(f"  Comment author: {comment.get('excel_author', 'N/A')}")
    finally:
        os.unlink(excel_file)


def example_6_catalog_filtering():
    """Example 6: Filtering columns by catalog."""
    print("\n" + "=" * 60)
    print("Example 6: Filtering columns with catalog")
    print("=" * 60)

    excel_file = create_sample_excel()

    try:
        with open(excel_file, 'rb') as f:
            # Only include these columns; others go to _sdc_extra
            headers_in_catalog = ['id', 'name', 'email']

            options = {'sheet_name': 'Employees'}
            row_iterator = get_excel_row_iterator(
                f,
                options=options,
                headers_in_catalog=headers_in_catalog
            )

            if row_iterator:
                for sheet_name, row_dict in row_iterator:
                    print(f"\nCatalog columns:")
                    print(f"  ID: {row_dict.get('id')}")
                    print(f"  Name: {row_dict.get('name')}")
                    print(f"  Email: {row_dict.get('email')}")

                    if '_sdc_extra' in row_dict:
                        print(f"Extra columns (not in catalog): {row_dict['_sdc_extra']}")
    finally:
        os.unlink(excel_file)


def example_7_counting_rows():
    """Example 7: Counting total rows across all sheets."""
    print("\n" + "=" * 60)
    print("Example 7: Counting rows across all sheets")
    print("=" * 60)

    excel_file = create_sample_excel()

    try:
        with open(excel_file, 'rb') as f:
            row_iterator = get_excel_row_iterator(f)

            if row_iterator:
                sheet_counts = {}
                total_count = 0

                for sheet_name, row_dict in row_iterator:
                    sheet_counts[sheet_name] = sheet_counts.get(sheet_name, 0) + 1
                    total_count += 1

                print(f"\nRows per sheet:")
                for sheet, count in sheet_counts.items():
                    print(f"  {sheet}: {count} rows")
                print(f"\nTotal rows: {total_count}")
    finally:
        os.unlink(excel_file)


if __name__ == '__main__':
    print("Singer Encodings - Excel Reader Examples\n")

    # Run all examples
    example_1_basic_reading()
    example_2_specific_sheet()
    example_3_key_properties()
    example_4_date_handling()
    example_5_hyperlinks_and_comments()
    example_6_catalog_filtering()
    example_7_counting_rows()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
