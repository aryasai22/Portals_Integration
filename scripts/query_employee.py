"""
Quick employee lookup from cached JSON data.
Usage: python scripts/query_employee.py ES-1114
"""
import json
import sys
from pathlib import Path
from glob import glob


def find_latest_employee_file():
    """Find the largest (most complete) employee JSON file."""
    pattern = "output/data/ceipal_employees_*.json"
    files = glob(pattern)
    if not files:
        print(f"No employee data files found matching: {pattern}")
        print("Run this first: python pipelines/bronze_ingestion.py --resources employees --json-output")
        sys.exit(1)

    # Get largest file (most complete dataset)
    largest = max(files, key=lambda f: Path(f).stat().st_size)
    return largest


def load_employees(json_file):
    """Load employee data from JSON file."""
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['records']


def find_employee(employees, employee_number):
    """Find employee by employee number."""
    for emp in employees:
        if emp.get('employee_number') == employee_number:
            return emp
    return None


def display_employee(emp):
    """Display employee information in a readable format."""
    if not emp:
        return

    print("\n" + "="*80)
    print(f"EMPLOYEE: {emp.get('first_name', '')} {emp.get('last_name', '')}")
    print("="*80)

    # Basic Info
    print("\nBASIC INFORMATION:")
    print(f"  Employee Number:    {emp.get('employee_number', 'N/A')}")
    print(f"  Short Name:         {emp.get('emp_short_name', 'N/A')}")
    print(f"  Email:              {emp.get('email_id', 'N/A')}")
    print(f"  Mobile:             {emp.get('mobile_phone', 'N/A')}")
    print(f"  Gender:             {emp.get('gender', 'N/A')}")
    print(f"  Marital Status:     {emp.get('marital_status', 'N/A')}")

    # Employment Info
    print("\nEMPLOYMENT DETAILS:")
    print(f"  Employee Type:      {emp.get('emp_type', 'N/A')} ({emp.get('emptype_category', 'N/A')})")
    print(f"  Status:             {emp.get('employee_status', 'N/A')}")
    print(f"  Date of Joining:    {emp.get('date_of_joining', 'N/A')}")
    if emp.get('release_date'):
        print(f"  Release Date:       {emp.get('release_date', 'N/A')}")
        print(f"  Resignation Status: {emp.get('resignation_status', 'N/A')}")
    print(f"  Classification:     {emp.get('classification', 'N/A')}")
    print(f"  Department:         {emp.get('department', 'N/A')}")
    print(f"  Role:               {emp.get('role', 'N/A')}")

    # Location & Work Auth
    print("\nLOCATION & AUTHORIZATION:")
    print(f"  Location:           {emp.get('location', 'N/A')}")
    print(f"  State:              {emp.get('permanent_state', 'N/A')}")
    print(f"  Country:            {emp.get('permanent_country', 'N/A')}")
    print(f"  Work Authorization: {emp.get('workauthorization', 'N/A')}")
    print(f"  Veteran Status:     {emp.get('veteran_status', 'N/A')}")

    # Management
    print("\nMANAGEMENT:")
    print(f"  Reporting Manager:  {emp.get('reporting_manager', 'N/A')}")
    print(f"  HR Manager:         {emp.get('hr_manager', 'N/A')}")
    print(f"  Finance Manager:    {emp.get('finance_manager', 'N/A')}")

    # System Info
    print("\nSYSTEM INFORMATION:")
    print(f"  Login Status:       {'Active' if emp.get('login_status') == 1 else 'Inactive'}")
    print(f"  Created:            {emp.get('created', 'N/A')}")
    print(f"  Last Modified:      {emp.get('modified', 'N/A')}")
    print(f"  Data Extracted:     {emp.get('_extracted_at', 'N/A')}")

    print("="*80 + "\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/query_employee.py <EMPLOYEE_NUMBER>")
        print("Example: python scripts/query_employee.py ES-1114")
        sys.exit(1)

    employee_number = sys.argv[1]

    # Find and load employee data
    print(f"Loading employee data...")
    json_file = find_latest_employee_file()
    print(f"Using file: {json_file}")

    employees = load_employees(json_file)
    print(f"Loaded {len(employees)} employees")

    # Search for employee
    print(f"\nSearching for employee: {employee_number}")
    emp = find_employee(employees, employee_number)

    if emp:
        display_employee(emp)
    else:
        print(f"\nEmployee '{employee_number}' not found in dataset.")
        print(f"Total employees in dataset: {len(employees)}")

        # Show some examples
        print("\nExample employee numbers:")
        for i, e in enumerate(employees[:5]):
            print(f"  - {e.get('employee_number')}")
        print("  ...")


if __name__ == "__main__":
    main()
