from constants import EXPORTS_DIR, QUERY_1, QUERY_2, QUERY_3
from src.utils.myutils import export_query_as_csv
from datetime import date
import sqlite3
import sys
import os


def run_analysis(exports_path: str = EXPORTS_DIR, query_1: str = QUERY_1, query_2: str = QUERY_2, query_3: str = QUERY_3):
    """
    Runs 3 SQL queries to obtain results that could answer the following questions:
    1. How do *monthly* starting salaries differ across positions with different titles and keywords?
    2. Do (filtered) positions for which 'United States Citizens' can apply have a higher average salary than those
       that 'Student/Internship Program Eligibles' can apply for? (by month)
    3. What are the organisations that have most open (filtered) positions?

    exports results of queries into CSV files in the `output_path` directory.

    ** Feel free to break this function down into smaller units
    (hint: potentially have a `export_csv(query_result)` function)
    """

    global ANALYSIS_DIR
    ANALYSIS_DIR = os.path.join(exports_path, str(date.today()))

    try:
        # Create analysis folder if necessary
        if not os.path.exists(os.path.join(ANALYSIS_DIR)):
            os.makedirs(ANALYSIS_DIR)
            # Export results of queries into CSV files in the `output_path` directory.
        for num, query in enumerate([query_1, query_2, query_3], start=1):
            export_query_as_csv(query, query_name=f"query{num}_{date.today()}.csv", path=ANALYSIS_DIR)

    except sqlite3.Error as error:
        print("Failed to execute the above query", error)
        sys.exit(1)
