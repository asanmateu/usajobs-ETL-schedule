from src.config.DB_connection import db_connect
from constants import DB_NAME
import sqlite3
import os
import csv
import sys


def export_query_as_csv(query: str, query_name: str, path: str, db_name: str = DB_NAME) -> None:
    """exports query results to CSV file.  """

    try:
        # Get the database connection
        db_connection = db_connect(db_name)
        with db_connection:
            db_cursor = db_connection.cursor()
            db_cursor.execute(query)
            rows = db_cursor.fetchall()
        try:
            # Write to CSV file
            with open(os.path.join(path, query_name), "w") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([i[0] for i in db_cursor.description])
                writer.writerows(rows)
        except IOError as err:
            print(err)
            sys.exit(1)
    except sqlite3.Error as err:
        print(err)
        sys.exit(1)
    finally:
        if db_connection:
            db_cursor.close()
            db_connection.close()
