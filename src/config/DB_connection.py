from constants import DATABASE_DIR
import sqlite3
import sys


def db_connect(db_path: str = DATABASE_DIR):
    """Connects to database and returns a database connection object. """
    try:
        db_connection = sqlite3.connect(db_path)
        return db_connection
    except sqlite3.Error as error:
        print("Failed to connect to the database.", error)
        sys.exit(1)
