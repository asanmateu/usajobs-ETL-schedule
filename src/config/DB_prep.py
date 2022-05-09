from constants import DB_NAME, BASE_DIR
from DB_connection import db_connect
import sqlite3
import sys


def prep_database(db_name: str = DB_NAME) -> None:
    """Connects to database and creates tables if necessary. """
    try:
        db_connection = db_connect(db_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("""CREATE TABLE IF NOT EXISTS POSITION (
                                TITLE_ID            VARCHAR(255)        PRIMARY KEY,
                                TITLE               VARCHAR(255)        NOT NULL,
                                ORGANISATION_NAME   VARCHAR(255)        NOT NULL,
                                REMUNERATION_MIN    REAL,
                                REMUNERATION_MAX    REAL,
                                REMUNERATION_RATE   VARCHAR(255),
                                WHO_MAY_APPLY       VARCHAR(255),
                                APPLICATION_CLOSE_DATE TIMESTAMP     NOT NULL);""")
        db_connection.commit()
        print(f"Database created: {db_name} at {BASE_DIR}")
        print("Table POSITION has been created")
    except sqlite3.Error as error:
        print("Failed to execute the above query", error)
        sys.exit(1)
    finally:
        if db_connection:
            db_connection.close()
