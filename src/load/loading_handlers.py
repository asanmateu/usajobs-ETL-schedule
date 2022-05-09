from src.config.DB_connection import db_connect
from constants import DB_NAME
import pandas as pd
import sqlite3
import sys


def load_data_into_db(df: pd.DataFrame, db_name: str = DB_NAME) -> None:
    """Loads dataframe into database using sqlalchemy. """
    try:
        db_connection = db_connect(db_name)
        with db_connection:
            db_cursor = db_connection.cursor()
            for index, row in df.iterrows():
                db_cursor.execute(""" INSERT OR REPLACE INTO POSITION (
                                                    TITLE_ID, 
                                                    TITLE, 
                                                    ORGANISATION_NAME, 
                                                    REMUNERATION_MIN, 
                                                    REMUNERATION_MAX, 
                                                    REMUNERATION_RATE, 
                                                    WHO_MAY_APPLY, 
                                                    APPLICATION_CLOSE_DATE
                                                    )    
                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                                  (row["PositionID"],
                                   row["PositionTitle"],
                                   row["OrganizationName"],
                                   row["RemunerationMin"],
                                   row["RemunerationMax"],
                                   row["RemunerationRate"],
                                   row["WhoMayApply"],
                                   row["ApplicationCloseDate"]))
            db_connection.commit()
    except sqlite3.Error as error:
        print("Failed to execute the above query", error)
        sys.exit(1)
    finally:
        if db_connection:
            db_connection.close()