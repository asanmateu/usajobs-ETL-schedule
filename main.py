import csv
import sqlite3
import smtplib
import requests
import sqlalchemy
from os import environ
from datetime import date
from datetime
from typing import List

BASE_URL = "https://data.usajobs.gov/api/"
PAGE_LIMIT = 500


def db_connect(db_name: str):
    """Connects to database and returns a database connection object. """
    try:
        db_connection = sqlite3.connect(db_name)
        return db_connection
    except sqlite3.Error as err:
        print(err)


def get_api_call(endpoint: str, params: dict, base_url: str = BASE_URL, page_limit: int = PAGE_LIMIT):
    """
    Makes a GET request with appropriate parameters, authentication,
    while respecting page and rate limits, and paginating if needed. 
    
    Returns a JSON API response object. """

    # Set up authentication
    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": environ["USER_AGENT"],
        "Authorization-Key": environ["API_KEY"]
    }

    # Set up default parameters
    params["ResultsPerPage"] = page_limit
    params["SortField"] = "DatePosted"

    # Make the API call
    url = base_url + endpoint
    response = requests.get(url, headers=headers, params=params)

    return response.json()


def parse_positions(response_json):
    """
    Parses a response JSON for wanted fields.

    Returns a list of positions of appropriate object type. """

    positions = []
    for result in response_json["SearchResult"]["SearchResultItems"]:
        position = {
            "title": result["MatchedObjectDescriptor"]["PositionTitle"],
            "organization": result["MatchedObjectDescriptor"]["OrganizationName"],
            "location": result["MatchedObjectDescriptor"]["PositionLocation"],
            "description": result["MatchedObjectDescriptor"]["JobSummary"],
            "date_posted": result["MatchedObjectDescriptor"]["DatePosted"],
            "date_closing": result["MatchedObjectDescriptor"]["DateClosing"],
            "keywords": result["MatchedObjectDescriptor"]["KeywordText"],
            "remuneration_min": result["MatchedObjectDescriptor"]["PositionRemuneration"]["MinimumRange"],
            "remuneration_max": result["MatchedObjectDescriptor"]["PositionRemuneration"]["MaximumRange"],
            "remuneration_rate": result["MatchedObjectDescriptor"]["PositionRemuneration"]["RateIntervalCode"],
            "who_may_apply": result["MatchedObjectDescriptor"]["UserArea"]["Details"]["WhoMayApply"]["Name"],
            "close_date": result["MatchedObjectDescriptor"]["ApplicationCloseDate"],
            "url": result["MatchedObjectDescriptor"]["PositionURI"],
        }
        positions.append(position)

    return positions


def extract_positions(titles: List[str], keywords: List[str]):
    """
    Makes API calls for titles and keywords, parses the responses. 
    
    Returns the values ready to be loaded into database. """

    # Set up parameters
    params_titles = {
        "Title": titles
    }

    params_keywords = {
        "Keyword": keywords,
    }

    # Get the API call
    api_response_titles = get_api_call("Search", params_titles)
    api_response_keywords = get_api_call("Search", params_keywords)

    # Parse the API responses
    positions_title_search = parse_positions(api_response_titles)
    positions_keyword_search = parse_positions(api_response_keywords)

    return positions_title_search, positions_keyword_search


def prep_database(db_name: str) -> None:
    """Connects to database and creates tables if necessary. """
    db_connection = db_connect(db_name)
    with db_connection:
        db_cursor = db_connection.cursor()
        db_cursor.execute("""CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY,
            title TEXT,
            organization TEXT,
            location TEXT,
            description TEXT,
            date_posted TEXT,
            date_closing TEXT,
            keywords TEXT,
            remuneration_min TEXT,
            remuneration_max TEXT,
            remuneration_rate TEXT,
            who_may_apply TEXT,
            close_date TEXT,
            url TEXT
        )""")


def load_data(row_values: List[dict], table_name: str) -> None:
    """Connects to database and loads values in corresponding tables. """
    db_connection = db_connect("db.sqlite")
    with db_connection:
        db_cursor = db_connection.cursor()
        db_cursor.executemany("""INSERT INTO {} VALUES (?,?,?,?,?,?,?,?)""".format(table_name), row_values)


def run_analysis(output_path: str):
    """
    Runs 3 SQL queries to obtain results that could answer the following questions:
    1. How do *monthly* starting salaries differ across positions with different titles and keywords?
    2. Do (filtered) positions for which 'United States Citizens' can apply have a higher average salary than those
       that 'Student/Internship Program Eligibles' can apply for? (by month)
    3. What are the organisations that have most open (filtered) positions?
    
    Exports results of queries into CSV files in the `output_path` directory.

    ** Feel free to break this function down into smaller units 
    (hint: potentially have a `export_csv(query_result)` function)  
    """
    # 1. How do *monthly* starting salaries differ across positions with different titles and keywords?
    query_1 = """
        SELECT DISTINCT title, keywords, remuneration_min
        FROM positions
        WHERE remuneration_rate = 'Monthly'
        """

    # 2. Do (filtered) positions for which 'United States Citizens' can apply have a higher average salary than those
    #    that 'Student/Internship Program Eligibles' can apply for? (by month)
    query_2 = """ """

    # 3. What are the organisations that have most open (filtered) positions?
    query_3 = """ """

    db_connection = db_connect("db.sqlite")
    with db_connection as db:
        db_cursor = db.cursor()
        db_cursor.execute(query_1)
        query_1_result = db_cursor.fetchall()
        db_cursor.execute(query_2)
        query_2_result = db_cursor.fetchall()
        db_cursor.execute(query_3)
        query_3_result = db_cursor.fetchall()


def send_reports(recipient_email: str, reports_path: str):
    """
    Loops through present CSV files in reports_path, 
    and sends them via email to recipient. 

    Returns None
    """

    # Set up email parameters
    email_params = {
        "recipient_email": recipient_email,
        "reports_path": reports_path
    }

    # Send email


if __name__ == "__main__":
    """
    Puts it all together, and runs everything end-to-end. 

    Feel free to create additional functions that represent distinct functional units, 
    rather than putting it all in here. 

    Optionally, enable running this script as a CLI tool with arguments for position titles and keywords. 
    """
    # import argparse

    curr_timestamp = int(datetime.timestamp(datetime.now()))

