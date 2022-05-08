import csv
import os
import sys
import ssl
import sqlite3
import smtplib
import requests
import argparse
from datetime import date
from typing import List
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd

load_dotenv()

BASE_URL = "https://data.usajobs.gov/api/"

BASE_DIR = os.path.realpath('')
DB_NAME = str(os.environ.get("DB_NAME"))
DATABASE_DIR = os.path.join(BASE_DIR, DB_NAME)
EXPORTS_DIR = os.path.join(BASE_DIR, r"exports")

# Set up main search parameters
TITLES = ['Data Analyst', 'Data Scientist', 'Data Engineering']
KEYWORDS = ['data', 'analysis', 'analytics']
SORT_FIELD = "DatePosted"
SORT_ORDER = "Descending"
PAGE_LIMIT = 500

# Change directory to the directory of this script
os.chdir(BASE_DIR)


def db_connect(db_path: str = DATABASE_DIR):
    """Connects to database and returns a database connection object. """
    try:
        db_connection = sqlite3.connect(db_path)
        return db_connection
    except sqlite3.Error as error:
        print("Failed to connect to the database.", error)
        sys.exit(1)


def get_api_call(endpoint: str, params: dict, base_url: str = BASE_URL, page_limit: int = PAGE_LIMIT):
    """
    Makes a GET request with appropriate parameters, authentication,
    while respecting page and rate limits, and paginating if needed. 

    Returns a JSON API response object. """

    # Set up authentication
    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": os.environ.get('USER_AGENT'),
        "Authorization-Key": os.environ.get('API_KEY')
    }

    # Set up default parameters
    params["ResultsPerPage"] = page_limit
    params["SortField"] = "DatePosted"
    params["SortOrder"] = "Descending"

    # Make the API call
    try:
        url = base_url + endpoint
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(err)
        sys.exit(1)


def parse_response(response_json):
    """
    Parses a response JSON for wanted fields.

    Returns a list of positions of appropriate object type. """

    # TODO: Could improve this by extracting all variations of same position / per location different salaries

    positions = []

    try:
        for job in response_json["SearchResult"]["SearchResultItems"]:
            position = {
                "PositionID": job["MatchedObjectDescriptor"]["PositionID"],
                "PositionTitle": job["MatchedObjectDescriptor"]["PositionTitle"].strip().title(),
                "OrganizationName": job["MatchedObjectDescriptor"]["OrganizationName"],
                "RemunerationMin": float(job["MatchedObjectDescriptor"]["PositionRemuneration"][0]["MinimumRange"]),
                "RemunerationMax": float(job["MatchedObjectDescriptor"]["PositionRemuneration"][0]["MaximumRange"]),
                "RemunerationRate": job["MatchedObjectDescriptor"]["PositionRemuneration"][0]["RateIntervalCode"],
                "WhoMayApply": job["MatchedObjectDescriptor"]["UserArea"]["Details"]["WhoMayApply"]["Name"],
                "ApplicationCloseDate": job["MatchedObjectDescriptor"]["ApplicationCloseDate"],
            }
            positions.append(position)

            return positions

    except() as err:
        print(err)
        sys.exit(1)

    except() as err:
        print(err)
        sys.exit(1)


def extract_positions(titles: List[str], keywords: List[str]):
    """
    Makes API calls for titles and keywords, parses the responses.

    Returns the values ready to be loaded into database. """

    # Set up API query parameters
    params_titles = {
        "PositionTitle": titles
    }

    params_keywords = {
        "Keyword": keywords
    }

    # Retrieve API responses
    try:
        api_response_titles = get_api_call("Search", params_titles)
        api_response_keywords = get_api_call("Search", params_keywords)

        # Parse the API responses
        title_search = parse_response(api_response_titles)
        keyword_search = parse_response(api_response_keywords)

        # Merge search results on PositionID into a DataFrame
        merged_search = title_search + keyword_search
        search_df = pd.DataFrame(merged_search)
        search_df = search_df.drop_duplicates(subset="PositionID", keep="first")

        return search_df

    except requests.exceptions.HTTPError as err:
        print(err)
        sys.exit(1)


def prep_database(db_name: str = DB_NAME) -> None:
    """Connects to database and creates tables if necessary. """
    # TODO: Could improve database design adding levels of granularity and foreign keys
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


def run_analysis(exports_path: str = EXPORTS_DIR):
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

    query_1 = """ 
        SELECT DISTINCT TITLE_ID, TITLE, REMUNERATION_MIN   
        FROM POSITION
        WHERE REMUNERATION_RATE = 'Monthly'
        AND LOWER(TITLE) LIKE '%data%'
        GROUP BY 1
        ORDER BY 2 DESC 
        """
    query_2 = """
        SELECT DISTINCT WHO_MAY_APPLY, AVG(REMUNERATION_MIN), AVG(REMUNERATION_MAX) 
        FROM POSITION
        WHERE WHO_MAY_APPLY LIKE '%United States Citizens%' 
        OR WHO_MAY_APPLY LIKE '%Student/Internship Program Eligibles%'
        AND REMUNERATION_RATE = 'Monthly'
        GROUP BY WHO_MAY_APPLY
        ORDER BY AVG(REMUNERATION_MIN) DESC
        """
    query_3 = """
        SELECT ORGANISATION_NAME, COUNT(TITLE_ID)
        FROM POSITION
        WHERE APPLICATION_CLOSE_DATE > DATE('NOW')
        GROUP BY 1
        ORDER BY 2 DESC
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


def send_reports(reports_path: str):
    """
    Loops through present CSV files in reports_path,
    and sends them via email to recipient.

    Returns None
    """
    # Set up email parameters
    msg = MIMEMultipart()
    msg["From"] = os.environ.get("SENDER_EMAIL")
    msg["To"] = os.environ.get("RECIPIENT_EMAIL")
    msg["Subject"] = "Antonio - Data Analysis Reports {}".format(date.today())
    msg.attach(MIMEText("Please find attached reports for today's analysis."))

    context = ssl.create_default_context()

    # Loop through CSV files in reports_path, attach to email
    try:
        for file in os.listdir(reports_path):
            with open(os.path.join(reports_path, file), "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    "attachment; filename={}".format(file),
                )
                msg.attach(part)
    except FileNotFoundError:
        print("No reports found in {}".format(reports_path))
        sys.exit(1)

    try:
        # Send email using SMTP
        smtp_server = smtplib.SMTP(os.environ.get('SMTP_SERVER'), int(os.environ.get('PORT')))
        smtp_server.ehlo()
        # Start TLS for security
        smtp_server.starttls(context=context)
        # Identify ourselves to smtp gmail client
        smtp_server.ehlo()
        # Identify to server this time with encrypted connection
        smtp_server.login(os.environ.get('SENDER_EMAIL'), os.environ.get('SENDER_PASSWORD'))
        # Send email
        smtp_server.sendmail(os.environ.get('EMAIL'), os.environ.get('EMAIL_TO'), msg.as_string())
        # Quit server
        smtp_server.quit()
    except Exception as e:
        print(e)
        sys.exit(1)


def run_pipeline():
    """
    Runs the pipeline.

    Returns None
    """
    # Step 1: Extract and transform job data from API
    try:
        print("Extracting and parsing job data from API...")
        response_df = extract_positions(TITLES, KEYWORDS)
    except Exception as e:
        print("Failed to extract and parse job data from API", e)
        sys.exit(1)
    print("Extraction complete.")

    # Step 2: Create SQLite database and load data into it
    try:
        print("Connecting to SQLite database...")
        prep_database()
        print("Database connection successful.")
        print("Loading data into database...")
        load_data_into_db(response_df)
        print("Data load complete.")
    except Exception as e:
        print("Failed to connect to database", e)
        sys.exit(1)

    # Step 3: Run analysis queries
    try:
        print("Running analysis queries...")
        run_analysis()
        print("Analysis complete.")
    except Exception as e:
        print("Failed to run analysis queries", e)
        sys.exit(1)

    # Step 4: Send report email
    print("Sending reports email...")
    try:
        send_reports(ANALYSIS_DIR)  # ANALYSIS_DIR is defined when function is executed
        print("Reports email sent.")
    except Exception as e:
        print("Failed to send reports email - make sure you set SENDER_EMAIL, SENDER_PASSWORD,"
              " and RECIPIENT_EMAIL environment variables", e)
        sys.exit(1)


if __name__ == "__main__":
    """
    Puts it all together, and runs everything end-to-end. 

    Feel free to create additional functions that represent distinct functional units, 
    rather than putting it all in here. 

    Optionally, enable running this script as a CLI tool with arguments for position titles and keywords. 
    """
    # Set up arguments for CLI tool to set search titles and keywords
    parser = argparse.ArgumentParser(
        description="Runs the data analysis pipeline setting main search parameters.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-t",
        type=str,
        help="A comma-separated list of titles to search for.",
        default=TITLES,
    )
    parser.add_argument(
        "-k",
        type=str,
        help="A comma-separated list of keywords to search for.",
        default=KEYWORDS,
    )
    parser.add_argument(
        "-sf",
        type=str,
        help="The field to sort the results by.",
        default=SORT_FIELD,
    )
    parser.add_argument(
        "-so",
        type=str,
        help="The order to sort the results by.",
        default=SORT_ORDER,
    )
    parser.add_argument(
        "-se",
        type=str,
        help="The email address of the sender.",
        default=os.environ.get("SENDER_EMAIL"),
    )
    parser.add_argument(
        "-sp",
        type=str,
        help="The password of the sender.",
        default=os.environ.get("SENDER_PASSWORD"),
    )
    parser.add_argument(
        "-re",
        type=str,
        help="The email address of the recipient.",
        default=os.environ.get("RECIPIENT_EMAIL"),
    )

    args = parser.parse_args()

    # Set search titles and keywords
    TITLES = args.titles
    KEYWORDS = args.keywords
    SORT_FIELD = args.sortfield
    SORT_ORDER = args.sortorder

    # Set email credentials
    os.environ["SENDER_EMAIL"] = args.sender_email
    os.environ["SENDER_PASSWORD"] = args.sender_password
    os.environ["RECIPIENT_EMAIL"] = args.recipient_email

    # Run pipeline
    run_pipeline()

    # Exit cleanly
    sys.exit(0)

    ###############################################################################################

# TODO - MAIN:
#       - Setup a cron job to run this script daily scheduled on GCP
#       - Use professional project structure and architecture
#       - Solve doubts: why response is so small?


# TODO - Additional:
#  - Add more specific error handling to catch errors accurately
#  - Improve models if we extract more data points
#  - Could add unit and integration tests with unittest and a CI/CD pipeline
#  - Could add logs for debugging with logger module
#  - Could add more robust email sending security
