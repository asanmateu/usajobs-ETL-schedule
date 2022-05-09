from src.analysis.report_sender import *
from src.analysis.analysis_generator import *
from src.extract.extraction_handlers import *
from src.config.DB_prep import *
from src.load.loading_handlers import *
from constants import *
from typing import List
import argparse


def run_pipeline(search_titles: List[str] = TITLES, search_keywords: List[str] = KEYWORDS):
    """
    Runs the pipeline.

    Returns None
    """
    # Step 1: Extract and transform job data from API
    try:
        print("Extracting and parsing job data from API...")
        response_df = extract_positions(search_titles, search_keywords)
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

    print("Pipeline complete.")


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

    # Set search parameters
    TITLES = [str(title) for title in args.t.split(",")]
    KEYWORDS = [str(keyword) for keyword in args.k.split(",")]
    SORT_FIELD = args.sortfield
    SORT_ORDER = args.sortorder

    # Set email credentials
    os.environ["SENDER_EMAIL"] = args.se
    os.environ["SENDER_PASSWORD"] = args.sp
    os.environ["RECIPIENT_EMAIL"] = args.re

    # Run pipeline
    run_pipeline()

    # Exit cleanly
    sys.exit(0)
