from dotenv import load_dotenv
import os

load_dotenv()

# API Settings
BASE_URL = "https://data.usajobs.gov/api/"
USER_AGENT = os.environ.get('USER_AGENT')
API_KEY = os.environ.get('API_KEY')

# Project paths
BASE_DIR = os.path.realpath('')
DB_NAME = str(os.environ.get("DB_NAME"))
DATABASE_DIR = os.path.join(BASE_DIR, DB_NAME)
EXPORTS_DIR = os.path.join(BASE_DIR, r"exports")
ANALYSIS_DIR = r""  # A global assignment will populate this with a path dedicated to the job's day

# Set up main search parameters
TITLES = ['Data Analyst', 'Data Scientist', 'Data Engineering']
KEYWORDS = ['data', 'analysis', 'analytics']
SORT_FIELD = "DatePosted"
SORT_ORDER = "Descending"
PAGE_LIMIT = 500

# Email settings
SMTP_SERVER = os.environ.get('SMTP_SERVER')
SMTP_PORT = os.environ.get('SMTP_PORT')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
SENDER_PASSWORD = os.environ.get('SENDER_PASSWORD')
RECIPIENT_EMAIL = os.environ.get('RECIPIENT_EMAIL')


# Analysis queries
QUERY_1 = """ 
        SELECT DISTINCT TITLE_ID, TITLE, REMUNERATION_MIN   
        FROM POSITION
        WHERE REMUNERATION_RATE = 'Monthly'
        AND LOWER(TITLE) LIKE '%data%'
        GROUP BY 1
        ORDER BY 2 DESC 
        """
QUERY_2 = """
        SELECT DISTINCT WHO_MAY_APPLY, AVG(REMUNERATION_MIN), AVG(REMUNERATION_MAX) 
        FROM POSITION
        WHERE WHO_MAY_APPLY LIKE '%United States Citizens%' 
        OR WHO_MAY_APPLY LIKE '%Student/Internship Program Eligibles%'
        AND REMUNERATION_RATE = 'Monthly'
        GROUP BY WHO_MAY_APPLY
        ORDER BY AVG(REMUNERATION_MIN) DESC
        """
QUERY_3 = """
        SELECT ORGANISATION_NAME, COUNT(TITLE_ID)
        FROM POSITION
        WHERE APPLICATION_CLOSE_DATE > DATE('NOW')
        GROUP BY 1
        ORDER BY 2 DESC
        """
