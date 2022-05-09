from constants import BASE_URL, PAGE_LIMIT, USER_AGENT, API_KEY
from typing import List
import requests
import sys


def get_api_call(endpoint: str, params: dict, base_url: str = BASE_URL, page_limit: int = PAGE_LIMIT,
                 user_agent: str = USER_AGENT, api_key: str = API_KEY):
    """
    Makes a GET request with appropriate parameters, authentication,
    while respecting page and rate limits, and paginating if needed.

    Returns a JSON API response object. """

    # Set up authentication
    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": user_agent,
        "Authorization-Key": api_key
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
