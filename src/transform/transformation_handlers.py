from typing import List
import pandas as pd
import sys


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


def transform_response_to_df(titles_response: List[str], keywords_response: List[str]):
    """
    Transforms a parse JSON to a pandas dataframe.

    Returns a pandas dataframe. """

    title_search = parse_response(titles_response)
    keyword_search = parse_response(keywords_response)

    # Merge search results on PositionID into a DataFrame
    merged_search = title_search + keyword_search
    search_df = pd.DataFrame(merged_search)
    search_df = search_df.drop_duplicates(subset="PositionID", keep="first")

    return search_df
