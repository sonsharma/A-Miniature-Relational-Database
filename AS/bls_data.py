"""
This script is used for getting up to date job outlook projections from BLS and matching to BG roles
- The data from BLS is downloaded manually from https://www.bls.gov/emp/tables/occupational-projections-and-characteristics.htm
- To get BG mapping data connect to API (only maps to BgtOcc)
- To get ONET mapping data (to map from BG 2010 values to BLS 2019), download from https://www.onetcenter.org/taxonomy/2019/walk.html
- Map all BgtOcc growth rates to all underlying suboccs
"""

import json
import gcsfs
import logging
import requests
import pandas as pd

from sqlalchemy import create_engine

from df_scripts.bg_access import get_token
from df_scripts import pipeline_config as PC

project_id = "citi-ventures"


def clean_data(element):
    """
    Import downloaded data from BLS and from ONET
    Clean and map to use 2010 ONET occupations

    Returns:
        pandas dataframe: file df with 2010 ONET occupations and 2019-2029 employement growth
    """
    fs, conn, engine = connection()

    # Importing data from GCP bucket
    with fs.open("gs://worthi_class_central_data/BLS_data/occupation.xlsx") as f:
        df_bls = pd.read_excel(f, sheet_name="Table 1.2", skiprows=1)

    # Importing data from GCP bucket
    with fs.open(
        "gs://worthi_class_central_data/BLS_data/2010_to_2019_Crosswalk.csv"
    ) as f:
        df_onet_map = pd.read_csv(f, encoding="ISO-8859-1")

    # BG uses the 2010 ONET occupations, we need to map the BLS 2019 ones to 2010
    df_bls = df_bls.rename(
        columns={
            "2019 National Employment Matrix title": "name_2019",
            "Employment change, percent, 2019-29": "employmentGrowth",
        }
    )

    # Keeping only important rows (Line item)
    df_bls = df_bls[df_bls["Occupation type"] == "Line item"]

    # Clean names
    df_bls["name_2019"] = [i.strip().lower() for i in df_bls["name_2019"]]

    # Keeping only the 2010 names
    # Cleaning both names to match BLS data
    df_onet_map["name"] = [
        i.strip().lower() for i in df_onet_map["O*NET-SOC 2010 Title"]
    ]
    df_onet_map["name_2019"] = [
        i.strip().lower() for i in df_onet_map["O*NET-SOC 2019 Title"]
    ]

    df = df_bls.merge(df_onet_map, on="name_2019", how="left")

    # Keeping only important columns
    df = df[["name", "employmentGrowth"]]
    logging.info(
        f"-- Fetched 2010 ONET occupations and 2019-2029 employement growth data. Dataframe shape: {str(df.shape)}",
    )
    return element, df


def BG_ONET_BGTocc_mapping(element):
    """
    Use BG API to get entire BgtOcc to ONET (2010) mapping
    Clean and covert to pandas DF

    Returns:
        pandas dataframe: file df with BG mapping of ONET and BgtOcc names
    """
    occu = element[0]
    df_bls = element[1]
    url = "https://apis.burning-glass.com/v3.5/ontology/query"
    payload = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\nPREFIX bgt: <http://www.burning-glass.com/Ontology#>\r\nPREFIX onet: <http://www.burning-glass.com/onet/Ontology#>\r\n \r\nSELECT ?OnetName ?BgtOccName \r\nWHERE {\r\n  ?Onet a onet:ONET.\r\n  ?Onet rdfs:label ?OnetName.\r\n  ?Onet bgt:bestFitBgtOcc ?BgtOcc.\r\n  ?BgtOcc rdfs:label ?BgtOccName.\r\n  FILTER langMatches(lang(?BgtOccName), \"en-us\")\r\n  FILTER langMatches(lang(?OnetName), \"en-us\")\r\n}"""
    access_token, _, _ = get_token()
    headers = {
        "Content-Type": "application/sparql-query",
        "Accept": "application/sparql-results+json",
        "Authorization": f"Bearer {access_token}",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response = json.loads(response.text)

    onet_names = []
    bgtocc_names = []
    for i in response["results"]["bindings"]:
        onet_names.append(i["OnetName"]["value"].lower())
        bgtocc_names.append(i["BgtOccName"]["value"].lower())

    df = pd.DataFrame()
    df["onet"] = onet_names  # 2010 ONET names
    df["bgtocc"] = bgtocc_names

    logging.info(
        f"-- Fetched BG mapping of ONET and BgtOcc names data. Dataframe shape: {str(df.shape)}",
    )
    return occu, df_bls, df


def merge_data(element):
    """
    Merge BLS data with BG to ONET mapping and then to subocc in DB

    Args:
        df_bls (pandas dataframe): 2019-2029 data from bls with 2010 ONET names
        df_map (pandas dataframe): 2010 ONET names to BgtOcc

    Returns:
        pandas dataframe: final dataframe with subocc and employement growth
    """
    fs, conn, engine = connection()

    occu = element[0]
    df_bls = element[1]
    df_map = element[2]

    # Merging the BLS data to ONET mapping
    df = df_bls.merge(df_map, left_on="name", right_on="onet", how="inner")
    df = df.rename(columns={"bgtocc": "bgtOccInfo"})

    logging.info(
        f"-- BLS data shape: {str(df_bls.shape)}. Mapping data shape: {str(df_map.shape)}\n-- Merged 2019-2029 BLS data (with 2010 ONET names) with 2010 ONET names to BgtOcc. Dataframe shape: {str(df.shape), str(df_bls.shape), str(df_map.shape)}",
    )

    occu["bgtOccInfo_lower"] = occu["bgtOccInfo"].str.lower()

    # Merging the above data with occupations table data using bgtOccInfo column
    df = occu.merge(df, how="left", left_on="bgtOccInfo_lower", right_on="bgtOccInfo")
    df = df.drop(
        ["name_y", "employmentGrowth_x", "bgtOccInfo_lower", "bgtOccInfo_y"], axis=1
    )
    df = df.rename(
        columns={
            "name_x": "name",
            "employmentGrowth_y": "employmentGrowth",
            "onet": "ONET_name",
            "bgtOccInfo_x": "bgtOccInfo",
        }
    )

    logging.info(
        f"-- Merged BLS data with occupation table. New occupation table shape: {str(df.shape)}",
    )
    return df


def connection():
    fs = gcsfs.GCSFileSystem(project=project_id)

    # Open a DB Connection/session for Sqlalchemy
    DATABASE_URL = "postgresql://{}:{}@{}:{}/{}".format(
        PC.DB_USER, PC.DB_PWD, PC.DB_ENV, PC.DB_PORT, PC.DB_NAME
    )
    engine = create_engine(DATABASE_URL)
    conn = engine.connect()
    return fs, conn, engine
