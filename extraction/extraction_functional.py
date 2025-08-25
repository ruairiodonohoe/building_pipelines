import json
import sqlite3
import certifi
import pandas as pd
import urllib3
import requests
from io import StringIO

import logging

logger = logging.getLogger(__name__)


def source_data_from_csv(csv_filename):
    try:
        df = pd.read_csv(csv_filename)
        logger.info(
            f"{csv_filename} : extracted {df.shape[0]} records from the csv file"
        )
    except Exception as e:
        logger.exception(
            f"{csv_filename} : - exception {e} encountered while extracting the csv file."
        )
        df = pd.DataFrame()
    return df


def source_data_from_parquet(parquet_filename):
    try:
        df = pd.read_parquet(parquet_filename)
        logger.info(
            f"{parquet_filename} : extracted {df.shape[0]} records from the parquet file."
        )
    except Exception as e:
        logger.exception(
            f"{parquet_filename} : - exception {e} encountered while extracting the parquet file"
        )
        df = pd.DataFrame()
    return df


def source_data_from_api(api_endpoint):
    try:
        apt_status = urllib3.request("GET", api_endpoint).status

        if apt_status == 200:
            logger.info(f"{apt_status} - ok : while invoking the api {api_endpoint}")
            http = urllib3.PoolManager(
                cert_reqs="CERT_REQUIRED", ca_certs=certifi.where()
            )
            data = json.loads(http.request("GET", api_endpoint).data.decode("utf-8"))
            df = pd.json_normalize(data)
            logger.info(f"{apt_status} - extracted {df.shape[0]} records from the api.")
        else:
            df = pd.DataFrame()
    except Exception as e:
        df = pd.DataFrame()

    return df


def source_data_from_db(db_name, table_name):
    try:
        with sqlite3.connect(db_name) as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            logger.info(
                f"{db_name} - read {df.shape[0]} records from the table: {table_name}"
            )
    except Exception as e:
        logger.exception(
            f"{db_name} : - exception {e} encoutered while reading date from table: {table_name}"
        )
        df = pd.DataFrame()
    return df


def source_data_from_webpage(url, keyword):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "ApplWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        html_io = StringIO(response.text)
        df_list = pd.read_html(html_io, match=keyword)
        df = df_list[0]
        logger.info(f"{url} - read {df.shape[0]} records from the page: {url}")
    except Exception as e:
        logging.exception(
            f"{url} : - exception {e} encoutered while reading data from page : {url}"
        )
        df = pd.DataFrame()
    return df


def extracted_data():
    parquet_filename = "data/yellow_tripdata_2022-01.parquet"
    csv_flename = "data/h9gi-nx95.csv"
    api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
    db_name = "data/movies.sqlite"
    table_name = "movies"
    webpage_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    matching_keyword = "by country"

    df_parquet, df_csv, df_api, df_db, df_html = (
        source_data_from_parquet(parquet_filename),
        source_data_from_csv(csv_flename),
        source_data_from_api(api_endpoint),
        source_data_from_db(db_name, table_name),
        source_data_from_webpage(webpage_url, matching_keyword),
    )
    return df_parquet, df_csv, df_api, df_db, df_html
