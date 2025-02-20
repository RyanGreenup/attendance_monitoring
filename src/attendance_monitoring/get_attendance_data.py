#!/usr/bin/env python
"""
This script retrieves attendance data from SEQTA and saves it to disk.

The data is retrieved from the SEQTA endpoint in XML format and converted to
JSON before being saved to disk. The JSON data is then loaded into a polars
DataFrame and saved to disk in parquet format.

The script also saves the data to a DuckDB database file. However, this is
likely to be removed in the future as a separate script will be used to
combine multiple parquet files into a single DuckDB database file.

That DuckDB database will then be used to query the data and generate reports.
"""

import os
import polars as pl
import xmltodict
import json
import requests
import duckdb
from typing import List, Optional
from pydantic import BaseModel, TypeAdapter
import datetime
from dotenv import load_dotenv
import typer


class AttendanceRecord(BaseModel):
    student_code: str
    absence_date: datetime.date
    period_code: int  # This is a code
    attendance_code: str
    trigger_absentee_sms: bool
    considered_late: bool
    resolved: bool
    on_campus: bool
    authorised: bool
    start_time: datetime.time
    end_time: datetime.time
    comments: Optional[str] = (
        None  # Making this optional in case it's not always present
    )


class AttendanceResponse(BaseModel):
    timestamp: str
    data: List[AttendanceRecord]


def get_seqta_password() -> str:
    load_dotenv()
    if (password := os.getenv("SEQTA_PASSWORD")) is None:
        raise ValueError("The environment variable SEQTA_PASSWORD is not set.")
    return password


def main(
    api_url: str = "https://ta.sirius.vic.edu.au/mgm/attendance",
    start_date: str = "2024-11-11",
    username: str = "mgm",
    cache_json: bool = False,
    output_dir: str = "data/raw",
) -> None:
    url = f"{api_url}?date={start_date}"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    print(f"Making request to {url}", end="... ")
    attendance_data = make_request(url, username, get_seqta_password(), cache_json)
    print("[SUCCESS]")

    print("Creating DataFrame", end="... ")
    df = pl.DataFrame(attendance_data)
    print("[SUCCESS]")
    print("Writing to disk", end="... ")
    write_table_to_disk(df, output_dir, "attendance_records", "attendance_records")
    print("[SUCCESS]")


def write_table_to_disk(
    df: pl.DataFrame, output_dir: str, table_name: str, db_file_name: str
) -> None:
    # Save this
    df.write_parquet(f"{output_dir}/{table_name}.parquet")
    write_to_duckdb(df, f"{output_dir}/{db_file_name}.duckdb", table_name)


def make_request(
    url: str, username: str, password: str, cache_json: bool = False
) -> List[AttendanceRecord]:
    # Make the GET request with basic authentication
    response = requests.get(url, auth=(username, password))
    # Check the response

    if response.status_code != 200:
        print("Error: Could not retrieve data")
        print(response.status_code)

    # The content is xml, convert to json
    dict_data = xmltodict.parse(response.content)

    if cache_json:
        print("Caching JSON data", end="... ")
        # Save to a file
        with open("attendance_data.json", "w") as file:
            json.dump(dict_data, file)
        print("[SUCCESS]")

    ta_attendance_response = TypeAdapter(AttendanceResponse)

    attendance_response = ta_attendance_response.validate_python(dict_data["response"])
    return attendance_response.data


def write_to_duckdb(df: pl.DataFrame, db_file_path: str, table_name: str) -> None:
    """
    Writes to a duckdb database file from a polars DataFrame
    and overwrites the table if it already exists.
    """
    # Connect to the DuckDB file
    con = duckdb.connect(database=db_file_path, read_only=False)

    try:
        # Create a temporary in-memory table from pandas DataFrame
        con.register("temp_table", df)

        # CTAS (Create Table As) method to create and populate the table in the database
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_table")

        # Verify the table exists and has the correct data
        print(
            con.execute(f"SELECT * FROM {table_name} LIMIT 10")
            .pl()
            .to_pandas()
            .to_markdown(index=False)
        )

    finally:
        # Close connection to the database file
        con.close()


if __name__ == "__main__":
    typer.run(main)
