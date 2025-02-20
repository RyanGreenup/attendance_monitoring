from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import typer
from datetime import date
import os
import polars as pl
import datetime
from .get_attendance_data import make_request, get_seqta_password
from .google_api import pull_table

pl.Config(tbl_cols=int(80 / 5), tbl_rows=6)


def _get_cache_directory(start_date: date) -> Path:
    xdg_cache_home = os.environ.get(
        "XDG_CACHE_HOME", Path(os.path.expanduser("~/.cache"))
    )
    cache_dir = os.path.join(xdg_cache_home, "sirius_college", "attendance_data")
    os.makedirs(Path(cache_dir), exist_ok=True)
    cache_file = os.path.join(cache_dir, f"attendance_data-{start_date}")
    return Path(cache_file)


class API_Credentials:
    def __init__(self, start_date: date):
        api_url = "https://ta.sirius.vic.edu.au/mgm/attendance"
        self.url = f"{api_url}?date={start_date}"
        self.password = get_seqta_password()
        self.username = "mgm"


def get_attendance_data(start_date: date) -> pl.DataFrame:
    cache_data = True
    cache_file = _get_cache_directory(start_date)
    try:
        df = pl.read_parquet(cache_file)
    except FileNotFoundError:
        creds = API_Credentials(start_date)
        attendance_data = make_request(creds.url, creds.username, creds.password, False)
        # This takes 30 seconds, however, caching it presents security challenges
        df = pl.DataFrame(attendance_data)
        if cache_data:
            df.write_parquet(cache_file)

    return df


def main():
    # df = join_data(DataStore.DRIVE_API)
    df = join_data(DataStore.LOCAL).sort(["absence_date", "period_code"])
    # (
    #     df.with_columns(absence_ratio=pl.lit(1))
    #     .rolling(index_column="absence_date", period="1mo", group_by="student_code")
    #     .agg([pl.col("absence_ratio").mean()])
    # )

    # df.group_by("student_code", "absence_date", "code").len()
    # ts = pl.DataFrame(df.drop("absence_date"), index=df.select("absence_date"))
    print(df)


class DataStore(Enum):
    DRIVE_API = "drive"
    DRIVE_COLAB = "drive_colab"
    LOCAL = "local"


class DataSource(Enum):
    POSTGRES = "postgres"
    SQL_SERVER = "sql_server"


class DataBase:
    """
    A class to abstract away the database to parquet files, google drive API or google sheets
    """

    def __init__(self, store: DataStore):
        self.dir = Path(
            os.path.expanduser(
                "~/Downloads/work/sirius/gdrive_export/gdrive/Services/data/extracted/parquets/"
            )
        )
        self.store = store

    def get_table(self, source: DataSource, table: str) -> pl.DataFrame:
        match self.store:
            case DataStore.LOCAL:
                path = os.path.join(self.dir, source.value, f"{table}.parquet")
                return pl.read_parquet(path)
            case DataStore.DRIVE_API:
                return pull_table(database=DataSource.POSTGRES.value, table_name=table)
            case _:
                raise NotImplementedError


def join_data(store: DataStore):
    database = DataBase(store)
    # Calculate the date
    today = date.today()
    start_date = today - datetime.timedelta(days=7 * 18)

    df = get_attendance_data(start_date)
    df = df.filter(
        pl.col("attendance_code").str.contains("absenceapproved").not_()
    ).filter(pl.col("resolved").not_())
    print(df[:6, :].to_pandas().to_markdown())

    # TODO pull from Google Drive API
    ci = database.get_table(DataSource.POSTGRES, "classinstance")
    p = database.get_table(DataSource.POSTGRES, "period")
    student = database.get_table(DataSource.POSTGRES, "vw_student_details")
    student = student.select(
        "Student Code",
        "Student First Name",
        "Student Surname",
        "Student Preferred Name",
        "Student DOB",
        "Student Gender",
        "Roll Group",
        "Campus Code",
        "Student Email",
    )
    class_times = (
        ci.rename(
            {
                "period": "period_id",
                "date": "class_date",
                "start": "class_start_time",
                "end": "class_end_time",
            }
        )
        .select(
            ["period_id", "code", "class_date", "class_start_time", "class_end_time"]
        )
        .join(
            p.rename({"code": "period"}).select(["id", "period"]),
            left_on="period_id",
            right_on="id",
        )
        # NOTE strict fills with Null
        .with_columns(period=pl.col("period").cast(pl.Int64, strict=False))
    )

    absence = df.join(
        class_times,
        left_on=["absence_date", "period_code"],
        right_on=["class_date", "period"],
    ).select(
        [
            "student_code",
            "absence_date",
            "period_code",
            "attendance_code",
            "start_time",
            "end_time",
            "comments",
            "period_id",
            "code",
            "class_start_time",
            "class_end_time",
        ]
    )

    return absence.join(student, left_on="student_code", right_on="Student Code")


if __name__ == "__main__":
    typer.run(main)
