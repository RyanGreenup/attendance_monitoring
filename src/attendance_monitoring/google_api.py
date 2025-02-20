import os
from pathlib import Path
import tempfile
from google.oauth2.service_account import Credentials
import io
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
from google.oauth2 import service_account
import sys
import polars as pl


def get_credentials() -> Credentials:
    """
    Retrieves and returns Google Drive API credentials from a service account file.

    The function reads the credentials from a specified JSON key file, which is associated with
    a Google Service Account. These credentials are required to authenticate requests made against
    the Google Drive API for read-only access to drive metadata.

    Returns:
        Credentials: An object containing the necessary authentication details for interacting
                     with the Google Drive API.

    Note:
        The scope for read-only metadata access has been commented out in the code, meaning that
        the credentials will be created without any specific scopes. This implies that the default
        scopes associated with the service account may apply instead.

    Raises:
        FileNotFoundError: If the specified SERVICE_ACCOUNT_FILE does not exist.
        ValueError: If the content of the SERVICE_ACCOUNT_FILE is invalid or improperly formatted.

    """

    # Setup the Drive API
    SCOPES = [
        "https://www.googleapis.com/auth/drive.metadata.readonly",
        "https://www.googleapis.com/auth/drive.file",
    ]
    SERVICE_ACCOUNT_FILE = os.path.expanduser(
        "~/.local/keys/google_drive_oauth2_key.json"
    )

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=None,  # scopes=SCOPES
    )
    return credentials


def get_files() -> dict[str, str]:
    """
    Retrieves a list of files from Google Drive and returns their IDs and names as a dictionary.

    Returns:
        dict[str, str]: A dictionary where each key is the ID of a file in Google Drive,
                        and its corresponding value is the name of that file.

    Notes:
        - This function uses Google Drive API v3 to fetch details about files.
        - The credentials required for authenticating with the Google Drive service are obtained from `get_credentials()`.
        - Only the first 10 files (pageSize=10) are fetched, including their IDs and names.
        - In case of an unexpected error while processing a file's ID or name, it is logged to stderr but does not halt the function execution.

    Example:
        >>> get_files()
        {'file_id_1': 'example_file.txt', 'file_id_2': 'another_example.pdf'}

    Raises:
        Any exception raised by the Google Drive API calls will be propagated.
    """
    credentials = get_credentials()
    service = build("drive", "v3", credentials=credentials)

    # Call the Drive v3 API to list files
    results = (
        service.files()
        .list(pageSize=10, fields="nextPageToken, files(id, name)")
        .execute()
    )

    # This returns [{'id': 'xxxxx', 'name': 'foo_bar.txt'}, {}...]
    items_dict: list[dict[str, str]] = results.get("files", [])

    d = dict()
    for item in items_dict:
        try:
            id = item["id"]
            name = item["name"]
            d[id] = name
        except Exception as e:
            print(e, file=sys.stderr)
    return d


def get_file_bytes(file_id: str) -> bytes:
    """
    Downloads the content of a specified file from Google Drive and returns it as bytes.
    Typically one would then write these bytes to a file:

    ```python
    with open("/tmp/path/to_file", "wb") as fp:
        fp.write(fb)
    ```

    Args:
        file_id (str): The unique identifier of the file in Google Drive to be downloaded.

    Returns:
        bytes: The raw byte content of the file.

    Raises:
        Any exceptions raised by the Google Drive API calls or during download will be propagated.

    Notes:
        - This function uses the Google Drive v3 API to fetch the file.
        - Credentials for authentication are obtained from `get_credentials()`.
        - A chunk downloader is used to handle large files, otherwise they may time out.
        - The download progress % is printed to stdout

    Example:
        >>> get_file_bytes('file_id_123')
        b'\\x00...<raw byte content>...'
    """
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    # Build a Chunk Downloader to handle large files
    request = service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)

    # Download and combine the chunks
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"\rDownload {int(status.progress() * 100)}.", end="")
    print("")

    return file.getvalue()


def get_file_name(file_id: str) -> str:
    """
    Retrieves the name of a file from Google Drive using its ID.

    Args:
        file_id (str): The unique identifier (ID) of the file in Google Drive.

    Returns:
        str: The name of the file associated with the given ID.

    Raises:
        FileNotFoundError: If the provided `file_id` does not exist or is not found in the files retrieved from Google Drive.

    Notes:
        - This function relies on the `get_files()` function to fetch a dictionary of file IDs and their names.
        - If the `file_id` is not present in the dictionary returned by `get_files()`, a KeyError is caught, and a FileNotFoundError is raised with an appropriate message.

    Example:
        >>> get_file_name('1234567890')
        'example_document.pdf'

        >>> get_file_name('invalid_id')
        Traceback (most recent call last):
            ...
        FileNotFoundError: ID: invalid_id not found on Google Drive, check it's been shared with the service account
    """
    try:
        file_name = get_files()[file_id]
    except KeyError:
        raise FileNotFoundError(
            f"ID: {file_id} not found on Google Drive, check it's been shared with the service account"
        )

    return file_name


def download_file(dir: Path, file_id: str) -> Path | None:
    """
    Downloads a file from Google Drive and saves it to the specified directory.

    Args:
        dir (Path): The directory path where the downloaded file will be saved.
        file_id (str): The ID of the file in Google Drive to download.

    Returns:
        Path | None: The path to the downloaded file if successful, otherwise `None`.

    Raises:
        IOError: If the provided path is not a directory or does not exist and cannot be created.

    Notes:
        - This function first checks whether the specified directory exists. If it doesn't, an `IOError` is raised.
        - The function creates the directory if it doesn't already exist.
        - It retrieves the file name using the `get_file_name(file_id)` function.
        - It then attempts to download the file bytes with `get_file_bytes(file_id)`.
        - If the file bytes are successfully retrieved, they are written to a file in the specified directory.

    Example:
        >>> from pathlib import Path
        >>> download_file(Path("/path/to/directory"), "file_id_123")
        PosixPath('/path/to/directory/filename.ext')
    """
    # Check that it's a directory
    if dir.exists():
        if not dir.is_dir():
            raise IOError("Must provide a directory path to write file")

    # Create the directory if it doesn't exist
    os.makedirs(dir, exist_ok=True)

    file_path = dir / get_file_name(file_id)

    # Write the bytes
    if fb := get_file_bytes(file_id):
        with open(file_path, "wb") as fp:
            fp.write(fb)
        return file_path

    return None


def read_excel(file_id: str) -> dict[str, pl.DataFrame]:
    """
    Reads an Excel file from Google Drive and returns its sheets as Polars DataFrames.

    Args:
        file_id (str): The ID of the Excel file in Google Drive.

    Returns:
        dict[str, pl.DataFrame]: A dictionary where each key is the name of a sheet,
                                 and the corresponding value is a Polars DataFrame containing the data from that sheet.

    Notes:
        - This function first retrieves the bytes of the specified Excel file using `get_file_bytes`.
        - The temporary file is created in the system's default temporary directory.
        - After reading the file, it uses Polars (`pl.read_excel`) to parse the Excel sheets into DataFrames.
        - The `infer_schema_length=None` argument ensures that the schema is inferred from all rows rather than a sample. Important for Excel which is loosely typed.

    Raises:
        FileNotFoundError: If the file could not be retrieved or written to disk.
        ValueError: If there are issues with reading the Excel file using Polars.

    Example:
        >>> read_excel('1234567890abcdef')
        {'Sheet1': shape: (10, 5),
         'Sheet2': shape: (20, 3)}
    """
    file = tempfile.mktemp()
    if fb := get_file_bytes(file_id):
        with open(file, "wb") as fp:
            fp.write(fb)
    return pl.read_excel(file, infer_schema_length=None, sheet_id=0)


def read_csv(file_id: str) -> pl.DataFrame:
    """
    Reads a CSV, refer to the read_excel docstring.
    """
    file = tempfile.mktemp()
    if fb := get_file_bytes(file_id):
        with open(file, "wb") as fp:
            fp.write(fb)
    return pl.read_csv(file, infer_schema_length=None)


def read_parquet(file_id: str) -> pl.DataFrame:
    """
    Reads a Parquet File, refer to the read_excel docstring.
    """
    file = tempfile.mktemp()
    if fb := get_file_bytes(file_id):
        with open(file, "wb") as fp:
            fp.write(fb)
    return pl.read_parquet(file)


def upload_file(file_path: str | Path, file_id: str) -> None:
    """
    Uploads a new version of an existing file to Google Drive.

    Args:
        file_path (str | Path): Path to the local file to upload
        file_id (str): The ID of the existing file in Google Drive to update

    Raises:
        FileNotFoundError: If the local file doesn't exist
        HttpError: If there are issues with the upload or file_id doesn't exist

    Example:
        >>> # Modify the file on Google Drive
        >>> # https://drive.google.com/file/d/abcxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/view?usp=drive_link
        >>> file_id="abcxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        >>> file_path="/tmp/file.txt"
        >>> with open(file_path, "w") as fp:
        >>>    fp.write("New Content")
        >>> upload_file("/tmp/file.txt", file_id)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Local file {file_path} not found")

    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    # Create media upload object
    media = MediaFileUpload(file_path, resumable=True)

    # Update the file content
    _file = service.files().update(fileId=file_id, media_body=media).execute()


def create_file(file_path: str | Path, name: str | None = None) -> str:
    """
    Creates a new file in Google Drive and returns its ID.
    This doesn't work because the service account has a separate account
    and it doesn't seem to actually create a file that can be shared later.
    This is only included as documentation, use the create_file_under_shared_directory
    function instead.

    Args:
        file_path (str | Path): Path to the local file to upload
        name (str | None): Optional name for the file in Drive. If None, uses local filename

    Returns:
        str: The ID of the newly created file in Google Drive

    Raises:
        FileNotFoundError: If the local file doesn't exist
        HttpError: If there are issues with the upload

    Example:
        >>> # Create a new file on Google Drive
        >>> file_path = "/tmp/new_file.txt"
        >>> with open(file_path, "w") as fp:
        >>>     fp.write("Hello World")
        >>> file_id = create_file(file_path, "greeting.txt")
        >>> print(f"New file ID: {file_id}")
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Local file {file_path} not found")

    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    # Use provided name or get filename from path
    file_metadata = {"name": name if name else Path(file_path).name}

    # Create media upload object
    media = MediaFileUpload(file_path, resumable=True)

    # Create the file
    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )

    return file.get("id")


def create_file_under_shared_directory(
    file_path: str | Path, parent_folder_id: str, name: str | None = None
) -> str:
    """
    Creates a new file in a specific Google Drive folder and returns its ID.

    Args:
        file_path (str | Path): Path to the local file to upload
        parent_folder_id (str): The ID of the folder where the file should be created
        name (str | None): Optional name for the file in Drive. If None, uses local filename

    Returns:
        str: The ID of the newly created file in Google Drive

    Raises:
        FileNotFoundError: If the local file doesn't exist
        HttpError: If there are issues with the upload

    Example:
        >>> file_path = "/tmp/new_file_from_api.txt"
        >>> with open(file_path, "w") as fp:
        >>>     fp.write("Hello from the Python API")
        >>> file_id = create_file(file_path, "greeting.txt")
        >>> parent_id="15cPwxQHydxoCC4Q3NwryTg-t15mUGtbT"
        >>> create_file_under_shared_directory(file_path, parent_id)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Local file {file_path} not found")

    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    # Use provided name or get filename from path
    file_metadata = {
        "name": name if name else Path(file_path).name,
        "parents": [parent_folder_id],  # Specify the parent folder
    }

    # Create media upload object
    media = MediaFileUpload(file_path, resumable=True)

    # Create the file
    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )

    return file.get("id")


def share_file(file_id: str, email: str, role: str = "writer") -> None:
    """
    Shares a Google Drive file with a user based on their email address.

    Args:
        file_id (str): The ID of the file to share
        email (str): The email address of the user to share with
        role (str): The role to grant. One of "reader", "writer", "commenter", "fileOrganizer", "organizer", "owner"
                   Defaults to "reader"

    Raises:
        HttpError: If there are issues with sharing (e.g., invalid file_id or email)
        ValueError: If an invalid role is specified

    Example:
        >>> # Share a file with read access
        >>> share_file("abc123xyz", "user@example.com")
        >>>
        >>> # Share a file with write access
        >>> share_file("abc123xyz", "editor@example.com", role="writer")
    """
    valid_roles = {
        "reader",
        "writer",
        "commenter",
        "fileOrganizer",
        "organizer",
        "owner",
    }
    if role not in valid_roles:
        raise ValueError(f"Role must be one of: {', '.join(valid_roles)}")

    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)

    # Create the permission
    permission = {"type": "user", "role": role, "emailAddress": email}

    # Share the file
    service.permissions().create(
        fileId=file_id, body=permission, sendNotificationEmail=True
    ).execute()


def pull_table(database: str, table_name) -> pl.DataFrame:
    tables = {
        "postgres": {
            "subject": "1fJ7l2qUQpkmTV9AqVe7JSEqqcFhBnVmz",
            "programmegrade": "1b3V2dOjCr6wzmPDFaYrVQ1mRT93OUq0y",
            "summarised_student_details": "1kaN3lRVkwzX8cW9tFrDdR4H30qcQjG9L",
            "summarised_academic_result": "1az_v-_ceMZrQIhQrZnpwaATpndPSHH1-",
            "vw_student_details": "1pv8qStJ7Qvq9WvFU9K1PX9-tNfLjizOO",
            "classinstance": "1aU65uOhoEFQMdHK57WPjQyLmxJVM5te2",
            "period": "1fJDgv8Fj-Kj_d5O8can5NHcAR8pFljoC",
        },
        "sqlserver": {},
    }

    return read_parquet(tables[database][table_name])
