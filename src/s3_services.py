from io import StringIO
import os
import tempfile
from typing import Union
import polars as pl
import boto3
import src.my_logger as ml
from pathlib import Path


logger = ml.get_logger()

# Conditional S3 session creation, defaults to standard session if no profile is provided


def get_s3_session(profile_name: str = None):
    if profile_name:
        session = boto3.Session(profile_name=profile_name)
    else:
        session = boto3.Session()
    return session.client('s3')

# Reads CSV file in S3, into a Polars DataFrame, with optional schema inference limit


def read_csv_from_s3(bucket_name: str, key: str, profile_name: str = None, infer_schema_length: int = 10000):
    s3 = get_s3_session(profile_name)
    response = s3.get_object(Bucket=bucket_name, Key=key)

    # Read the CSV into memory, [.decode("utf-8")] converts bytes into string
    csv_content = response["Body"].read().decode("utf-8")
    # StringIO(csv_content) turns the string into a file-like object
    # infer_schema_length limits the number of rows
    df = pl.read_csv(StringIO(csv_content),
                     infer_schema_length=infer_schema_length)
    logger.info(f"Read {df.height} records from {key}")
    return df

# Uploads a CSV file to AWS s3 bucket


def upload_csv_to_s3(file_name: str, bucket_name: str, key: str, profile_name: str = None):
    # Path(),turns the string path into a Path object from pathlib
    file_path = Path(file_name)
    s3 = get_s3_session(profile_name)
    # str(file_path) converts the Path object back to a string, for upload_file which expects a string path
    # upload_file() efficiently handles multipart uploads for large files
    s3.upload_file(str(file_path), bucket_name, key)
    logger.info(f"Uploaded {file_name} to s3://{bucket_name}/{key}")


# Gets Polars DataFrame, converts to CSV in memory, and uploads CSV to S3 directly (no temporary files)
def upload_polars_to_s3(df: pl.DataFrame, bucket: str, key: str, profile_name: str = None):
    s3 = get_s3_session(profile_name)

    # in-memory buffer for CSV data, avoids disk I/O, efficient for small to medium datasets (up to a few hundred MBs, depending on memory)
    csv_buffer = StringIO()

    # Write df as CSV, to the in-memory buffer, then reset buffer back to 0 (beginning), for upload
    df.write_csv(csv_buffer)
    csv_buffer.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    logger.info(f"Uploaded {df.height} rows to s3://{bucket}/{key}")


# Write DataFrame to S3 as Parquet (lazy or eager)
def write_parquet_to_s3(
    df: Union[pl.DataFrame, pl.LazyFrame],
    bucket: str,
    key: str,
    profile: str,
):
    """
    Writes a DataFrame or LazyFrame to S3 as Parquet.
    {key} example: nppes/raw/
    """
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")

    # Uses Polars’ streaming engine to reduce memory usage for large datasets (if it's a LazyFrame)
    if isinstance(df, pl.LazyFrame):
        df = df.collect(streaming=True)

    # Writes to a temporary file first (parquet format), then uploads to S3 to avoid memory issues with large datasets
    # (delete=False) doesn't delete it immediately, so it's available for upload before cleanup
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        df.write_parquet(tmp.name)
        tmp_path = tmp.name

    # rstrip('/') removes trailing slashes from key, for consitent output path e.g.(nppes/raw/data.parquet instead of nppes/raw//data.parquet)
    s3_key = f"{key.rstrip('/')}/data.parquet"

    # Uploads using upload_fileobj, more efficient for large files than put_object, automatically handles multipart uploads
    with open(tmp_path, "rb") as f:
        s3.upload_fileobj(f, bucket, s3_key)

    # Clean up the temp file, prevent clutter/leaks
    os.remove(tmp_path)

    return f"s3://{bucket}/{s3_key}"


# Write a (Cleaned) DataFrame to S3 as Parquet (lazy or eager)
def write_clean_parquet_to_s3(
    df: Union[pl.DataFrame, pl.LazyFrame],
    bucket: str,
    key: str,
    profile: str,
):
    """
    Writes a DataFrame or LazyFrame to S3 as Parquet.
    {clean_key} example: nppes/clean/
    """
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")

    # Uses Polars’ streaming engine to reduce memory usage for large datasets (if it's a LazyFrame)
    if isinstance(df, pl.LazyFrame):
        df = df.collect(streaming=True)

    # Writes to a temporary file first (parquet format), then uploads to S3 to avoid memory issues with large datasets
    # (delete=False) doesn't delete it immediately, so it's available for upload before cleanup/analysis
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        df.write_parquet(tmp.name)
        tmp_path = tmp.name

    # rstrip('/') removes trailing slashes from key, for consitent output path e.g.(nppes/clean/clean_data.parquet instead of nppes/clean//clean_data.parquet)
    s3_key = f"{key.rstrip('/')}/clean_data.parquet"

    # Uploads using upload_fileobj, more efficient for large files than put_object, automatically handles multipart uploads
    with open(tmp_path, "rb") as f:
        s3.upload_fileobj(f, bucket, s3_key)

    # Clean up the temp file, prevent clutter/leaks
    os.remove(tmp_path)

    return f"s3://{bucket}/{s3_key}"


# Creates a Polars LazyFrame that points to Parquet files in S3 without loading them unitl, collect(),fetch(), or sink_*() is called
def scan_parquet_from_s3(
    bucket: str,
    key: str,
    profile: str,
) -> pl.LazyFrame:
    """
    Lazily scans Parquet files from S3.
    """

    session = boto3.Session(profile_name=profile)
    # get_frozen_credentials() stores the credential, to prevent credential rotation/expiration
    creds = session.get_credentials().get_frozen_credentials()

    # Credentials for AWS access
    storage_options = {
        "aws_access_key_id": creds.access_key,
        "aws_secret_access_key": creds.secret_key,
        "aws_session_token": creds.token,
        "region": session.region_name,
    }

    # rstrip('/') removes trailing slashes from key, for consitent output path e.g.(nppes/raw/*.parquet instead of nppes/raw//*.parquet)
    # The (*.parquet) pattern acts as a glob, letting Polars read/match all/only parquet files in the specific s3
    s3_path = f"s3://{bucket}/{key.rstrip('/')}/*.parquet"

    return pl.scan_parquet(
        s3_path,
        storage_options=storage_options
    )
