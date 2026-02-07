from io import StringIO
import os
import tempfile
from typing import Union
import polars as pl
import boto3
import my_logger as ml


logger = ml.get_logger()


def get_s3_session(profile_name: str = None):
    if profile_name:
        session = boto3.Session(profile_name=profile_name)
    else:
        session = boto3.Session()
    return session.client('s3')


def upload_polars_to_s3(df: pl.DataFrame, bucket: str, key: str, profile_name: str = None):
    s3 = get_s3_session(profile_name)

    csv_buffer = StringIO()
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

# Clean up the temp file
    os.remove(tmp_path)

    return f"s3://{bucket}/{s3_key}"


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

    if isinstance(df, pl.LazyFrame):
        df = df.collect(streaming=True)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        df.write_parquet(tmp.name)
        tmp_path = tmp.name

    s3_key = f"{key.rstrip('/')}/clean_data.parquet"

    with open(tmp_path, "rb") as f:
        s3.upload_fileobj(f, bucket, s3_key)

    os.remove(tmp_path)

    return f"s3://{bucket}/{s3_key}"


def scan_parquet_from_s3(
    bucket: str,
    key: str,
    profile: str,
) -> pl.LazyFrame:
    """
    Lazily scans Parquet files from S3.
    """

    session = boto3.Session(profile_name=profile)
    creds = session.get_credentials().get_frozen_credentials()

    storage_options = {
        "aws_access_key_id": creds.access_key,
        "aws_secret_access_key": creds.secret_key,
        "aws_session_token": creds.token,
        "region": session.region_name,
    }

    s3_path = f"s3://{bucket}/{key.rstrip('/')}/*.parquet"

    return pl.scan_parquet(
        s3_path,
        storage_options=storage_options
    )
