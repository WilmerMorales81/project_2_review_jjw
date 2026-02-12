from dotenv import load_dotenv
import boto3
import os
import polars as pl
import my_logger as ml
import s3_services as s3s
from src.upload import raw_data_key, cleaned_data_key


def main():

    logger = ml.get_logger()
    load_dotenv()

    bucket = os.getenv('AWS_BUCKET')
    profile = os.getenv('AWS_PROFILE')

    session = boto3.Session(profile_name=profile)
    s3 = boto3.client('s3')
    print(f"Using profile: {session.profile_name}")

    raw_data_lf = pl.scan_csv(
        raw_data_key,
        infer_schema_length=10_000
    )
    logger.info("CSV scanned lazily")

    raw_s3_path = s3s.write_parquet_to_s3(
        raw_data_lf,
        bucket,
        raw_data_key,
        profile
    )
    logger.info(f"Parquet written to {raw_s3_path}")

    clean_data_lf = pl.scan_csv(
        cleaned_data_key,
        infer_schema_length=10_000
    )
    logger.info("Parquet scanned lazily")

    clean_s3_path = s3s.write_clean_parquet_to_s3(
        clean_data_lf,
        bucket,
        cleaned_data_key,
        profile
    )
    logger.info(f"Cleaned parquet written to {clean_s3_path}")


if __name__ == "__main__":
    main()
