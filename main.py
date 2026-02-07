from dotenv import load_dotenv
import boto3
import os


def main():
    load_dotenv()

    s3 = boto3.client('s3')
    bucket = os.getenv('AWS_BUCKET')


if __name__ == "__main__":
    main()

