import os
from dotenv import load_dotenv

load_dotenv()

raw_nppes_key = os.getenv('RAW_NPPES_KEY')
raw_taconomy_key = os.getenv('RAW_TACONOMY_KEY')
raw_fips_key = os.getenv('RAW_FIPS_KEY')
raw_zip_county_key = os.getenv('RAW_ZIP_COUNTY_KEY')

raw_data_key = (
    raw_nppes_key,
    raw_taconomy_key,
    raw_fips_key,
    raw_zip_county_key
)

clean_nppes_key = os.getenv('CLEAN_NPPES_KEY')
clean_taconomy_key = os.getenv('CLEAN_TACONOMY_KEY')
clean_fips_key = os.getenv('CLEAN_FIPS_KEY')
clean_zip_county_key = os.getenv('CLEAN_ZIP_COUNTY_KEY')

cleaned_data_key = (
    clean_nppes_key,
    clean_taconomy_key,
    clean_fips_key,
    clean_zip_county_key
)
