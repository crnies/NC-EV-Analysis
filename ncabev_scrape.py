import os
import time
import zipfile
import requests
import pandas as pd

# Constants
ABSENTEE_ZIP_URL = "https://dl.ncsbe.gov/ENRS/2024_11_05/absentee_20241105.zip" # Change destination for each new election
DOWNLOAD_DIR = "downloads"
OUTPUT_CSV = "aggregated_absentee_summary.csv"
RATE_LIMIT_SECONDS = 2

# Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def download_and_extract_csv(zip_url, extract_to):
    print(f"Downloading absentee file from: {zip_url}")
    response = requests.get(zip_url)
    time.sleep(RATE_LIMIT_SECONDS)
    zip_path = os.path.join(extract_to, os.path.basename(zip_url))
    with open(zip_path, "wb") as f:
        f.write(response.content)

    with zipfile.ZipFile(zip_path, "r") as z:
        print("Contents of ZIP archive:")
        for name in z.namelist():
            print(f" - {name}")

        csv_files = [name for name in z.namelist() if name.lower().endswith(".csv")]
        if not csv_files:
            raise ValueError("No .csv file found in the ZIP archive.")

        extracted_filename = csv_files[0]
        z.extract(extracted_filename, path=extract_to)
        print(f"Extracted: {extracted_filename}")
        return os.path.join(extract_to, extracted_filename)

def parse_absentee_file(filepath, usecols=None):
    try:
        df = pd.read_csv(filepath, dtype=str, encoding="latin1")
        df.columns = df.columns.str.strip()
        for col in df.columns:
            df[col] = df[col].str.strip()
        print(f"Loaded {len(df):,} records from {os.path.basename(filepath)}")
        return df
    except Exception as e:
        print(f"Failed to parse file: {e}")
        return pd.DataFrame()

# Aggregate Data for Reporting Dataset
def process_data(df, output_csv=OUTPUT_CSV):
    accepted_count = df[df["ballot_rtn_status"] == "ACCEPTED"].shape[0]
    print(f"Total 'ACCEPTED' ballots: {accepted_count:,}\n") # sanity check

    group_cols = [
        "county_desc",
        "race",
        "ethnicity",
        "gender",
        "age",
        "voter_party_code",
        "cong_dist_desc",
        "nc_house_desc",
        "nc_senate_desc",
        "ballot_req_delivery_type",
        "ballot_req_type",
        "ballot_req_dt",
        "ballot_send_dt", "ballot_rtn_dt",
        "ballot_rtn_status"
    ]

    summary = df.groupby(group_cols).size().reset_index(name="ballot_count")
    summary.to_csv(output_csv, index=False)
    print(f"Aggregated data saved to: {output_csv}")

# Run the Script
def main():
    csv_path = download_and_extract_csv(ABSENTEE_ZIP_URL, DOWNLOAD_DIR)
    df = parse_absentee_file(csv_path)
    if not df.empty:
        process_data(df)

if __name__ == "__main__":
    main()
