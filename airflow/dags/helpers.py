import logging
from datetime import datetime
from typing import List

import pandas as pd
from google.cloud import storage


# Define the data types for each column in the CSV file
data_types = {
  "ride_id": str,
  "rideable_type": str,
  "start_station_name": str,
  "end_station_name": str,
  "start_station_id": str,
  "end_station_id": str,
  "start_lat": float,
  "start_lng": float,
  "end_lat": float,
  "end_lng": float,
  "member_casual": str,
}

def load_data(src_file: str) -> pd.DataFrame:
  """Load CSV file into a Pandas DataFrame with the correct data types."""
  date_parser = lambda date: datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
  parse_dates: List[str] = ["started_at", "ended_at"]
  df = pd.read_csv(src_file, parse_dates=parse_dates, date_parser=date_parser)
  df = df.astype(data_types)
  return df


def convert_to_parquet(src_file: str, dest_file: str) -> None:
  """Converts CSV files to Parquet format."""
  if not src_file.endswith(".csv"):
    logging.error("Can only accept source files in CSV format, for the moment")
    return
  if src_file.endswith("202209-divvy-tripdata.csv"):
    src_file = src_file.replace("tripdata.csv", "publictripdata.csv") 

  df = load_data(src_file)
  df.to_parquet(dest_file, index=False)


def upload_to_gcs(bucket: str, object_name: str, local_file: str) -> None:
  """Uploads a file to Google Cloud Storage."""
  # Increase the chunk size to speed up the upload process
  # and prevent timeouts for large files.
  # OR change 50 to 5 if files > MB on 800 kbps upload sped
  storage.blob._MAX_MULTIPART_SIZE = 50 * 1024 * 1024  
  storage.blob._DEFAULT_CHUNKSIZE = 50 * 1024 * 1024  

  # Upload the file to GCS
  client = storage.Client()
  bucket = client.bucket(bucket)
  blob = bucket.blob(object_name)
  blob.upload_from_filename(local_file)
