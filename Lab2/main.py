import os
import requests
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOADS = "downloads"

def download_and_extract(url):
    response = requests.get(url)
    if not response.ok:
        return
    
    filename = os.path.basename(url)
    filepath = f"{DOWNLOADS}/{filename}"
    with open(filepath, "wb") as f:
        f.write(response.content)
    
    with zipfile.ZipFile(filepath, "r") as zip_file:
        zip_file.extractall(DOWNLOADS)
    
    os.remove(filepath)
    print(filename)

def main():
    os.makedirs(DOWNLOADS, exist_ok=True)
    for url in download_uris:
        download_and_extract(url)

if __name__ == "__main__":
    main()