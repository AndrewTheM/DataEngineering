import os
import json
import csv
from flatten_json import flatten

JSON = "data"
CSV = "csv"

def json_to_csv(json_folder, csv_folder):
    for root, _, files in os.walk(json_folder):
        for json_file_name in [f for f in files if f.endswith(".json")]:
            json_file_path = os.path.join(root, json_file_name)
            csv_file_path = os.path.join(csv_folder, json_file_name.replace(".json", ".csv"))

            with open(json_file_path, "r") as json_file:
                data = json.load(json_file)
            
            json_data = flatten(data)
            with open(csv_file_path, "w", newline="") as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=json_data.keys())
                csv_writer.writeheader()
                csv_writer.writerow(json_data)

def main():
    os.makedirs(CSV, exist_ok=True)
    json_to_csv(JSON, CSV)

if __name__ == "__main__":
    main()