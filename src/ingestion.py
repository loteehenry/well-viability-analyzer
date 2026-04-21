"""
Data ingestion module - ingestion.py

- Ensures the existence of the file, validates file type(csv)
- Validates Schema
- Tags VALID and FLAGGED rows
- Outputs errors and parsed data
"""

import os
import csv

required_columns = [
    "well_id", "date", "oil_production_bbl",
    "gas_production_mcf", "operating_cost_usd",
    "oil_price_usd_per_bbl", "gas_price_usd_per_mcf",
    "downtime_hours", "water_cut_percent"
]

# function to validate file as csv
def validate_file(file_path: str) -> None:
    # Validate file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found")
    # Validate the file is csv
    if not file_path.lower().endswith('.csv'):
        raise ValueError(f"File {file_path} is not a CSV file")
    # Ensure file is openable and readable
    try:
        with open(file_path, "r", newline='') as f:
            reader = csv.reader(f)
            next(reader) # try reading first row
    except Exception as ex:
        raise IOError(f"File {file_path} could not be opened, {ex}")

# Validates csv file schema
def validate_schema(file_path: str) -> None:
    try:
        with open(file_path, "r", newline='') as f:
            reader = csv.reader(f)
            header = [col.strip().lower() for col in next(reader)]
            missing = [col for col in required_columns if col not in header]
            if missing:
                raise ValueError(f"Missing required column(s) {missing}")
    except Exception as ex:
        raise ValueError(f"Schema validation failed, {ex}")

# Main function to structure and organize the data
# Prints error logs and returns parsed data
def structure_data(file_path: str) -> list:
    # Ensures proper files before work begins
    validate_file(file_path)
    validate_schema(file_path)

    parsed_data = []
    error_log = []
    seen_keys = set()

    with open(file_path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header_row = [header.strip().lower() for header in header]
        col_idx = {col : header_row.index(col) for col in required_columns}

        for row_index, row in enumerate(reader, start=1):
            # initialize data dictionary
            data = {}
            # check for empty rows
            if not any(cell.strip() for cell in row):
                error_log.append({
                    "error": "EMPTY ROW",
                    "error_index": row_index,
                })
                continue
            # check for empty cells
            if any(cell.strip() == "" for cell in row):
                error_log.append({
                    "error": "EMPTY CELL",
                    "error_index": row_index,
                })
            # data conversion and store into temp data obj
            try:
                data["status"] = "VALID"
                data["well_id"] = row[col_idx["well_id"]]
                data["date"] = row[col_idx["date"]]
                data["oil_production_bbl"] = round(float(row[col_idx["oil_production_bbl"]]), 2)
                data["gas_production_mcf"] = round(float(row[col_idx["gas_production_mcf"]]), 2)
                data["operating_cost_usd"] = round(float(row[col_idx["operating_cost_usd"]]), 2)
                data["oil_price_usd_per_bbl"] = round(float(row[col_idx["oil_price_usd_per_bbl"]]), 2)
                data["gas_price_usd_per_mcf"] = round(float(row[col_idx["gas_price_usd_per_mcf"]]), 2)
                data["downtime_hours"] = round(float(row[col_idx["downtime_hours"]]), 2)
                data["water_cut_percent"] = round(float(row[col_idx["water_cut_percent"]]), 2)
                data["profit"] = round(float(((data["oil_production_bbl"] * data["oil_price_usd_per_bbl"]) + (data["gas_production_mcf"] * data["gas_price_usd_per_mcf"])) - data["operating_cost_usd"]), 2)
            except Exception as ex:
                error_log.append({
                    "error": f"TYPE CONVERSION ERROR {ex}",
                    "error_index": row_index,
                })
                data["status"] = "FAILED"
            # moved check here to avoid conversion failure at date
            if data["status"] == "VALID":
                # duplicate check
                key = (data["well_id"], data["date"])
                if key in seen_keys:
                    error_log.append({
                        "error": "DUPLICATE RECORD",
                        "error_index": row_index,
                    })
                    data["status"] = "FAILED"
                seen_keys.add(key)
            # appends only status = "VALID" to parsed_data
            if data["status"] == "VALID":
                parsed_data.append(data)

    print("ERROR LOG:\n")
    if len(error_log) > 0:
        for err in error_log:
            print(f"Error {err['error']} at row {err['error_index']}")
    else:
        print("Zero Errors Found...")
        print("\n\n")

    return parsed_data
