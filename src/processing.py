"""
Data processing module - processing.py

- Processes the provided list of object records for each well entry
- Derives required columns for output csv file
"""

# Fields to be returned by processing module:
"""
- well_id
- total_oil_produced_bbl
- total_gas_produced_mcf
- downtime_percent
- average_water_cut
- total_oil_revenue
- total_gas_revenue
- total_operating_cost
- well_profit
"""

def process_data(data: list) -> list:
    print(f"Processing {len(data)} records...")

    # creates a dictionary where each key is the value's well_id
    indexed_data = {record["well_id"]: {"well_id": record["well_id"]} for record in data}

    # loop through well
    for well_id in list(indexed_data.keys()):
        well_count = 0

        total_oil_produced = 0
        total_gas_produced = 0
        total_downtime = 0
        total_water_cut = 0
        total_oil_revenue = 0
        total_gas_revenue = 0
        total_operating_cost = 0

        for record in data:
            if record["well_id"] == well_id:
                well_count += 1
                # find total oil produced bbl
                total_oil_produced += record["oil_production_bbl"]
                # find total gas produced mcf
                total_gas_produced += record["gas_production_mcf"]
                # find total downtime percent
                total_downtime += record["downtime_hours"]
                # find total water cut
                total_water_cut += record["water_cut_percent"]
                # find total oil revenue
                total_oil_revenue += record["oil_production_bbl"] * record["oil_price_usd_per_bbl"]
                # find total gas revenue
                total_gas_revenue += record["gas_production_mcf"] * record["gas_price_usd_per_mcf"]
                # find total operating cost
                total_operating_cost += record["operating_cost_usd"]

        # calculate downtime percent
        downtime_percent = (total_downtime / (24*30)) * 100
        # calculate average water cut
        average_water_cut = total_water_cut / well_count

        indexed_data[well_id]["recorded_days"] = well_count
        indexed_data[well_id]["total_oil_produced_bbl"] = round(total_oil_produced, 2)
        indexed_data[well_id]["total_gas_produced_mcf"] = round(total_gas_produced, 2)
        indexed_data[well_id]["downtime_percent"] = round(downtime_percent, 2)
        indexed_data[well_id]["average_water_cut"] = round(average_water_cut, 2)
        indexed_data[well_id]["total_oil_revenue"] = round(total_oil_revenue, 2)
        indexed_data[well_id]["total_gas_revenue"] = round(total_gas_revenue, 2)

        total_well_revenue = total_oil_revenue + total_gas_revenue
        indexed_data[well_id]["total_well_revenue"] = round(total_well_revenue, 2)

        indexed_data[well_id]["total_operating_cost"] = round(total_operating_cost, 2)
        indexed_data[well_id]["well_profit"] = round(total_well_revenue - total_operating_cost, 2)

        indexed_data[well_id]["trends"] = {
            "profits_trend": [entry["profit"] for entry in data if entry["well_id"] == well_id][-10:],
            "oil_production_trend": [entry["oil_production_bbl"] for entry in data if entry["well_id"] == well_id][-10:],
            "gas_production_trend": [entry["gas_production_mcf"] for entry in data if entry["well_id"] == well_id][-10:],
            "operating_expense_trend": [entry["operating_cost_usd"] for entry in data if entry["well_id"] == well_id][-10:],
            "downtime_trend": [entry["downtime_hours"] for entry in data if entry["well_id"] == well_id][-10:],
            "water_cut_trend": [entry["water_cut_percent"] for entry in data if entry["well_id"] == well_id][-10:]
        }

    processed_data = list(indexed_data.values())
    return processed_data
