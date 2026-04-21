"""
Data cleaning module - data_cleaner.py

- Performs sanity check through range validation
- Ensures logic consistency across all data fields
- Drops all records found with INVALID or ILLOGICAL data
"""

# validate all relevant values are within stipulated numerical ranges
def range_validation(data: list) -> tuple:
    """
    Valid ranges:

    oil_production_bbl >= 0
    gas_production_mcf >= 0
    operating_cost_usd >= 0
    oil_price_usd_per_bbl > 0
    gas_price_usd_per_mcf > 0
    0 <= downtime_hours <= 24
    0 <= water_cut_percent <= 100
    """

    def is_not_negative(val: float) -> bool:
        return val >= 0
    def is_greater_than_zero(val: float) -> bool:
        return val > 0
    def is_within_range(upper_limit: int, val: float) -> bool:
        return 0 <= val <= upper_limit

    range_validated_data = []
    failed_validation_data = []

    for record_index, record in enumerate(data):
        result_list = []
        for key, value in record.items():
            # only runs for columns with numerical values
            if type(value) == float:
                match key:
                    case "oil_production_bbl":
                        result = is_not_negative(value)
                        result_list.append(result)
                    case "gas_production_mcf":
                        result = is_not_negative(value)
                        result_list.append(result)
                    case "operating_cost_usd":
                        result = is_not_negative(value)
                        result_list.append(result)
                    case "oil_price_usd_per_bbl":
                        result = is_greater_than_zero(value)
                        result_list.append(result)
                    case "gas_price_usd_per_mcf":
                        result = is_greater_than_zero(value)
                        result_list.append(result)
                    case "downtime_hours":
                        result = is_within_range(24, value)
                        result_list.append(result)
                    case "water_cut_percent":
                        result = is_within_range(100, value)
                        result_list.append(result)

        if all(result_list):
            range_validated_data.append(record)
        else:
            print(f"Record {record['well_id']} on {record['date']} failed validation")
            failed_validation_data.append(record)

    return range_validated_data, failed_validation_data


# ensures inference from certain fields don't contradict one another in the same record
def logic_validation(data: list) -> tuple:
    """
    Logic conditions:

    If downtime = 24; oil and gas production = 0
    If water cut percentage = 100; oil = 0
    If operating cost USD = 0; gas, oil, water cut = 0 and downtime = 0
    """

    logic_validated_data = []
    failed_logic_test_data = []
    for record_index, record in enumerate(data):
        if record['downtime_hours'] == 24:
            if record['oil_production_bbl'] != 0 or record['gas_production_mcf'] != 0:
                print(f"RECORD {record['well_id']} on {record['date']} FAILED LOGIC TEST (downtime hours)...")
                failed_logic_test_data.append(record)
        elif record['water_cut_percent'] == 100:
            if record['oil_production_bbl'] != 0 or record['gas_production_mcf'] != 0:
                print(f"RECORD {record['well_id']} on {record['date']} FAILED LOGIC TEST (water_cut_percent)...")
                failed_logic_test_data.append(record)
        elif record['operating_cost_usd'] == 0:
            if record['oil_production_bbl'] != 0 or record['gas_production_mcf'] != 0 or record['downtime_hours'] != 24:
                print(f"RECORD {record['well_id']} on {record['date']} FAILED LOGIC TEST (operating_cost_usd)...")
                failed_logic_test_data.append(record)
        else:
            logic_validated_data.append(record)

    return logic_validated_data, failed_logic_test_data


# main data cleaning pipeline
def clean_data(data: list) -> tuple:
    range_validated_data, failed_validation_data = range_validation(data)
    cleaned_data, failed_logic_test_data = logic_validation(range_validated_data)

    return cleaned_data, failed_logic_test_data, failed_validation_data

