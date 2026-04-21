"""
Data analysis module - analysis.py

- main analysis function takes both cleaned data and processed data
- checks for trends and derives insights
- also provides metrics for the entire field
"""

"""

- Profitability:
    average daily profits
    check's daily profits trend
    profit margin percentage
    measured as relatively low, average or high compared to other wells
    also measured relative to a standard benchmark
- Volume of production: (oil_bbl, gas_mcf)
    average daily production
    checks daily production trend
    measured as relatively low, average or high compared to other wells
    also measured relative to a standard benchmark
- Cost discipline: (opex)
    average daily expenses
    checks expense trend if rising or falling or random
- Reliability: (downtime_percent)
    measured from downtime percent given
    average downtime
    also checks for rising downtime (trends)
- Reservoir quality: (water_cut_percent)
    average water_cut
    measured from water_cut percent given
    checks for rising water_cut(trends)

"""

def analyze_trend(data: list) -> dict:
    def find_slope(xarr: list, st_id: int, end_id: int) -> int:
        slope = (xarr[end_id] - xarr[st_id])/(end_id-st_id)
        return slope

    trend_obj = {}

    rng = data[-1]-data[0]
    if rng < 0:
        ovr_trend = "decreasing"
    elif rng > 0:
        ovr_trend = "increasing"
    else:
        ovr_trend = "none"

    trend_obj["overall_trend"] = ovr_trend

    slopes = []
    for index, a in enumerate(data):
        id_1 = index
        id_2 = index+2

        if id_2 > len(data)-1:
            break
        else:
            slopes.append(find_slope(data, id_1, id_2))
            trend_obj[f"slope_{index+1}"] = find_slope(data, id_1, id_2)

    signs = []
    for slope in slopes:
        if slope > 0:
            signs.append(True)
        elif slope < 0:
            signs.append(False)

    n_pos = 0
    n_neg = 0
    for sign in signs:
        if sign:
            n_pos += 1
        else:
            n_neg += 1

    if n_pos >= (0.6 * (len(data)-2)):
        trend_obj["notes"] = "Increasing Steadily"
    elif n_neg >= (0.6 * (len(data)-2)):
        trend_obj["notes"] = "Decreasing Steadily"
    else:
        trend_obj["notes"] = "Unsteady Trend"

    return trend_obj

def profitability_analysis(data: dict) -> dict:
    profitability_obj = {}
    avg_daily_profit = round(data["well_profit"] / data["recorded_days"], 2)
    total_profit = data["well_profit"]
    profit_margin = (data["well_profit"] / data["total_well_revenue"]) * 100
    profit_trend = analyze_trend(data["trends"]["profits_trend"])
    profitability_obj["total_profit"] = total_profit
    profitability_obj["average_daily_profits"] = avg_daily_profit
    profitability_obj["profit_margin"] = profit_margin
    profitability_obj["profit_trend"] = profit_trend

    return profitability_obj

def production_analysis(data: dict) -> dict:
    production_obj = {
        "oil_production": {},
        "gas_production": {}
    }
    avg_oil_daily = round(data["total_oil_produced_bbl"] / data["recorded_days"], 2)
    avg_gas_daily = round(data["total_gas_produced_mcf"] / data["recorded_days"], 2)
    oil_production_trend = analyze_trend(data["trends"]["oil_production_trend"])
    gas_production_trend = analyze_trend(data["trends"]["gas_production_trend"])
    production_obj["oil_production"]["average_oil_production_daily"] =  avg_oil_daily
    production_obj["oil_production"]["oil_production_trend"] = oil_production_trend
    production_obj["gas_production"]["average_gas_production_daily"] = avg_gas_daily
    production_obj["gas_production"]["gas_production_trend"] = gas_production_trend

    return production_obj

def cost_analysis(data: dict) -> dict:
    cost_analysis_obj = {}
    avg_daily_cost = (data["total_operating_cost"] / data["recorded_days"], 2)
    cost_trend = analyze_trend(data["trends"]["operating_expense_trend"])
    cost_analysis_obj["average_daily_cost"] = avg_daily_cost
    cost_analysis_obj["cost_trend"] = cost_trend

    return cost_analysis_obj

def reliability_analysis(data: dict) -> dict:
    reliability_obj = {}
    avg_daily_downtime = data["downtime_percent"]
    downtime_trend = analyze_trend(data["trends"]["downtime_trend"])
    reliability_obj["average_daily_downtime"] = avg_daily_downtime
    reliability_obj["downtime_trend"] = downtime_trend

    return reliability_obj

def quality_analysis(data: dict) -> dict:
    quality_dict = {}
    avg_water_cut = data["average_water_cut"]
    water_cut_trend = analyze_trend(data["trends"]["water_cut_trend"])
    quality_dict["average_water_cut"] = avg_water_cut
    quality_dict["water_cut_trend"] = water_cut_trend

    return quality_dict

def main(processed_data: list) -> dict:
    analyzed = {data["well_id"]: {"well_id": data["well_id"]} for data in processed_data}

    for data in processed_data:
        profitability = profitability_analysis(data)
        analyzed[data["well_id"]]["profitability"] = profitability
        production = production_analysis(data)
        analyzed[data["well_id"]]["production"] = production
        cost = cost_analysis(data)
        analyzed[data["well_id"]]["cost"] = cost
        reliability = reliability_analysis(data)
        analyzed[data["well_id"]]["reliability"] = reliability
        quality = quality_analysis(data)
        analyzed[data["well_id"]]["quality"] = quality

    return analyzed
