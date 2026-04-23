🛢️ Well Viability Analyzer (CLI)

A modular Python command-line tool for analyzing oil & gas well data and determining operational viability through structured data processing, validation, and analysis.

📌 Overview

The Well Viability Analyzer ingests raw well production data, processes and cleans it, performs analytical evaluations, and exports structured results.

✅ Inputs
1 CSV file containing raw well data
📤 Outputs (2 files)
analysis_results.csv → Computed metrics per well
removed_records.csv → Invalid or dropped records during cleaning
🧠 System Architecture

The system is built with five core modules, each handling a specific stage in the data pipeline:

1. Ingestion Module
Reads input CSV file
Parses raw data into Python data structures
Handles file-related errors
2. Processing Module
Organizes and structures ingested data
Groups records by well
Prepares data for analysis (e.g., sorting by date, normalizing formats)
3. Data Cleaning Module (data_cleaner.py)
Performs:
Range validation (e.g., no negative production values)
Logical consistency checks
Removes invalid records
Outputs:
Clean dataset
Removed/failed records
4. Analysis Module
Computes:
Average production (oil & gas)
Profitability metrics
Trend analysis (increasing / decreasing / stable)
Produces structured results per well (dictionary-based internally)
5. Export Module
Writes outputs to CSV files:
analysis_results.csv
removed_records.csv
📁 Project Structure
well-viability-analyzer/
│
├── main.py
├── input/
├── output/
│
├── src/
│   ├── ingestion.py
│   ├── processing.py
│   ├── data_cleaner.py
│   ├── analyzer.py
│   ├── exporter.py
│
├── tests/
├── requirements.txt
└── README.md
🚀 Usage (CLI)

This program is run entirely from the command line.

🔹 Command Format
python main.py <input_csv_path> <output_directory>
🔹 Arguments
Argument	Description
input_csv_path	Path to the input CSV file
output_directory	Directory where output files will be saved
🔹 Example
python main.py data/wells.csv output/
📤 Output Details

After execution, the specified output directory will contain:

📊 analysis_results.csv

Contains analyzed well-level metrics.

Typical fields may include:

well_id
avg_oil_production_bbl
avg_gas_production_mcf
profitability_class
production_trend
viability_status
❌ removed_records.csv

Contains records excluded during cleaning.

Reasons for removal include:

Invalid numeric values
Missing required fields
Logical inconsistencies
📄 Input CSV Format

Expected structure:

well_id,date,oil_production_bbl,gas_production_mcf,operating_cost,oil_price,gas_price
W001,2024-01-01,120,300,5000,75,3
W001,2024-01-02,130,320,5000,75,3
...
⚙️ Installation
git clone https://github.com/your-username/well-viability-analyzer.git
cd well-viability-analyzer

python -m venv venv
venv\Scripts\activate   # Windows
source venv/bin/activate # Mac/Linux

pip install -r requirements.txt
🔄 Execution Flow
CSV Input
   ↓
Ingestion
   ↓
Processing
   ↓
Data Cleaning
   ↓
Analysis
   ↓
Export
   ↓
CSV Outputs
⚠️ Assumptions & Constraints
Input data must be structured and properly formatted
Output directory must exist prior to execution
All numeric fields must follow realistic physical constraints
🧪 Testing
pytest
🔧 Future Improvements
Add decision module (separate viability reasoning layer)
Visualization support (charts, dashboards)
Machine learning integration for predictive analysis
API or web interface (Flask/FastAPI)
🤝 Contributing
Follow modular design principles
Keep functions small and focused
Add tests for new features
Avoid committing generated CSV files
📜 License

MIT License

💡 Notes

This project emphasizes:

Clean pipeline design
Engineering-grade data validation
Scalable modular architecture
