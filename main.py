from src import ingestion, data_cleaner, processing, analysis

if __name__ == "__main__":
    data = ingestion.structure_data('data/input/test_well_viability_dataset.csv')
    clean, failed_logic, failed_validation = data_cleaner.clean_data(data)
    processed = processing.process_data(clean)
    analyzed = analysis.main(processed)
    print(analyzed)
