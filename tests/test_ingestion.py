import os
import unittest
from datetime import datetime
import csv
import shutil
from pathlib import Path

# Import your module
from src import ingestion


TEST_DIR = Path("test_ingestion_data")


class TestIngestion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        TEST_DIR.mkdir(exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        if TEST_DIR.exists():
            shutil.rmtree(TEST_DIR)

    def setUp(self):
        self.test_files = []

    def tearDown(self):
        for path in self.test_files:
            if os.path.exists(path):
                os.remove(path)

    def create_csv(self, name, rows):
        path = TEST_DIR / name
        path = str(path)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        self.test_files.append(path)
        return path

    def test_nonexistent_file_path(self):
        with self.assertRaises(FileNotFoundError):
            ingestion.structure_data("this/path/does/not/exist.csv")

    def test_non_csv_file(self):
        # Create a “non‑CSV” file.
        bad_path = TEST_DIR / "data.txt"
        bad_path.write_text("this is not a csv")
        self.test_files.append(str(bad_path))

        with self.assertRaises(ValueError):
            ingestion.structure_data(str(bad_path))

    def test_non_readable_csv(self):
        # On some systems you’d test permission denied, but here we’ll simulate by trying to read a very short file
        # that cannot be parsed as CSV properly.
        bad_csv_path = TEST_DIR / "unparsable.csv"
        with open(bad_csv_path, "w", newline="") as f:
            f.write("unclosed,quote\n")
        self.test_files.append(str(bad_csv_path))

        with self.assertRaises(OSError):  # or whatever you decide to raise
            ingestion.structure_data(str(bad_csv_path))

    def test_empty_csv_file(self):
        path = self.create_csv("empty.csv", [])
        # ingestion.validate_file should pass existence and extension, but
        # ingestion.validate_schema should fail if header is required.
        # If your code expects header, empty CSV is invalid.
        with self.assertRaises(ValueError):
            ingestion.structure_data(path)

    def test_incomplete_columns_csv(self):
        # Missing some required columns
        header = ["well_id", "date", "oil_production_bbl", "gas_production_mcf"]
        rows = [
            header,
            ["W-001", "2025-01-01", "100.0", "500.0"],
        ]
        path = self.create_csv("incomplete_cols.csv", rows)

        with self.assertRaises(ValueError):
            ingestion.structure_data(path)

    def test_only_columns_csv(self):
        # Only the header, no data rows
        required = [
            "well_id", "date", "oil_production_bbl",
            "gas_production_mcf", "operating_cost_usd",
            "oil_price_usd_per_bbl", "gas_price_usd_per_mcf",
            "downtime_hours", "water_cut_percent",
        ]
        path = self.create_csv("only_header.csv", [required])
        # Your `structure_data` should handle this:
        #  - validate_file → OK
        #  - validate_schema → OK
        #  - then return [] (no rows)
        data = ingestion.structure_data(path)
        self.assertEqual(data, [])  # no parsed rows

    def test_all_proper_values(self):
        header = [
            "well_id", "date", "oil_production_bbl",
            "gas_production_mcf", "operating_cost_usd",
            "oil_price_usd_per_bbl", "gas_price_usd_per_mcf",
            "downtime_hours", "water_cut_percent",
        ]
        rows = [
            header,
            ["W-001", "2025-01-01", "100.0", "500.0", "1000.0", "80.0", "3.0", "0.0", "15.0"],
            ["W-002", "2025-01-01", "120.0", "450.0", "1100.0", "80.0", "3.0", "2.0", "18.0"],
        ]
        path = self.create_csv("good_data.csv", rows)
        data = ingestion.structure_data(path)

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["well_id"], "W-001")
        self.assertEqual(data[0]["date"], datetime(2025, 1, 1))
        self.assertEqual(data[0]["oil_production_bbl"], 100.0)
        self.assertEqual(data[0]["gas_production_mcf"], 500.0)
        self.assertEqual(data[0]["operating_cost_usd"], 1000.0)
        self.assertEqual(data[0]["oil_price_usd_per_bbl"], 80.0)
        self.assertEqual(data[0]["gas_price_usd_per_mcf"], 3.0)
        self.assertEqual(data[0]["downtime_hours"], 0.0)
        self.assertEqual(data[0]["water_cut_percent"], 15.0)

    def test_empty_rows(self):
        header = [
            "well_id", "date", "oil_production_bbl",
            "gas_production_mcf", "operating_cost_usd",
            "oil_price_usd_per_bbl", "gas_price_usd_per_mcf",
            "downtime_hours", "water_cut_percent",
        ]
        rows = [
            header,
            ["W-001", "2025-01-01", "100.0", "500.0", "1000.0", "80.0", "3.0", "0.0", "15.0"],
            # empty row
            ["", "", "", "", "", "", "", "", ""],
            ["W-002", "2025-01-01", "120.0", "450.0", "1100.0", "80.0", "3.0", "2.0", "18.0"],
        ]
        path = self.create_csv("with_empty_rows.csv", rows)
        data = ingestion.structure_data(path)

        # Only 2 rows should be valid (index 1 and 3 in CSV)
        self.assertEqual(len(data), 2)
        self.assertEqual({d["well_id"] for d in data}, {"W-001", "W-002"})

    def test_empty_cells(self):
        header = [
            "well_id", "date", "oil_production_bbl",
            "gas_production_mcf", "operating_cost_usd",
            "oil_price_usd_per_bbl", "gas_price_usd_per_mcf",
            "downtime_hours", "water_cut_percent",
        ]
        rows = [
            header,
            ["W-001", "2025-01-01", "100.0", "500.0", "1000.0", "80.0", "3.0", "0.0", "15.0"],
            ["W-002", "2025-01-01", "", "450.0", "1100.0", "80.0", "3.0", "2.0", "18.0"],  # missing oil_production_bbl
        ]
        path = self.create_csv("with_empty_cells.csv", rows)
        data = ingestion.structure_data(path)

        # Only 1 row should be valid (first one)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["well_id"], "W-001")

    def test_wrong_datatypes(self):
        header = [
            "well_id", "date", "oil_production_bbl",
            "gas_production_mcf", "operating_cost_usd",
            "oil_price_usd_per_bbl", "gas_price_usd_per_mcf",
            "downtime_hours", "water_cut_percent",
        ]
        rows = [
            header,
            ["W-001", "2025-01-01", "100.0", "500.0", "1000.0", "80.0", "3.0", "0.0", "15.0"],
            ["W-002", "invalid_date", "120.0", "450.0", "1100.0", "80.0", "3.0", "2.0", "18.0"],  # wrong date
            ["W-003", "2025-01-01", "not_a_number", "450.0", "1100.0", "80.0", "3.0", "2.0", "18.0"],  # wrong oil_prod
        ]
        path = self.create_csv("wrong_datatypes.csv", rows)
        data = ingestion.structure_data(path)

        # Only first row should be valid; others have type conversion errors
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["well_id"], "W-001")

    def test_date_wrong_separator(self):
        # Only allowed: -, /, \ (you normalize these to -)
        header = [
            "well_id", "date", "oil_production_bbl",
            "gas_production_mcf", "operating_cost_usd",
            "oil_price_usd_per_bbl", "gas_price_usd_per_mcf",
            "downtime_hours", "water_cut_percent",
        ]
        rows = [
            header,
            ["W-001", "2025-01-01", "100.0", "500.0", "1000.0", "80.0", "3.0", "0.0", "15.0"],
            ["W-002", "2025/01/01", "120.0", "450.0", "1100.0", "80.0", "3.0", "2.0", "18.0"],
            ["W-003", "2025\\01\\01", "130.0", "500.0", "1200.0", "80.0", "3.0", "0.0", "20.0"],
            ["W-004", "2025.01.01", "140.0", "550.0", "1300.0", "80.0", "3.0", "0.0", "22.0"],  # this should fail if you don't allow "."
        ]
        path = self.create_csv("date_separators.csv", rows)
        data = ingestion.structure_data(path)

        # Only rows with allowed separators should be valid
        # Depends on how you normalize in ingestion.py:
        # - if you normalize -, /, \ → valid
        # - if you don’t normalize "." → W‑004 should fail
        # If your code only allows -, /, \ via normalization, then:
        #   - W‑001, W‑002, W‑003 should be valid
        #   - W‑004 should fail (type error on date)
        valid_ids = {d["well_id"] for d in data}
        self.assertSetEqual(valid_ids, {"W-001", "W-002", "W-003"})

    def test_duplicate_data(self):
        header = [
            "well_id", "date", "oil_production_bbl",
            "gas_production_mcf", "operating_cost_usd",
            "oil_price_usd_per_bbl", "gas_price_usd_per_mcf",
            "downtime_hours", "water_cut_percent",
        ]
        rows = [
            header,
            ["W-001", "2025-01-01", "100.0", "500.0", "1000.0", "80.0", "3.0", "0.0", "15.0"],
            ["W-001", "2025-01-01", "100.0", "500.0", "1000.0", "80.0", "3.0", "0.0", "15.0"],  # duplicate
            ["W-002", "2025-01-01", "120.0", "450.0", "1100.0", "80.0", "3.0", "2.0", "18.0"],
        ]
        path = self.create_csv("duplicate_data.csv", rows)
        data = ingestion.structure_data(path)

        # Only 2 rows should be valid (first W‑001 and W‑002; second W‑001 is duplicate)
        self.assertEqual(len(data), 2)
        self.assertEqual({d["well_id"] for d in data}, {"W-001", "W-002"})


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
