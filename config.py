from pathlib import Path

BASE = Path(__file__).resolve().parent

# INPUTS (edit these)
OBS_CSV = str(BASE / "datasets" / "hourly_observations.csv")
STATION_INFO_CSV = str(BASE / "datasets" / "station_info.csv")

# DERIVED / OUTPUTS
WORKDIR = str(BASE / "duckdb_tmp")
OBS_PARQUET = str(BASE / "artifacts" / "obs.parquet")
STATION_EN_CSV = str(BASE / "artifacts" / "station_en.csv")
HOLIDAYS_CSV = str(BASE / "artifacts" / "jp_holidays_2018_2025.csv")
FEATURED_PARQUET = str(BASE / "artifacts" / "featured.parquet")
FEATURE_TABLE_CSV = str(BASE / "artifacts" / "feature_table.csv")

# PERFORMANCE
THREADS = 32
MEMORY_LIMIT = "300GB"

# YEARS for holidays
START_YEAR = 2018
END_YEAR = 2025