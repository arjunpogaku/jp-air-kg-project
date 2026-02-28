import duckdb
from pathlib import Path
from config import OBS_CSV, OBS_PARQUET, WORKDIR, THREADS, MEMORY_LIMIT

def main():
    Path(OBS_PARQUET).parent.mkdir(parents=True, exist_ok=True)
    Path(WORKDIR).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={THREADS};")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}';")
    con.execute(f"PRAGMA temp_directory='{WORKDIR}';")
    con.execute("PRAGMA enable_progress_bar=true;")

    con.execute(f"""
    COPY (
      SELECT
        CAST(stationid AS VARCHAR) AS stationid,
        CAST(obsdate AS TIMESTAMP) AS obsdate,
        so2, no, no2, nox, co, ox, nmhc, ch4, thc, spm, pm25,
        sp, wd, ws, temp, hum
      FROM read_csv_auto('{OBS_CSV}', header=true)
    ) TO '{OBS_PARQUET}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    con.close()
    print("Wrote:", OBS_PARQUET)

if __name__ == "__main__":
    main()