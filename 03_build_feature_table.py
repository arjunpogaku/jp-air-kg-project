import duckdb
from pathlib import Path
from config import (
    OBS_PARQUET, STATION_EN_CSV, HOLIDAYS_CSV,
    FEATURED_PARQUET, FEATURE_TABLE_CSV,
    WORKDIR, THREADS, MEMORY_LIMIT
)

# Japan EQS thresholds for guideline-based 5-level labels
G_SO2 = 0.10      # ppm hourly
G_OX  = 0.06      # ppm hourly
G_SPM = 0.20      # mg/m3 hourly
G_CO8H = 20.0     # ppm 8-hour mean
G_PM25_DAILY = 35.0  # ug/m3 daily mean proxy

def ratio5_label_sql(x_expr: str, g: float) -> str:
    r = f"({x_expr} / {g})"
    return f"""
    CASE
      WHEN {x_expr} IS NULL THEN NULL
      WHEN {r} <= 1 THEN 'safe'
      WHEN {r} <= 2 THEN 'moderate'
      WHEN {r} <= 3 THEN 'slightly_unhealthy'
      WHEN {r} <= 4 THEN 'unhealthy'
      ELSE 'very_unhealthy'
    END
    """

def per_station_quantile_label_sql(x_col: str, q20: str, q40: str, q60: str, q80: str) -> str:
    return f"""
    CASE
      WHEN {x_col} IS NULL THEN NULL
      WHEN {q20} IS NULL THEN NULL
      WHEN {x_col} <= {q20} THEN 'safe'
      WHEN {x_col} <= {q40} THEN 'moderate'
      WHEN {x_col} <= {q60} THEN 'slightly_unhealthy'
      WHEN {x_col} <= {q80} THEN 'unhealthy'
      ELSE 'very_unhealthy'
    END
    """

def wind_dir_8_sql(wd_expr: str) -> str:
    # returns N/NE/E/SE/S/SW/W/NW or NULL
    return f"""
    CASE
      WHEN {wd_expr} IS NULL THEN NULL
      WHEN {wd_expr} < 0 THEN NULL
      WHEN {wd_expr} >= 337.5 OR {wd_expr} < 22.5 THEN 'N'
      WHEN {wd_expr} < 67.5 THEN 'NE'
      WHEN {wd_expr} < 112.5 THEN 'E'
      WHEN {wd_expr} < 157.5 THEN 'SE'
      WHEN {wd_expr} < 202.5 THEN 'S'
      WHEN {wd_expr} < 247.5 THEN 'SW'
      WHEN {wd_expr} < 292.5 THEN 'W'
      ELSE 'NW'
    END
    """

def main():
    Path(FEATURED_PARQUET).parent.mkdir(parents=True, exist_ok=True)
    Path(WORKDIR).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={THREADS};")
    con.execute(f"PRAGMA memory_limit='{MEMORY_LIMIT}';")
    con.execute(f"PRAGMA temp_directory='{WORKDIR}';")
    con.execute("PRAGMA enable_progress_bar=true;")

    # Load lookups
    con.execute(f"""
      CREATE OR REPLACE TABLE station_en AS
      SELECT CAST(stationid AS VARCHAR) AS stationid, * EXCLUDE(stationid)
      FROM read_csv_auto('{STATION_EN_CSV}', header=true);
    """)
    con.execute(f"""
      CREATE OR REPLACE TABLE jp_holidays AS
      SELECT * FROM read_csv_auto('{HOLIDAYS_CSV}', header=true);
    """)

    # Raw obs
    con.execute(f"""
      CREATE OR REPLACE VIEW obs AS
      SELECT
        CAST(stationid AS VARCHAR) AS stationid,
        CAST(obsdate AS TIMESTAMP) AS obsdate,
    
        CAST(so2  AS DOUBLE) AS so2,
        CAST(no   AS DOUBLE) AS no,
        CAST(no2  AS DOUBLE) AS no2,
        CAST(nox  AS DOUBLE) AS nox,
        CAST(co   AS DOUBLE) AS co,
        CAST(ox   AS DOUBLE) AS ox,
        CAST(nmhc AS DOUBLE) AS nmhc,
        CAST(ch4  AS DOUBLE) AS ch4,
        CAST(thc  AS DOUBLE) AS thc,
        CAST(spm  AS DOUBLE) AS spm,
        CAST(pm25 AS DOUBLE) AS pm25,
    
        CAST(sp   AS DOUBLE) AS sp,
        CAST(wd   AS DOUBLE) AS wd,
        CAST(ws   AS DOUBLE) AS ws,
        CAST(temp AS DOUBLE) AS temp,
        CAST(hum  AS DOUBLE) AS hum
    
      FROM read_parquet('{OBS_PARQUET}');
    """)

    # Build engineered base (spatial + temporal + rolling + daily means)
    con.execute(f"""
    CREATE OR REPLACE TABLE feat AS
    WITH base AS (
      SELECT
        o.*,

        -- spatial
        s.stationname,
        s.prefecture_en, s.city_en, s.street_en, s.pincode, s.lat, s.lon,

        -- time
        CAST(o.obsdate AS DATE) AS date,
        EXTRACT(year FROM o.obsdate)::INT AS year,
        EXTRACT(hour FROM o.obsdate)::INT AS hour,
        strftime(o.obsdate, '%B') AS month_name,
        strftime(o.obsdate, '%A') AS weekday_name,
        EXTRACT(dow FROM o.obsdate)::INT AS dow,  -- 0=Sunday

        CASE WHEN EXTRACT(dow FROM o.obsdate) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN h.date IS NOT NULL THEN TRUE ELSE FALSE END AS is_holiday,

        CASE
          WHEN EXTRACT(hour FROM o.obsdate) BETWEEN 0 AND 4 THEN 'late_night'
          WHEN EXTRACT(hour FROM o.obsdate) BETWEEN 5 AND 8 THEN 'morning'
          WHEN EXTRACT(hour FROM o.obsdate) BETWEEN 9 AND 11 THEN 'late_morning'
          WHEN EXTRACT(hour FROM o.obsdate) BETWEEN 12 AND 16 THEN 'afternoon'
          WHEN EXTRACT(hour FROM o.obsdate) BETWEEN 17 AND 20 THEN 'evening'
          ELSE 'night'
        END AS time_of_day,

        CASE
          WHEN EXTRACT(hour FROM o.obsdate) BETWEEN 7 AND 9 THEN TRUE
          WHEN EXTRACT(hour FROM o.obsdate) BETWEEN 17 AND 19 THEN TRUE
          ELSE FALSE
        END AS is_peak_hour

      FROM obs o
      LEFT JOIN station_en s ON o.stationid = s.stationid
      LEFT JOIN jp_holidays h ON CAST(o.obsdate AS DATE) = CAST(h.date AS DATE)
    ),
    roll AS (
      SELECT
        *,

        -- true time windows (handles missing hours)
        AVG(co)  OVER w8  AS co_roll8h

      FROM base
      WINDOW
        w8 AS (
          PARTITION BY stationid
          ORDER BY obsdate
          RANGE BETWEEN INTERVAL 8 HOURS PRECEDING AND CURRENT ROW
        )
    ),
    agg AS (
      SELECT
        *,

        AVG(pm25) OVER (PARTITION BY stationid, date) AS pm25_daily_mean,
        AVG(no2)  OVER (PARTITION BY stationid, date) AS no2_daily_mean

      FROM roll
    )
    SELECT * FROM agg;
    """)

    # Per-station quantiles for data-driven pollutants + meteorology
    con.execute("""
    CREATE OR REPLACE TABLE station_q AS
    SELECT
      stationid,

      -- pollutants (per station)
      quantile_cont(no,   0.20) AS no_q20,   quantile_cont(no,   0.40) AS no_q40,
      quantile_cont(no,   0.60) AS no_q60,   quantile_cont(no,   0.80) AS no_q80,

      quantile_cont(nox,  0.20) AS nox_q20,  quantile_cont(nox,  0.40) AS nox_q40,
      quantile_cont(nox,  0.60) AS nox_q60,  quantile_cont(nox,  0.80) AS nox_q80,

      quantile_cont(nmhc, 0.20) AS nmhc_q20, quantile_cont(nmhc, 0.40) AS nmhc_q40,
      quantile_cont(nmhc, 0.60) AS nmhc_q60, quantile_cont(nmhc, 0.80) AS nmhc_q80,

      quantile_cont(ch4,  0.20) AS ch4_q20,  quantile_cont(ch4,  0.40) AS ch4_q40,
      quantile_cont(ch4,  0.60) AS ch4_q60,  quantile_cont(ch4,  0.80) AS ch4_q80,

      quantile_cont(thc,  0.20) AS thc_q20,  quantile_cont(thc,  0.40) AS thc_q40,
      quantile_cont(thc,  0.60) AS thc_q60,  quantile_cont(thc,  0.80) AS thc_q80,

      -- meteorology (per station)
      quantile_cont(sp,   0.20) AS sp_q20,   quantile_cont(sp,   0.40) AS sp_q40,
      quantile_cont(sp,   0.60) AS sp_q60,   quantile_cont(sp,   0.80) AS sp_q80,

      quantile_cont(ws,   0.20) AS ws_q20,   quantile_cont(ws,   0.40) AS ws_q40,
      quantile_cont(ws,   0.60) AS ws_q60,   quantile_cont(ws,   0.80) AS ws_q80,

      quantile_cont(temp, 0.20) AS temp_q20, quantile_cont(temp, 0.40) AS temp_q40,
      quantile_cont(temp, 0.60) AS temp_q60, quantile_cont(temp, 0.80) AS temp_q80,

      quantile_cont(hum,  0.20) AS hum_q20,  quantile_cont(hum,  0.40) AS hum_q40,
      quantile_cont(hum,  0.60) AS hum_q60,  quantile_cont(hum,  0.80) AS hum_q80

    FROM feat
    GROUP BY stationid;
    """)

    # Build labels
    so2_label = ratio5_label_sql("f.so2", G_SO2)
    ox_label  = ratio5_label_sql("f.ox",  G_OX)
    spm_label = ratio5_label_sql("f.spm", G_SPM)
    co_label  = ratio5_label_sql("f.co_roll8h", G_CO8H)
    pm25_label = ratio5_label_sql("f.pm25_daily_mean", G_PM25_DAILY)

    no2_label = """
    CASE
      WHEN f.no2_daily_mean IS NULL THEN NULL
      WHEN f.no2_daily_mean <= 0.04 THEN 'safe'
      WHEN f.no2_daily_mean <= 0.06 THEN 'moderate'
      WHEN f.no2_daily_mean <= 0.12 THEN 'slightly_unhealthy'
      WHEN f.no2_daily_mean <= 0.18 THEN 'unhealthy'
      ELSE 'very_unhealthy'
    END
    """

    no_label   = per_station_quantile_label_sql("f.no",   "q.no_q20",   "q.no_q40",   "q.no_q60",   "q.no_q80")
    nox_label  = per_station_quantile_label_sql("f.nox",  "q.nox_q20",  "q.nox_q40",  "q.nox_q60",  "q.nox_q80")
    nmhc_label = per_station_quantile_label_sql("f.nmhc", "q.nmhc_q20", "q.nmhc_q40", "q.nmhc_q60", "q.nmhc_q80")
    ch4_label  = per_station_quantile_label_sql("f.ch4",  "q.ch4_q20",  "q.ch4_q40",  "q.ch4_q60",  "q.ch4_q80")
    thc_label  = per_station_quantile_label_sql("f.thc",  "q.thc_q20",  "q.thc_q40",  "q.thc_q60",  "q.thc_q80")

    sp_label   = per_station_quantile_label_sql("f.sp",   "q.sp_q20",   "q.sp_q40",   "q.sp_q60",   "q.sp_q80")
    ws_label   = per_station_quantile_label_sql("f.ws",   "q.ws_q20",   "q.ws_q40",   "q.ws_q60",   "q.ws_q80")
    temp_label = per_station_quantile_label_sql("f.temp", "q.temp_q20", "q.temp_q40", "q.temp_q60", "q.temp_q80")
    hum_label  = per_station_quantile_label_sql("f.hum",  "q.hum_q20",  "q.hum_q40",  "q.hum_q60",  "q.hum_q80")

    wd_dir = wind_dir_8_sql("f.wd")

    # Write FEATURED_PARQUET (wide engineered, reusable)
    con.execute(f"""
    COPY (
      SELECT
        f.*,
        -- derived string types for table
        'T' || strftime(f.obsdate, '%Y%m%d%H') AS tid,
        'S' || f.stationid AS sid,
        CASE WHEN f.is_weekend THEN 'weekend' ELSE 'weekDay' END AS dayType,
        CASE WHEN f.is_holiday THEN 'holiday' ELSE 'workday' END AS workType,
        CASE WHEN f.is_peak_hour THEN 'peakHour' ELSE 'nonPeakHour' END AS tHT,

        -- wind direction category
        {wd_dir} AS wd_dir,

        -- labels: all pollutants
        {pm25_label} AS pm25_label,
        {so2_label}  AS so2_label,
        {no_label}   AS no_label,
        {no2_label}  AS no2_label,
        {nox_label}  AS nox_label,
        {co_label}   AS co_label,
        {ox_label}   AS ox_label,
        {nmhc_label} AS nmhc_label,
        {ch4_label}  AS ch4_label,
        {thc_label}  AS thc_label,
        {spm_label}  AS spm_label,

        -- labels: meteorology
        {sp_label}   AS sp_label,
        {ws_label}   AS ws_label,
        {temp_label} AS temp_label,
        {hum_label}  AS hum_label

      FROM feat f
      LEFT JOIN station_q q ON f.stationid = q.stationid
    ) TO '{FEATURED_PARQUET}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Write FEATURE_TABLE_CSV (like your screenshot)
    con.execute(f"""
    COPY (
      SELECT
        tid AS TID,
        sid AS SID,
        prefecture_en AS Prefecture,
        city_en AS City,
        street_en AS Street,
        pincode AS Pincode,

        hour AS Hour,
        year AS Year,
        month_name AS Month,
        weekday_name AS Day,
        time_of_day AS Time,
        dayType,
        workType,
        tHT,

        -- pollutants
        pm25 AS "PM2.5",
        so2  AS SO2,
        no   AS NO,
        no2  AS NO2,
        nox  AS NOx,
        co   AS CO,
        ox   AS Ox,
        nmhc AS NMHC,
        ch4  AS CH4,
        thc  AS THC,
        spm  AS SPM,

        -- meteorology
        sp   AS SP,
        wd   AS WD,
        wd_dir AS WD_dir,
        ws   AS WS,
        temp AS TEMP,
        hum  AS HUM,

        -- labels (all)
        pm25_label AS "PM2.5_label",
        so2_label  AS "SO2_label",
        no_label   AS "NO_label",
        no2_label  AS "NO2_label",
        nox_label  AS "NOx_label",
        co_label   AS "CO_label",
        ox_label   AS "Ox_label",
        nmhc_label AS "NMHC_label",
        ch4_label  AS "CH4_label",
        thc_label  AS "THC_label",
        spm_label  AS "SPM_label",

        sp_label   AS "SP_label",
        ws_label   AS "WS_label",
        temp_label AS "TEMP_label",
        hum_label  AS "HUM_label"

      FROM read_parquet('{FEATURED_PARQUET}')
    ) TO '{FEATURE_TABLE_CSV}' (HEADER, DELIMITER ',');
    """)

    con.close()
    print("Wrote:", FEATURED_PARQUET)
    print("Wrote:", FEATURE_TABLE_CSV)

if __name__ == "__main__":
    main()