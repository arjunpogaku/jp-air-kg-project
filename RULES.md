# Japan Air Pollution Feature Engineering Rules (2018–2025)

## Inputs
### A) Hourly observation file (CSV)
Columns (original):
- stationid (string-like; keep as TEXT to preserve leading zeros)
- obsdate (timestamp; hourly)
- Pollutants (11):
  so2, no, no2, nox, co, ox, nmhc, ch4, thc, spm, pm25
- Meteorology (5):
  sp (pressure), wd (wind direction), ws (wind speed), temp (temperature), hum (humidity)

Units (assumed):
- so2, no, no2, nox, co, ox, nmhc, ch4, thc are in ppm (as provided)
- spm is in mg/m³
- pm25 is in µg/m³
- wd may include -1 meaning missing/invalid direction

### B) Station info file (CSV)
- stationid, stationname, address, geolocation
- geolocation stored like "(lon,lat)" typically

## Output
A "feature table" with:
- IDs:
  - TID = "T" + YYYYMMDDHH (e.g., T2018010922)
  - SID = "S" + stationid (e.g., S01101010)
- Spatial (English):
  prefecture, city, street, pincode, lat, lon
- Temporal:
  hour, year, month_name, weekday_name, time_of_day,
  dayType (weekDay/weekend),
  workType (workday/holiday),
  tHT (peakHour/nonPeakHour)
- Measurements:
  All 11 pollutants + 5 meteorology variables
- Labels:
  A label for EACH pollutant and EACH meteorology variable

## Temporal rules
- hour/year extracted from obsdate
- month_name: January...December
- weekday_name: Monday...Sunday
- time_of_day buckets (by hour):
  00–04 late_night
  05–08 morning
  09–11 late_morning
  12–16 afternoon
  17–20 evening
  21–23 night
- dayType:
  weekend if Saturday or Sunday else weekDay
- workType:
  holiday if obsdate date is in Japanese public holidays else workday
- tHT (traffic peak proxy):
  peakHour if hour in [7..9] or [17..19] else nonPeakHour

## Labeling rules (5 levels)
Labels:
- safe
- moderate
- slightly_unhealthy
- unhealthy
- very_unhealthy

### A) Guideline-based pollutants (ratio-to-threshold)
For pollutant value x and guideline threshold G:
r = x / G
- safe: r <= 1
- moderate: 1 < r <= 2
- slightly_unhealthy: 2 < r <= 3
- unhealthy: 3 < r <= 4
- very_unhealthy: r > 4

We use:
- SO2 (ppm): x = so2 (hourly), G = 0.10
- Ox (ppm):  x = ox (hourly),  G = 0.06
- SPM (mg/m³): x = spm (hourly), G = 0.20
- CO (ppm): x = 8-hour rolling mean co_8h, G = 20.0
- PM2.5 (µg/m³): x = daily mean pm25_daily, G = 35.0

### B) NO2 forced to 5 levels using daily mean (Japan "zone" extended)
Let x = daily mean of hourly no2 (ppm):
- safe: x <= 0.04
- moderate: 0.04 < x <= 0.06
- slightly_unhealthy: 0.06 < x <= 0.12  (2x 0.06)
- unhealthy: 0.12 < x <= 0.18          (3x 0.06)
- very_unhealthy: x > 0.18

### C) Per-station percentile labels (data-driven)
For each station and variable, compute quantiles q20,q40,q60,q80 over the full time range.
Apply to:
- Pollutants: no, nox, nmhc, ch4, thc
- Meteorology: sp, ws, temp, hum
- (wd handled separately as direction category)

Label:
- safe: x <= q20
- moderate: q20 < x <= q40
- slightly_unhealthy: q40 < x <= q60
- unhealthy: q60 < x <= q80
- very_unhealthy: x > q80

### D) Wind direction (wd) classification
wd is circular.
If wd is NULL or wd < 0 => wd_dir = NULL.
Else map degrees to 8-compass:
N, NE, E, SE, S, SW, W, NW
Using boundaries at 22.5 degrees increments.

Optional: you may also assign wd "intensity label" using station percentiles of wd variance,
but by default only compass category is produced.

## Spatial enrichment rules
- Use station_info geolocation (lat/lon) and reverse geocode into English:
  prefecture (state), city, street/road, pincode (postcode)
- Cache results to avoid repeated calls.
- Join spatial fields into observation records by stationid.

## Performance rules
- Treat stationid as VARCHAR everywhere (do NOT cast to INT).
- Convert the big observation CSV to Parquet once.
- Use DuckDB for feature creation (multi-threaded, disk-backed).