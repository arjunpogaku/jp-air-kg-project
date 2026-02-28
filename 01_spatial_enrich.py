import re, time, sqlite3
import numpy as np
import pandas as pd
from tqdm import tqdm
from geopy.geocoders import Nominatim
from pathlib import Path
from config import STATION_INFO_CSV, STATION_EN_CSV

GEO_RE = re.compile(r"\(?\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)?")

def parse_geolocation(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return (None, None)
    m = GEO_RE.match(str(val).strip())
    if not m:
        return (None, None)
    a, b = float(m.group(1)), float(m.group(2))
    # assume (lon,lat) if first looks like longitude
    if abs(a) > 90 and abs(b) <= 90:
        lon, lat = a, b
    else:
        lat, lon = a, b
    return (lat, lon)

class GeoCache:
    def __init__(self, db_path="artifacts/geocache.sqlite"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS geocache (
            lat REAL, lon REAL,
            prefecture TEXT, city TEXT, street TEXT, pincode TEXT,
            PRIMARY KEY(lat, lon)
        )""")
        self.conn.commit()

    def get(self, lat, lon):
        cur = self.conn.execute(
            "SELECT prefecture, city, street, pincode FROM geocache WHERE lat=? AND lon=?",
            (float(lat), float(lon))
        )
        row = cur.fetchone()
        if row:
            return dict(prefecture=row[0], city=row[1], street=row[2], pincode=row[3])
        return None

    def put(self, lat, lon, prefecture, city, street, pincode):
        self.conn.execute(
            "INSERT OR REPLACE INTO geocache(lat, lon, prefecture, city, street, pincode) VALUES (?,?,?,?,?,?)",
            (float(lat), float(lon), prefecture, city, street, pincode)
        )
        self.conn.commit()

def main(sleep_s=1.1):
    st = pd.read_csv(STATION_INFO_CSV)
    st.columns = [c.strip().lower() for c in st.columns]

    if "geolocation" not in st.columns:
        raise ValueError("station_info must contain geolocation")

    latlon = st["geolocation"].map(parse_geolocation)
    st["lat"] = latlon.map(lambda x: x[0])
    st["lon"] = latlon.map(lambda x: x[1])

    geocoder = Nominatim(user_agent="jp-air-spatial-enrich", timeout=10)
    cache = GeoCache()

    pref, city, street, pin = [], [], [], []

    for lat, lon in tqdm(list(zip(st["lat"].values, st["lon"].values)), desc="Reverse geocoding stations"):
        if pd.isna(lat) or pd.isna(lon):
            pref.append(None); city.append(None); street.append(None); pin.append(None)
            continue

        cached = cache.get(lat, lon)
        if cached:
            pref.append(cached["prefecture"]); city.append(cached["city"])
            street.append(cached["street"]); pin.append(cached["pincode"])
            continue

        time.sleep(sleep_s)
        loc = geocoder.reverse((lat, lon), language="en", zoom=18, addressdetails=True)
        if not loc:
            cache.put(lat, lon, None, None, None, None)
            pref.append(None); city.append(None); street.append(None); pin.append(None)
            continue

        addr = loc.raw.get("address", {})
        prefecture = addr.get("state") or addr.get("province") or addr.get("region")
        cty = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality") or addr.get("county")
        rd = addr.get("road") or addr.get("neighbourhood") or addr.get("suburb")
        postcode = addr.get("postcode")

        cache.put(lat, lon, prefecture, cty, rd, postcode)
        pref.append(prefecture); city.append(cty); street.append(rd); pin.append(postcode)

    out = pd.DataFrame({
        "stationid": st["stationid"].astype(str),
        "stationname": st.get("stationname", pd.Series([None]*len(st))).astype(str),
        "prefecture_en": pref,
        "city_en": city,
        "street_en": street,
        "pincode": pin,
        "lat": st["lat"],
        "lon": st["lon"],
    })

    Path(STATION_EN_CSV).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(STATION_EN_CSV, index=False)
    print("Wrote:", STATION_EN_CSV, "rows=", len(out))

if __name__ == "__main__":
    main()