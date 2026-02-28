import pandas as pd
import holidays
from config import HOLIDAYS_CSV, START_YEAR, END_YEAR
from pathlib import Path

def main():
    Path(HOLIDAYS_CSV).parent.mkdir(parents=True, exist_ok=True)
    jp = holidays.Japan(years=range(START_YEAR, END_YEAR + 1))
    rows = [(d.isoformat(), name) for d, name in jp.items()]
    df = pd.DataFrame(rows, columns=["date", "holiday_name"]).drop_duplicates().sort_values("date")
    df.to_csv(HOLIDAYS_CSV, index=False)
    print("Wrote:", HOLIDAYS_CSV, "rows=", len(df))

if __name__ == "__main__":
    main()