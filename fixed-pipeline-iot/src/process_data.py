import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER', 'iot_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'iot_password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'iot_db')

CONN_STR = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SCHEMA_SQL_PATH = os.path.join(os.path.dirname(__file__), "..", "sql", "schema.sql")
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "IOT-temp.csv")

def create_schema():
    engine = create_engine(CONN_STR, future=True)
    with engine.begin() as conn, open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
        conn.exec_driver_sql(f.read())
    print("✅ Schema e views criadas/atualizadas.")

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Expected raw headers similar to: id,room_id/id,noted_date,temp,out/in
    cols = [c.strip().lower() for c in df.columns]
    rename_map = {}
    for c in cols:
        if "room" in c:
            rename_map[c] = "room"
        elif "noted_date" in c or "date" in c:
            rename_map[c] = "ts_raw"
        elif c == "temp" or "temper" in c:
            rename_map[c] = "temperature_c"
        elif "out/in" in c or "outin" in c or "location" in c:
            rename_map[c] = "location"
        elif c == "id":
            rename_map[c] = "raw_id"
    df.columns = [rename_map.get(c, c) for c in cols]
    return df

def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    # Handle formats like "08-12-2018 09:29" (DD-MM-YYYY HH:MM)
    if "ts_raw" in df.columns:
        df["ts"] = pd.to_datetime(df["ts_raw"], errors="coerce", dayfirst=True, infer_datetime_format=True)
        df = df.drop(columns=["ts_raw"])
    elif "date" in df.columns:
        df["ts"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True, infer_datetime_format=True)
        df = df.drop(columns=["date"])
    return df

def _clean_location(df: pd.DataFrame) -> pd.DataFrame:
    if "location" in df.columns:
        df["location"] = df["location"].astype(str).str.strip().str.capitalize()
        df.loc[~df["location"].isin(["In", "Out"]), "location"] = None
    else:
        df["location"] = None
    return df

def load_csv_to_df(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _normalize_columns(df)
    df = _parse_dates(df)
    df = _clean_location(df)
    # Keep only required cols
    keep = ["room", "ts", "temperature_c", "location"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    df = df[keep].dropna(subset=["ts", "temperature_c"])
    # Fix decimal comma if any
    df["temperature_c"] = (df["temperature_c"].astype(str)
                           .str.replace(",", ".", regex=False)
                           .astype(float))
    return df

def import_csv_to_db(csv_path: str = DATA_PATH):
    engine = create_engine(CONN_STR, future=True)
    df = load_csv_to_df(csv_path)
    with engine.begin() as conn:
        df.to_sql("temperature_readings", conn, if_exists="append", index=False)
    print(f"✅ {len(df)} linhas importadas para temperature_readings.")

if __name__ == "__main__":
    create_schema()
    if os.path.exists(DATA_PATH):
        import_csv_to_db(DATA_PATH)
