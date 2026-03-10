import pandas as pd
from supabase import create_client

SUPABASE_URL = "https://jrscftcdnkqnpuwqfswg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impyc2NmdGNkbmtxbnB1d3Fmc3dnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzA5NTM1MywiZXhwIjoyMDg4NjcxMzUzfQ._0HNXpJ4lD1b7p7ISoFWo5LyQ829MqiyvPQGieHIYZM"

EXCEL_FILE = "pp_data.xlsx"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_value(v):
    if pd.isna(v):
        return None
    return v

def load_sheet(sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name, dtype=object)
    return df.map(clean_value)

def insert_rows(table: str, df: pd.DataFrame):
    rows = df.to_dict(orient="records")
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            supabase.table(table).insert(batch).execute()
            print(f"OK {table}: {i + len(batch)}/{len(rows)}")
        except Exception as e:
            print(f"ERROR {table}: {e}")

def migrate():
    mapping = {
        "orders": "orders",
        "items": "order_items",
        "events": "events",
        "messages": "messages",
        "traders": "traders",
        "trader_subs": "trader_subscriptions",
        "settings": "settings",
        "legal_log": "legal_log",
    }

    for sheet, table in mapping.items():
        print(f"START {sheet} -> {table}")
        df = load_sheet(sheet)
        insert_rows(table, df)

    print("DONE")

if __name__ == "__main__":
    migrate()