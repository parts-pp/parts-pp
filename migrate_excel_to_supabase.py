import pandas as pd
from supabase import create_client

SUPABASE_URL = "https://jrscftcdnkqnpuwqfswg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impyc2NmdGNkbmtxbnB1d3Fmc3dnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzA5NTM1MywiZXhwIjoyMDg4NjcxMzUzfQ._0HNXpJ4lD1b7p7ISoFWo5LyQ829MqiyvPQGieHIYZM"

EXCEL_FILE = "pp_data.xlsx"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_sheet(sheet):
    df = pd.read_excel(EXCEL_FILE, sheet_name=sheet, dtype=str)
    df = df.where(pd.notnull(df), None)
    return df

def insert_rows(table, df):
    data = df.to_dict(orient="records")
    for row in data:
        try:
            supabase.table(table).insert(row).execute()
        except Exception as e:
            print(f"خطأ في {table}: {e}")

def migrate():
    sheets = [
        "orders",
        "items",
        "events",
        "messages",
        "traders",
        "settings",
        "legal_log",
        "trader_subs",
    ]

    for sheet in sheets:
        print(f"نقل {sheet}")
        df = load_sheet(sheet)
        insert_rows(sheet, df)

    print("تم نقل جميع البيانات")

if __name__ == "__main__":
    migrate()

