import pandas as pd
from supabase import create_client

# ====== إعدادات Supabase ======
SUPABASE_URL = "https://jrscftcdnkqnpuwqfswg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impyc2NmdGNkbmtxbnB1d3Fmc3dnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMwOTUzNTMsImV4cCI6MjA4ODY3MTM1M30.gbpk9-c2eoN0PG1IvKKyHvjmRFXBCJvQgB8HTfrQ6Sg"

# ====== ملف Excel ======
EXCEL_FILE = "pp_data.xlsx"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_sheet(sheet_name):
    df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name)
    df = df.where(pd.notnull(df), None)  # تحويل NaN إلى None
    return df

def insert_rows(table, df):
    data = df.to_dict(orient="records")
    for row in data:
        try:
            supabase.table(table).insert(row).execute()
        except Exception as e:
            print(f"خطأ في إدخال صف في {table}: {e}")

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
        print(f"نقل {sheet} -> {table}")
        df = load_sheet(sheet)
        insert_rows(table, df)

    print("تم نقل جميع البيانات")

if __name__ == "__main__":
    migrate()