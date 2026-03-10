import pandas as pd
from supabase import create_client

# ====== إعدادات Supabase ======
SUPABASE_URL = "ضع_رابط_المشروع_هنا"
SUPABASE_KEY = "ضع_service_role_key_هنا"

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