import math
import pandas as pd
from supabase import create_client

SUPABASE_URL = "https://jrscftcdnkqnpuwqfswg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impyc2NmdGNkbmtxbnB1d3Fmc3dnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzA5NTM1MywiZXhwIjoyMDg4NjcxMzUzfQ._0HNXpJ4lD1b7p7ISoFWo5LyQ829MqiyvPQGieHIYZM"

EXCEL_FILE = "pp_data.xlsx"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def clean_value(v):
    if v is None:
        return None

    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass

    # تحويل التواريخ والأرقام وكل شيء إلى نص آمن
    return str(v).strip() if str(v).strip() != "" else None


def load_sheet(sheet):
    df = pd.read_excel(EXCEL_FILE, sheet_name=sheet, dtype=object)
    df = df.map(clean_value)
    return df


def insert_rows(table, df):
    rows = df.to_dict(orient="records")
    total = len(rows)
    ok_count = 0
    err_count = 0

    for i, row in enumerate(rows, start=1):
        try:
            clean_row = {k: v for k, v in row.items() if v is not None}
            supabase.table(table).insert(clean_row).execute()
            ok_count += 1
            print(f"OK {table}: {i}/{total}")
        except Exception as e:
            err_count += 1
            print(f"ERROR {table}: row {i}/{total}: {e}")

    print(f"FINISH {table}: ok={ok_count}, errors={err_count}")


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
        print(f"START {sheet}")
        df = load_sheet(sheet)
        insert_rows(sheet, df)

    print("DONE")


if __name__ == "__main__":
    migrate()

