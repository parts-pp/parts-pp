import traceback
import pandas as pd
from supabase import create_client

SUPABASE_URL = "https://jrscftcdnkqnpuwqfswg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impyc2NmdGNkbmtxbnB1d3Fmc3dnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzA5NTM1MywiZXhwIjoyMDg4NjcxMzUzfQ._0HNXpJ4lD1b7p7ISoFWo5LyQ829MqiyvPQGieHIYZM"

EXCEL_FILE = "pp_data.xlsx"


def clean_scalar(v):
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if v is None:
        return None

    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s


def load_sheet(sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name, dtype=object)
    df = df.astype(object)
    df = df.where(pd.notnull(df), None)
    return df


def sanitize_row(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        vv = clean_scalar(v)
        if vv is not None:
            out[k] = vv
    return out


def insert_rows(table: str, df: pd.DataFrame, supabase):
    rows = df.to_dict(orient="records")
    total = len(rows)
    ok_count = 0
    err_count = 0

    for i, row in enumerate(rows, start=1):
        try:
            clean_row = sanitize_row(row)
            if not clean_row:
                print(f"SKIP {table}: row {i}/{total} empty")
                continue

            supabase.table(table).insert(clean_row).execute()
            ok_count += 1
            print(f"OK {table}: {i}/{total}")
        except Exception as e:
            err_count += 1
            print(f"ERROR {table}: row {i}/{total}: {e}")

    print(f"FINISH {table}: ok={ok_count}, errors={err_count}")


def migrate():
    print("START SCRIPT")

    if "ضع هنا" in SUPABASE_KEY or not SUPABASE_KEY.strip():
        raise RuntimeError("SUPABASE_KEY لم يتم وضعه بشكل صحيح")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    sheets = [
        "traders",
    ]

    for sheet in sheets:
        print(f"START {sheet}")
        df = load_sheet(sheet)
        insert_rows(sheet, df, supabase)

    print("DONE")


if __name__ == "__main__":
    try:
        migrate()
    except Exception as e:
        print("FATAL ERROR:", e)
        traceback.print_exc()
        raise

SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impyc2NmdGNkbmtxbnB1d3Fmc3dnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MzA5NTM1MywiZXhwIjoyMDg4NjcxMzUzfQ._0HNXpJ4lD1b7p7ISoFWo5LyQ829MqiyvPQGieHIYZM"
