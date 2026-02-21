import os

def parse_admin_ids() -> set[int]:
    raw = (os.getenv("PP_ADMIN_IDS") or "").strip()
    if not raw:
        return set()
    out = set()
    for p in raw.split(","):
        p = p.strip()
        if p.isdigit():
            out.add(int(p))
    return out
