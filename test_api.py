# writedata.py
import json
import datetime as dt
import requests
from requests.auth import HTTPBasicAuth

BASE_URL = "https://newcastleunited7703.edge10online.co.uk"

USERNAME = "edge10"
PASSWORD = "loRWROgw0XtgMnnit0g6o2s2NKWIWJm6yYJIzpLCT0dyVvpvY7Sb5FPFA1QzuWN"  # exact

AUTH = HTTPBasicAuth(USERNAME, PASSWORD)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

def iso_date_seconds(date_str: str) -> str:
    d = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    return dt.datetime(d.year, d.month, d.day, 0, 0, 0).isoformat(timespec="seconds")

def to_contact_type(v: str) -> int:
    v = (v or "").strip().lower()
    if v in {"2", "staff", "s"}:
        return 2
    return 1  # default player

def clean(d: dict) -> dict:
    return {k: v for k, v in d.items() if v not in ("", None, {}, [])}

def test_auth():
    r = requests.get(f"{BASE_URL}/api/entity/", auth=AUTH, headers={"Accept":"application/json"}, timeout=30)
    print("Auth test:", r.status_code)
    if not r.ok:
        print(r.text)

def create_subject(payload: dict):
    url = f"{BASE_URL}/api/entity/subject"
    r = requests.post(url, auth=AUTH, headers=HEADERS, json=payload, timeout=45)
    print("\nCreate status:", r.status_code)
    if not r.ok:
        try:
            print(json.dumps(r.json(), indent=2))
        except Exception:
            print(r.text)
        return None
    data = r.json()
    print("Created id:", data.get("id"))
    print(json.dumps(data, indent=2))
    return data.get("id")

if __name__ == "__main__":
    # 1) quick auth check
    test_auth()

    # 2) minimal prompts
    first_name = input("First name [Test]: ") or "Test"
    last_name  = input("Last name [Player]: ") or "Player"
    dob_str    = input("DOB YYYY-MM-DD [2001-01-01]: ") or "2001-01-01"
    gender     = (input("Gender male/female/unknown [unknown]: ") or "unknown").lower()
    ctype_in   = input("Contact type player=1 staff=2 [1]: ") or "1"
    title_in   = input("Title Mr/Mrs/Ms/Miss/Dr (blank to omit): ").strip()

    email      = input("Email (optional): ").strip()
    mobile     = input("Mobile (optional): ").strip()
    username   = input("Username (optional): ").strip()
    groups_raw = input("Group IDs comma separated (optional): ").strip()

    title_map = {"mr":"Mr","mrs":"Mrs","ms":"Ms","miss":"Miss","dr":"Dr","prof":"Prof","mx":"Mx"}
    title = title_map.get(title_in.lower()) if title_in else None

    payload = clean({
        "contactType": to_contact_type(ctype_in),
        "dateOfBirth": iso_date_seconds(dob_str),
        "title": title,
        "gender": gender,
        "firstName": first_name,
        "lastName": last_name,
        "emailAddress": email,
        "mobileNumber": mobile,
        "username": username,
        "profile": {"customID": None},
        "groupIds": [g.strip() for g in groups_raw.split(",") if g.strip()],
    })

    print("\nSubmitting payload:")
    print(json.dumps(payload, indent=2))

    create_subject(payload)
