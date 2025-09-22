# writedata.py
import json
import datetime as dt
import requests
from requests.auth import HTTPBasicAuth

# ---------- Basic config (edit if needed) ----------
SITE_SLUG = "newcastleunited7703"          # e.g. "northamptontownfc"
SITE_DOMAIN = "edge10online.co.uk"         # may vary per client
BASE_URL = f"https://{SITE_SLUG}.{SITE_DOMAIN}"

USERNAME = "edge10"
PASSWORD = "loRWROgw0XtgMnnit0g6o2s2NKWIWJm6yYJIzpLCT0dyVvpvY7Sb5FPFA1QzuWN"

AUTH = HTTPBasicAuth(USERNAME, PASSWORD)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

VALID_TITLES = {"mr", "mrs", "ms", "miss", "dr", "prof", "mx"}
TITLE_CASE = {"mr": "Mr", "mrs": "Mrs", "ms": "Ms", "miss": "Miss", "dr": "Dr", "prof": "Prof", "mx": "Mx"}


def iso_date_seconds_from_str(date_str: str) -> str:
    """Convert 'YYYY-MM-DD' -> 'YYYY-MM-DDT00:00:00'."""
    d = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    return dt.datetime(d.year, d.month, d.day, 0, 0, 0).isoformat(timespec="seconds")


def to_contact_type(value: str) -> int:
    """Map player/staff -> 1/2 (or accept 1/2 directly)."""
    v = (value or "").strip().lower()
    if v in {"1", "player", "p"}:
        return 1
    if v in {"2", "staff", "s"}:
        return 2
    return 1  # default to player


def normalize_title(s: str) -> str | None:
    """Return canonical title (Mr/Ms/Dr...) or None to omit."""
    s = (s or "").strip()
    if not s:
        return None
    low = s.lower()
    if low in VALID_TITLES:
        return TITLE_CASE[low]
    return None  # unrecognised -> omit field


def clean(d: dict) -> dict:
    """Drop empty strings, None, empty dicts and lists."""
    return {k: v for k, v in d.items() if v not in ("", None, {}, [])}


def prompt(msg: str, default: str | None = None) -> str:
    tip = f" [{default}]" if default not in (None, "") else ""
    val = input(f"{msg}{tip}: ").strip()
    return val if val else (default or "")


def main():
    print("\n=== Create Player (Subject) ===")

    # Minimal prompts to test write
    first_name = prompt("First name", "testplayerapi_test")
    last_name = prompt("Last name", "testplayerapi_test_lastname")
    dob_str = prompt("Date of Birth (YYYY-MM-DD)", "2001-01-01")
    gender = prompt("Gender (male/female/unknown)", "male").lower()
    ctype_in = prompt("Contact type (player=1 / staff=2)", "1")
    title_in = prompt("Title (Mr/Mrs/Ms/Miss/Dr) - leave blank to omit", "")

    email = prompt("Email (optional)", "")
    mobile = prompt("Mobile (optional)", "")
    username = prompt("Username (optional)", "")
    groups_raw = prompt("Group IDs (comma separated GUIDs, optional)", "")

    payload = {
        "contactType": to_contact_type(ctype_in),                 # 1=player, 2=staff
        "dateOfBirth": iso_date_seconds_from_str(dob_str),        # 'YYYY-MM-DDT00:00:00'
        "title": normalize_title(title_in),                       # omit if None
        "gender": gender,                                         # match API shape
        "profession": "",
        "address": "",
        "city": "",
        "region": "",
        "postcode": "",
        "country": "",
        "firstName": first_name,
        "lastName": last_name,
        "emailAddress": email,
        "mobileNumber": mobile,
        "username": username,
        "profile": {"customID": None},                            # mirrors example shape
        "groupIds": [g.strip() for g in groups_raw.split(",") if g.strip()],
    }

    payload = clean(payload)  # drop empty or None fields (avoids title validation)

    url = f"{BASE_URL}/api/entity/subject"

    print("\nSubmitting payload:")
    print(json.dumps(payload, indent=2))

    try:
        resp = requests.post(url, auth=AUTH, headers=HEADERS, json=payload, timeout=45)
    except requests.RequestException as e:
        print("\nRequest failed:", e)
        return

    print("\nStatus:", resp.status_code)
    if not resp.ok:
        # Try to show API error details if JSON
        try:
            print(json.dumps(resp.json(), indent=2))
        except Exception:
            print(resp.text)
        return

    created = resp.json()
    print("\n✅ Created subject id:", created.get("id"))
    print(json.dumps(created, indent=2))

    # Optional read-back to confirm
    sid = created.get("id")
    if sid:
        read_url = f"{BASE_URL}/api/entity/subject/{sid}"
        try:
            r = requests.get(read_url, auth=AUTH, headers={"Accept": "application/json"}, timeout=30)
            print("\nRead-back status:", r.status_code)
            if r.ok:
                print(json.dumps(r.json(), indent=2))
            else:
                print(r.text)
        except requests.RequestException as e:
            print("\nRead-back failed:", e)


if __name__ == "__main__":
    main()
