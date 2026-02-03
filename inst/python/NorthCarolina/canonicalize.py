import re
import pandas as pd


def classify_nc_office(name: str) -> str | None:
    s = name.lower()
    if "mayor" in s:
        return "mayor"
    if "city council" in s or "town council" in s:
        return "city_council"
    if "board of education" in s or "school" in s:
        return "school_board"
    if "county" in s:
        return "county_legislature"
    return None


def extract_district(name: str) -> str | None:
    m = re.search(r"district\s*(\d+)", name, re.IGNORECASE)
    return m.group(1) if m else None


def extract_jurisdiction(name: str, office: str | None):
    if office == "mayor":
        m = re.search(r"(city|town) of ([A-Z\s]+)", name, re.I)
        return (m.group(2).title(), "city") if m else (None, None)
    if office == "school_board":
        return (name.split(" BOARD")[0].title(), "school_district")
    return (None, None)
