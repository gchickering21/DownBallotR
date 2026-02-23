from __future__ import annotations

import io
import re
import zipfile

import pandas as pd
import requests

_MEMBER_SCORE_RULES = [
    (re.compile(r"results[_-]?pct", re.IGNORECASE), 200),
    (re.compile(r"\bresults\b", re.IGNORECASE), 40),
    (re.compile(r"\bpct\b", re.IGNORECASE), 20),
    (re.compile(r"layout", re.IGNORECASE), -500),
    (re.compile(r"readme|info|note", re.IGNORECASE), -500),
]


def download_zip_bytes(zip_url: str, timeout: int = 60) -> bytes:
    r = requests.get(zip_url, timeout=timeout)
    r.raise_for_status()
    return r.content


def _score_member(name: str) -> int:
    score = 0
    for rx, s in _MEMBER_SCORE_RULES:
        if rx.search(name):
            score += s
    if name.lower().endswith((".txt", ".csv", ".tsv")):
        score += 20
    return score


def _select_results_member(zf: zipfile.ZipFile) -> str:
    members = [m for m in zf.namelist() if not m.endswith("/")]
    if not members:
        raise ValueError("ZIP has no members")

    scored = sorted(((m, _score_member(m)) for m in members), key=lambda x: x[1], reverse=True)
    best, best_score = scored[0]

    # If confident, return
    if best_score >= 50:
        return best

    # Fallback: choose the largest plausible data file (txt/csv/tsv) excluding readme/layout
    candidates = []
    for m in members:
        ml = m.lower()
        if not ml.endswith((".txt", ".csv", ".tsv")):
            continue
        if "readme" in ml or "layout" in ml or "info" in ml or "note" in ml:
            continue
        try:
            candidates.append((m, zf.getinfo(m).file_size))
        except KeyError:
            continue

    if candidates:
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    raise ValueError(f"Could not identify results file. Top candidate: {best} (score={best_score})")


def _read_delimited(data: bytes, filename: str) -> pd.DataFrame:
    # Strip embedded NUL bytes — some NC ZIP files contain \x00 padding that
    # causes "Embedded NUL in string" errors when R receives the DataFrame.
    data = data.replace(b"\x00", b"")
    raw = io.BytesIO(data)

    lower = filename.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(raw, sep=",", dtype=str, engine="python")

    # try tab first
    try:
        raw.seek(0)
        df = pd.read_csv(raw, sep="\t", dtype=str, engine="python")
        if df.shape[1] > 1:
            return df
    except Exception:
        pass

    # fallback comma
    raw.seek(0)
    return pd.read_csv(raw, sep=",", dtype=str, engine="python")


def read_results_pct_from_zip(zip_bytes: bytes) -> tuple[str, pd.DataFrame]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        member = _select_results_member(zf)
        with zf.open(member) as f:
            data = f.read()
    return member, _read_delimited(data, member)
