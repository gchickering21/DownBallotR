from __future__ import annotations

import re
from datetime import datetime
import requests
from lxml import html

from .models import NcElectionZip

INDEX_URL = "https://www.ncsbe.gov/results-data/election-results/historical-election-results-data"
ZIP_RE = re.compile(r"results_pct_(\d{8})\.zip$")


def discover_nc_results_zips() -> list[NcElectionZip]:
    r = requests.get(INDEX_URL, timeout=30)
    r.raise_for_status()
    doc = html.fromstring(r.text)

    urls = doc.xpath("//a[contains(@href,'results_pct_') and contains(@href,'.zip')]/@href")
    labels = doc.xpath("//a[contains(@href,'results_pct_') and contains(@href,'.zip')]/text()")

    out: list[NcElectionZip] = []
    for url, label in zip(urls, labels):
        m = ZIP_RE.search(url)
        if not m:
            continue
        d = datetime.strptime(m.group(1), "%Y%m%d").date()
        out.append(
            NcElectionZip(
                election_date=d,
                zip_url=url,
                label=" ".join(label.split()),
            )
        )

    return sorted(out, key=lambda x: x.election_date)
