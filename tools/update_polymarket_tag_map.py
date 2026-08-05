#!/usr/bin/env python3
"""Refresh the checked-in Polymarket tag ID lookup without runtime loading."""

import argparse
import json
import subprocess
import time
from pathlib import Path
from urllib.parse import urlencode


def fetch_tags():
    tags = {}
    for offset in range(0, 100_000, 100):
        query = urlencode({"limit": 100, "offset": offset, "order": "id", "ascending": "true"})
        url = "https://gamma-api.polymarket.com/tags?" + query
        for attempt in range(8):
            try:
                response = subprocess.run(
                    ["curl", "-fsS", "--max-time", "30", url],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                page = json.loads(response.stdout)
                break
            except (subprocess.CalledProcessError, json.JSONDecodeError):
                if attempt == 7:
                    raise
                time.sleep(0.5 * (attempt + 1))
        if not page:
            break
        for tag in page:
            tag_id = str(tag.get("id") or "").strip()
            if tag_id:
                tags[tag_id] = {
                    "label": tag.get("label") or "",
                    "slug": tag.get("slug") or "",
                }
        if len(page) < 100:
            break
        time.sleep(0.05)
    return dict(sorted(tags.items(), key=lambda item: (0, int(item[0])) if item[0].isdigit() else (1, item[0])))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("config/polymarket_tags.json"))
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(fetch_tags(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
