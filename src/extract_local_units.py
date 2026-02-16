"""
Extract local unit types from the IFRC GO API (production & staging)
and output counts to an Excel file suitable for waffle chart visualization.
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from collections import Counter

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()
TOKEN = os.getenv("IFRC_GO_TOKEN")

HEADERS = {"Authorization": f"Token {TOKEN}"}
LIMIT = 50  # records per page

ENVIRONMENTS = {
    "production": "https://goadmin.ifrc.org/api/v2/local-units/",
    "staging": "https://goadmin-stage.ifrc.org/api/v2/local-units/",
}

OUTPUT_FILE = "local_units_summary.xlsx"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fetch_all_local_units(base_url: str) -> list[dict]:
    """Paginate through all local units using offset & limit."""
    all_results = []
    offset = 0

    while True:
        params = {"limit": LIMIT, "offset": offset}
        print(f"  Fetching {base_url}  offset={offset} ...")
        resp = requests.get(base_url, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        all_results.extend(results)

        total = data.get("count", 0)
        offset += LIMIT

        print(f"    Got {len(results)} records (total so far: {len(all_results)}/{total})")

        # Stop when we've retrieved all records
        if offset >= total or len(results) == 0:
            break

    return all_results


def count_types(records: list[dict]) -> Counter:
    """Count occurrences of each local-unit type from type_details.name."""
    counter = Counter()
    for record in records:
        type_details = record.get("type_details")
        if type_details and type_details.get("name"):
            counter[type_details["name"]] += 1
        else:
            counter["Unknown / No Type"] += 1
    return counter


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    env_counts = {}

    for env_name, base_url in ENVIRONMENTS.items():
        print(f"\n{'='*60}")
        print(f"Fetching from {env_name.upper()} ({base_url})")
        print(f"{'='*60}")
        try:
            records = fetch_all_local_units(base_url)
            counts = count_types(records)
            env_counts[env_name] = counts
            print(f"  Total records: {sum(counts.values())}")
            print(f"  Types found: {dict(counts)}")
        except requests.exceptions.ConnectionError as e:
            print(f"\n  ⚠️  WARNING: Could not reach {env_name} ({base_url})")
            print(f"     The server may be on an internal network or VPN.")
            print(f"     Skipping {env_name} and continuing...\n")
        except requests.exceptions.HTTPError as e:
            print(f"\n  ⚠️  WARNING: HTTP error from {env_name}: {e}")
            print(f"     Skipping {env_name} and continuing...\n")

    if not env_counts:
        print("\n❌ Could not fetch data from any environment. Exiting.")
        return

    # Merge all type names from all reachable environments
    all_types = sorted(set().union(*[c.keys() for c in env_counts.values()]))

    # Build the DataFrame — include columns only for reachable environments
    rows = []
    for type_name in all_types:
        row = {"categories": type_name}
        for env_name in ENVIRONMENTS:
            if env_name in env_counts:
                row[f"count_{env_name}"] = env_counts[env_name].get(type_name, 0)
        rows.append(row)

    df = pd.DataFrame(rows)

    # Add percentage and waffle-cell columns for each reachable environment
    for env_name in env_counts:
        count_col = f"count_{env_name}"
        total = df[count_col].sum()

        # Percentage of total (useful for waffle chart sizing)
        df[f"pct_{env_name}"] = (
            (df[count_col] / total * 100).round(2) if total else 0
        )

        # Waffle cells: a 10×10 grid = 100 cells, each cell ≈ 1%
        df[f"waffle_cells_{env_name}"] = (
            (df[count_col] / total * 100).round(0).astype(int)
            if total
            else 0
        )

    # Save to Excel
    df.to_excel(OUTPUT_FILE, index=False, sheet_name="Local Unit Types")
    print(f"\n✅ Saved to {OUTPUT_FILE}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
