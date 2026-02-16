"""
Extract local unit types broken down by region from the IFRC GO API
(production & staging). Output is organized for treemap visualization:
categories × Region × count.

Region mapping is derived from the /api/v2/country/ endpoint:
  region 0 = Africa
  region 1 = Americas
  region 2 = Asia Pacific
  region 3 = Europe
  region 4 = MENA
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
    "production": {
        "local_units": "https://goadmin.ifrc.org/api/v2/local-units/",
        "country": "https://goadmin.ifrc.org/api/v2/country/",
    },
    "staging": {
        "local_units": "https://goadmin-stage.ifrc.org/api/v2/local-units/",
        "country": "https://goadmin-stage.ifrc.org/api/v2/country/",
    },
}

REGION_MAP = {
    0: "Africa",
    1: "Americas",
    2: "Asia Pacific",
    3: "Europe",
    4: "MENA",
}

OUTPUT_FILE = "local_units_treemap.xlsx"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fetch_paginated(base_url: str, label: str = "") -> list[dict]:
    """Paginate through all records using offset & limit."""
    all_results = []
    offset = 0

    while True:
        params = {"limit": LIMIT, "offset": offset}
        print(f"  [{label}] Fetching offset={offset} ...")
        resp = requests.get(base_url, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        all_results.extend(results)

        total = data.get("count", 0)
        offset += LIMIT

        print(f"    Got {len(results)} records (total so far: {len(all_results)}/{total})")

        if offset >= total or len(results) == 0:
            break

    return all_results


def build_country_to_region(countries: list[dict]) -> dict[int, str]:
    """Build a mapping from country ID → region name."""
    mapping = {}
    for c in countries:
        country_id = c.get("id")
        region_id = c.get("region")
        if country_id is not None and region_id is not None:
            mapping[country_id] = REGION_MAP.get(region_id, f"Unknown ({region_id})")
    return mapping


def process_environment(env_name: str, urls: dict) -> pd.DataFrame | None:
    """Fetch and process data for a single environment. Returns a DataFrame or None on failure."""
    try:
        # Step 1: Fetch countries for region mapping
        print(f"\n  Fetching country data for region mapping ...")
        countries = fetch_paginated(urls["country"], label=f"{env_name}/Countries")
        country_to_region = build_country_to_region(countries)
        print(f"  Mapped {len(country_to_region)} countries to regions\n")

        # Step 2: Fetch all local units
        print(f"  Fetching all local units ...")
        local_units = fetch_paginated(urls["local_units"], label=f"{env_name}/Local Units")
        print(f"  Total local units: {len(local_units)}\n")

        # Step 3: Count by (type, region)
        type_region_counter = Counter()
        unresolved_countries = set()

        for unit in local_units:
            # Get the type name
            type_details = unit.get("type_details")
            type_name = (
                type_details["name"]
                if type_details and type_details.get("name")
                else "Unknown Type"
            )

            # Get the country ID and resolve to region
            country_id = unit.get("country")
            region_name = country_to_region.get(country_id)

            if region_name is None:
                unresolved_countries.add(country_id)
                region_name = "Unknown Region"

            type_region_counter[(type_name, region_name)] += 1

        if unresolved_countries:
            print(f"  ⚠️  {len(unresolved_countries)} country IDs could not be mapped: {unresolved_countries}")

        # Build DataFrame
        rows = []
        for (type_name, region_name), count in sorted(type_region_counter.items()):
            rows.append({
                "categories": type_name,
                "Region": region_name,
                "count": count,
            })

        df = pd.DataFrame(rows)
        df = df.sort_values(["categories", "Region"]).reset_index(drop=True)
        return df

    except requests.exceptions.ConnectionError:
        print(f"\n  ⚠️  WARNING: Could not reach {env_name}")
        print(f"     The server may be on an internal network or VPN.")
        print(f"     Skipping {env_name} and continuing...\n")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"\n  ⚠️  WARNING: HTTP error from {env_name}: {e}")
        print(f"     Skipping {env_name} and continuing...\n")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    all_sheets = {}

    for env_name, urls in ENVIRONMENTS.items():
        print(f"\n{'='*60}")
        print(f"Processing {env_name.upper()}")
        print(f"{'='*60}")

        df = process_environment(env_name, urls)
        if df is not None:
            all_sheets[env_name] = df
            print(f"\n  ✅ {env_name}: {len(df)} rows (type × region combinations)")
            print(df.to_string(index=False))

    if not all_sheets:
        print("\n❌ Could not fetch data from any environment. Exiting.")
        return

    # Save each environment to a separate sheet in the same Excel file
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for env_name, df in all_sheets.items():
            sheet_name = env_name.capitalize()
            df.to_excel(writer, index=False, sheet_name=sheet_name)

    print(f"\n✅ Saved to {OUTPUT_FILE} ({', '.join(all_sheets.keys())} sheets)")


if __name__ == "__main__":
    main()
