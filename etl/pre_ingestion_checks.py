"""
Pre-ingestion data quality checks.

This script validates cleaned CSV datasets before loading them into PostgreSQL.
The validation strategy is intentionally tolerant, as the data comes from
official statistical sources (INE, EUSTAT, EPA), which often contain known
limitations such as aggregated categories, missing dimensions, or duplicated
logical keys.

The objective is to detect structural or critical issues without blocking
the pipeline due to semantically valid characteristics of public data.
"""

import pandas as pd
from pathlib import Path
from typing import Dict
import sys
from datetime import datetime

# ======================================================
# CONFIGURATION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_clean"

STOP_ON_CRITICAL_ERROR = True

# ======================================================
# DATASET CONFIGURATION
#
# Each dataset defines:
# - required_columns: minimum schema required for ingestion
# - pk: logical primary key used only for validation (not SQL PK)
# - positive_numeric_cols: columns expected to contain non-negative values
# - check_year_range: whether to validate year plausibility
#
# Logical PKs are adapted to the semantic structure of each dataset.
# ======================================================

DATASETS: Dict[str, Dict] = {

    "population_territory.csv": {
        "required_columns": [
            "level", "territory", "year", "population",
            "sex", "age_group", "nationality", "periodo"
        ],

        # Nationality is intentionally excluded from the logical PK.
        # Many INE population series represent total population aggregates
        # where nationality is not applicable and therefore NULL.
        # Including nationality in the PK would generate false data
        # quality warnings without adding analytical value.
        "pk": [
            "level", "territory", "year",
            "sex", "age_group", "periodo"
        ],

        "positive_numeric_cols": ["population"],

        # INE mixes statistical reference years, mid-year data,
        # projections and provisional figures. Therefore, a strict
        # year range validation is not applied to this dataset.
        "check_year_range": False
    },

    "birth_country_clean.csv": {
        "required_columns": [
            "region", "indicator", "sex", "age_group",
            "nationality", "year", "value"
        ],

        # Age group is part of the PK because it is analytically relevant.
        # In some INE series, age group is implicitly "All ages" and not
        # explicitly provided. These cases may appear as NULL and are
        # reported as warnings rather than errors.
        "pk": [
            "region", "indicator", "sex",
            "age_group", "nationality", "year"
        ],

        "positive_numeric_cols": ["value"],
        "check_year_range": True
    },

    "eustat_population_clean.csv": {
        "required_columns": ["province", "nationality", "year", "population"],
        "pk": ["province", "nationality", "year"],
        "positive_numeric_cols": ["population"],
        "check_year_range": True
    },

    "eustat_population_nationality_clean.csv": {
        "required_columns": [
            "ccaa", "province", "nationality",
            "relation_with_activity", "sex", "year", "population"
        ],
        "pk": [
            "province", "nationality",
            "relation_with_activity", "sex", "year"
        ],
        "positive_numeric_cols": ["population"],
        "check_year_range": True
    },

    "epa_contract_clean.csv": {
        "required_columns": [
            "sex", "age_group", "indicator",
            "disability", "year", "value"
        ],

        # EPA indicators may legitimately contain negative values
        # (e.g. year-on-year variation for specific groups).
        "pk": [
            "sex", "age_group", "indicator",
            "disability", "year"
        ],

        "positive_numeric_cols": [],
        "check_year_range": True
    },

    "ine_employment_clean.csv": {
        "required_columns": [
            "relation_activity", "territory", "sex",
            "sector", "nationality", "year", "value"
        ],
        "pk": [
            "relation_activity", "territory", "sex",
            "sector", "nationality", "year"
        ],
        "positive_numeric_cols": [],
        "check_year_range": True
    },

    "eustat_activity_nationality_clean.csv": {
        "required_columns": [
            "rate_type", "province", "nationality",
            "quarter", "year", "value"
        ],
        "pk": ["rate_type", "province", "nationality", "quarter", "year"],
        "positive_numeric_cols": [],
        "check_year_range": True
    },

    "crime_detentions_clean.csv": {
        "required_columns": [
            "province", "region", "nationality",
            "sex", "year", "value"
        ],
        "pk": ["province", "region", "nationality", "sex", "year"],
        "positive_numeric_cols": ["value"],
        "check_year_range": True
    },

    "crime_total_clean.csv": {
        "required_columns": [
            "territory", "crime_type",
            "age_group", "sex", "year", "value"
        ],
        "pk": ["territory", "crime_type", "age_group", "sex", "year"],
        "positive_numeric_cols": ["value"],
        "check_year_range": True
    },

    "housing_prices_annual.csv": {
        "required_columns": [
            "year", "province", "region_type",
            "source", "price_per_m2"
        ],
        "pk": ["year", "province", "source"],
        "positive_numeric_cols": ["price_per_m2"],
        "check_year_range": True
    },

    "ine_pobreza.csv": {
        "required_columns": [
            "level", "territory", "year",
            "indicator", "poverty_rate"
        ],
        "pk": ["territory", "indicator", "year"],
        "positive_numeric_cols": ["poverty_rate"],
        "check_year_range": True
    },

    "ine_foreign_population_province.csv": {
        "required_columns": [
            "province", "province_std", "year",
            "foreign_population"
        ],
        "pk": ["province_std", "year"],
        "positive_numeric_cols": ["foreign_population"],
        "check_year_range": True
    },

    "ine_ecp_foreign_population_province.csv": {
        "required_columns": [
            "province_std", "year", "foreign_population",
            "total_population", "foreign_population_pct"
        ],
        "pk": ["province_std", "year"],
        "positive_numeric_cols": ["foreign_population", "total_population"],
        "check_year_range": True
    }
}

# ======================================================
# VALIDATION LOGIC
# ======================================================

def validate_dataset(path: Path, cfg: dict) -> dict:
    """
    Validates a single dataset according to its configuration.
    Errors indicate critical issues that should stop ingestion.
    Warnings indicate expected or explainable data characteristics.
    """

    result = {
        "file": path.name,
        "status": "OK",
        "errors": [],
        "warnings": [],
        "rows": 0,
    }

    if not path.exists():
        result["status"] = "ERROR"
        result["errors"].append("File does not exist")
        return result

    try:
        df = pd.read_csv(path, low_memory=False)
        result["rows"] = len(df)
    except Exception as e:
        result["status"] = "ERROR"
        result["errors"].append(f"CSV read error: {e}")
        return result

    # Required columns check
    missing = [c for c in cfg["required_columns"] if c not in df.columns]
    if missing:
        result["status"] = "ERROR"
        result["errors"].append(f"Missing required columns: {missing}")

    # NULLs in logical primary key (reported as warnings)
    for col in cfg["pk"]:
        if col in df.columns:
            nulls = df[col].isna().sum()
            if nulls > 0:
                result["warnings"].append(
                    f"NULL values in PK column '{col}': {nulls}"
                )

    # Duplicate logical primary keys
    # Official datasets may legitimately contain multiple series
    # for the same logical dimensions (revisions, provisional data, etc.).
    if cfg["pk"]:
        duplicates = df.duplicated(subset=cfg["pk"]).sum()
        if duplicates > 0:
            result["warnings"].append(
                f"Duplicated logical PK rows (expected in official data): {duplicates}"
            )

    # Negative value validation
    for col in cfg["positive_numeric_cols"]:
        if col in df.columns:
            negatives = (df[col] < 0).sum()
            if negatives > 0:
                result["warnings"].append(
                    f"Negative values found in '{col}': {negatives}"
                )

    # Year plausibility check
    if cfg.get("check_year_range", True) and "year" in df.columns:
        current_year = datetime.now().year
        invalid_years = df[
            (df["year"] < 1990) | (df["year"] > current_year + 1)
        ]
        if len(invalid_years) > 0:
            result["warnings"].append(
                f"Years outside reasonable range: {len(invalid_years)}"
            )

    if result["errors"]:
        result["status"] = "ERROR"
    elif result["warnings"]:
        result["status"] = "WARNING"

    return result

# ======================================================
# MAIN EXECUTION
# ======================================================

def run():
    """
    Executes pre-ingestion checks for all configured datasets.
    """

    print("\n========== PRE-INGESTION DATA QUALITY CHECKS ==========\n")

    error_count = 0

    for file, cfg in DATASETS.items():
        result = validate_dataset(DATA_DIR / file, cfg)

        print(f"[{result['status']}] {file} -> {result['rows']} rows")

        for err in result["errors"]:
            print("  ERROR:", err)

        for warn in result["warnings"]:
            print("  WARNING:", warn)

        print("-" * 60)

        if result["status"] == "ERROR":
            error_count += 1

    if error_count > 0 and STOP_ON_CRITICAL_ERROR:
        print("Critical errors detected. Pipeline execution stopped.")
        sys.exit(1)

    print("Pre-ingestion checks completed successfully.")

if __name__ == "__main__":
    run()
