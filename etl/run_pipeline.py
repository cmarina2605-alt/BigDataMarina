"""
Main pipeline execution script.

This script orchestrates the complete end-to-end pipeline, including:

 1. Database initialization        (Supabase / PostgreSQL)
 2. Data download                  (INE, EUSTAT, Interior, MIVAU, elections)
 3. Data transformation            (Python cleaners under etl/transform_*.py)
 4. Pre-ingestion data quality     (fail-fast structural checks)
 5. Load data into PostgreSQL      (TRUNCATE + COPY, idempotent)
 6. Post-ingestion data quality    (informational checks on the OLTP layer)
 7. Replicate to BigQuery          (raw base tables  ->  BigQuery dataset)
 8. dbt run on BigQuery            (build 17 stg + 3 int + 3 mart views)
 9. Refresh dashboard charts       (BigQuery -> matplotlib PNGs)

The pipeline follows a fail-fast strategy:
- If pre-ingestion checks fail, the pipeline stops immediately.
- Post-ingestion checks are executed only after successful ingestion.
- Replication, dbt run and dashboard refresh stop on failure because
  every analytical consumer downstream depends on them.

This approach ensures that structurally invalid data is never loaded
into the database, that the analytical layer is rebuilt only from a
validated OLTP snapshot, and that the dashboard always reflects the
most recent dbt run.
"""

import subprocess
import time
import sys


def run_step(name, cmd, stop_on_failure=True):
    """
    Executes a pipeline step and measures execution time.

    Parameters:
    - name: Descriptive name of the step
    - cmd: Command to execute as a list
    - stop_on_failure: Whether to stop the pipeline if the step fails
    """

    print(f"\n========== {name} ==========")
    start = time.time()

    result = subprocess.run(cmd)

    elapsed = round(time.time() - start, 2)

    if result.returncode != 0:
        print(f"ERROR in {name} ({elapsed}s)")

        if stop_on_failure:
            print("Pipeline execution stopped.")
            sys.exit(1)
        else:
            print("Pipeline continues despite failure.")
            return

    print(f"OK {name} ({elapsed}s)")


if __name__ == "__main__":

    total_start = time.time()

    print("\n========== STARTING DATA PIPELINE ==========")


    # --------------------------------------------------
    # 1. DATABASE INITIALIZATION
    # --------------------------------------------------
    # Creates all required tables. This step is idempotent
    # and can be safely executed multiple times.
    run_step(
        "INIT DATABASE",
        [sys.executable, "etl/init_db.py"]
    )

    # --------------------------------------------------
    # 2. DATA DOWNLOAD
    # --------------------------------------------------

    # Demography
    run_step("DOWNLOAD INE", [sys.executable, "download/ine_download.py"])
    run_step("DOWNLOAD EUSTAT", [sys.executable, "download/eustat_download.py"])
    run_step("DOWNLOAD EUSTAT POPULATION NATIONALITY",
             [sys.executable, "download/eustat_population_nationality.py"])

    # Crime
    run_step("DOWNLOAD CRIME DETENTIONS",
             [sys.executable, "download/crime_download.py"])
    run_step("DOWNLOAD CRIME TOTAL",
             [sys.executable, "download/crime_total_download.py"])

    # Employment
    run_step("DOWNLOAD EMPLOYMENT INE",
             [sys.executable, "download/employ_download.py"])
    run_step("DOWNLOAD EMPLOYMENT EUSTAT",
             [sys.executable, "download/employ_eustat_download.py"])

    # Elections
    run_step("DOWNLOAD and TRANSFORM ELECTIONS",
             [sys.executable, "download/elections_download_transform.py"])

    # Housing
    run_step("DOWNLOAD HOUSING",
             [sys.executable, "download/vivienda_download.py"])

    # Social / poverty
    run_step("DOWNLOAD POVERTY DATA",
             [sys.executable, "download/ine_pobreza_download.py"])

    # --------------------------------------------------
    # 3. DATA TRANSFORMATION
    # --------------------------------------------------

    # Demography
    run_step("TRANSFORM INE POPULATION",
             [sys.executable, "etl/transform_ine_population.py"])
    run_step("TRANSFORM BIRTH COUNTRY",
             [sys.executable, "etl/transform_birth_country.py"])
    run_step("TRANSFORM EUSTAT POPULATION",
             [sys.executable, "etl/transform_eustat_population.py"])
    run_step("TRANSFORM EUSTAT POPULATION NATIONALITY",
             [sys.executable, "etl/transform_eustat_population_nationality.py"])

    # Crime
    run_step("TRANSFORM CRIME DETENTIONS",
             [sys.executable, "etl/transform_crime.py"])
    run_step("TRANSFORM CRIME TOTAL",
             [sys.executable, "etl/transform_crime_total.py"])

    # Employment
    run_step("TRANSFORM EMPLOYMENT INE",
             [sys.executable, "etl/transform_employ.py"])
    run_step("TRANSFORM EMPLOYMENT EUSTAT",
             [sys.executable, "etl/transform_employ_eustat.py"])
    run_step("TRANSFORM EPA CONTRACTS",
             [sys.executable, "etl/transform_epa_contract.py"])

    # Housing
    run_step("TRANSFORM HOUSING",
             [sys.executable, "etl/transform_vivienda.py"])

    # Social
    run_step("TRANSFORM POVERTY DATA",
             [sys.executable, "etl/transform_ine_pobreza.py"])

    # --------------------------------------------------
    # 4. PRE-INGESTION DATA QUALITY CHECKS
    # --------------------------------------------------
    # These checks validate the structure and basic quality
    # of the cleaned CSV files before loading them into the database.
    # Any critical error at this stage stops the pipeline.
    run_step(
        "PRE-INGESTION DATA QUALITY CHECKS",
        [sys.executable, "etl/pre_ingestion_checks.py"],
        stop_on_failure=True
    )


    # --------------------------------------------------
    # 5. LOAD DATA INTO POSTGRESQL
    # --------------------------------------------------
    run_step(
        "LOAD DATA INTO POSTGRESQL",
        [sys.executable, "etl/load_postgres.py"]
    )

    # --------------------------------------------------
    # 6. POST-INGESTION DATA QUALITY CHECKS
    # --------------------------------------------------
    # These checks validate the database state after ingestion.
    # They are informative and do not stop the pipeline.
    run_step(
        "POST-INGESTION DATA QUALITY CHECKS",
        [sys.executable, "etl/data_quality_checks.py"],
        stop_on_failure=False
    )

    # --------------------------------------------------
    # 7. REPLICATE BASE TABLES TO BIGQUERY
    # --------------------------------------------------
    # Copies the 12 OLTP base tables from Supabase into the
    # BigQuery dataset so that the analytical warehouse has
    # a fresh snapshot before dbt runs on top of it.
    run_step(
        "REPLICATE TO BIGQUERY",
        [sys.executable, "etl/replicate_to_bigquery.py"]
    )

    # --------------------------------------------------
    # 8. RUN dbt ON BIGQUERY
    # --------------------------------------------------
    # Executes the dbt project against the BigQuery target,
    # rebuilding the 17 staging, 3 intermediate and 3 mart
    # views. schema.yml tests run immediately after to catch
    # contract regressions.
    run_step("DBT RUN",  ["dbt", "run",  "--profiles-dir", ".dbt"])
    run_step("DBT TEST", ["dbt", "test", "--profiles-dir", ".dbt"],
             stop_on_failure=False)

    # --------------------------------------------------
    # 9. REFRESH DASHBOARD CHARTS
    # --------------------------------------------------
    # Queries the freshly built marts in BigQuery and writes
    # the four PNGs consumed by the report and slides. This
    # step is the live-serving entry point for the analytical
    # use case.
    run_step(
        "REFRESH DASHBOARD CHARTS",
        [sys.executable, "dashboard/build_charts_from_marts.py"]
    )

    total_elapsed = round(time.time() - total_start, 2)

    print(f"\nPipeline completed in {total_elapsed}s")
