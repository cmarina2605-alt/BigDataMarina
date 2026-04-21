"""
Database initialization script.

This script creates the PostgreSQL schema used by the project.
All tables are documented at table and column level using
PostgreSQL comments in order to improve schema readability
and data understanding.

The schema uses minimal indexing due to Supabase free tier
limitations. Data integrity and logical consistency are
validated at the ETL level.
"""

import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path


# ======================================================
# ENVIRONMENT CONFIGURATION
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def get_conn():
    """
    Creates and returns a PostgreSQL connection using
    environment variables.
    """

    return psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        sslmode="require"
    )


# ======================================================
# SQL SCHEMA WITH DOCUMENTATION
# ======================================================

SQL = """
-- =====================================================
-- FULL DATABASE RESET (DROP EVERYTHING IN PUBLIC)
-- =====================================================
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;

-- =====================================================
-- INE TOTAL POPULATION
-- =====================================================
CREATE TABLE population_total (
    id           BIGSERIAL PRIMARY KEY,
    level        TEXT NOT NULL,
    territory    TEXT NOT NULL,
    year         INTEGER NOT NULL,
    population   NUMERIC NOT NULL,
    sex          TEXT,
    age_group    TEXT,
    nationality  TEXT,
    concept      TEXT,
    data_type    TEXT,
    unit         TEXT DEFAULT 'Personas',
    tipo_dato    TEXT,
    periodo      TEXT,
    created_at   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE population_total IS
'Total population data published by INE, disaggregated by demographic dimensions.';

COMMENT ON COLUMN population_total.level IS
'Administrative aggregation level (e.g. country, region, province).';

COMMENT ON COLUMN population_total.territory IS
'Name of the geographical territory.';

COMMENT ON COLUMN population_total.year IS
'Reference year of the population data.';

COMMENT ON COLUMN population_total.population IS
'Number of inhabitants for the given dimensions.';

COMMENT ON COLUMN population_total.sex IS
'Sex category of the population (may be NULL for totals).';

COMMENT ON COLUMN population_total.age_group IS
'Age group category (may be NULL for aggregated series).';

COMMENT ON COLUMN population_total.nationality IS
'Nationality category (may be NULL when not applicable).';

COMMENT ON COLUMN population_total.concept IS
'Statistical concept as defined by INE metadata.';

COMMENT ON COLUMN population_total.data_type IS
'Type of data (e.g. provisional, definitive).';

COMMENT ON COLUMN population_total.unit IS
'Measurement unit of the population values.';

COMMENT ON COLUMN population_total.tipo_dato IS
'Original data type label provided by INE.';

COMMENT ON COLUMN population_total.periodo IS
'Statistical reference period (e.g. January 1st, mid-year).';


-- =====================================================
-- EUSTAT TOTAL POPULATION
-- =====================================================
CREATE TABLE IF NOT EXISTS population_eustat_total (
    id BIGSERIAL PRIMARY KEY,
    province TEXT NOT NULL,
    nationality TEXT NOT NULL,
    year INTEGER NOT NULL,
    population INTEGER NOT NULL
);

COMMENT ON TABLE population_eustat_total IS
'Total population data published by EUSTAT.';

COMMENT ON COLUMN population_eustat_total.province IS
'Province within the Basque Country.';

COMMENT ON COLUMN population_eustat_total.nationality IS
'Nationality group of the population.';

COMMENT ON COLUMN population_eustat_total.year IS
'Reference year of the population data.';

COMMENT ON COLUMN population_eustat_total.population IS
'Number of inhabitants.';


-- =====================================================
-- EUSTAT POPULATION BY NATIONALITY
-- =====================================================


CREATE TABLE population_eustat_nationality (
    id BIGSERIAL PRIMARY KEY,
    ccaa TEXT NOT NULL DEFAULT 'País Vasco',
    province TEXT NOT NULL,
    nationality TEXT NOT NULL,
    relation_with_activity TEXT NOT NULL,
    sex TEXT NOT NULL,
    year INTEGER NOT NULL,
    population NUMERIC NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE population_eustat_nationality IS
'Population data by nationality and labour activity status from EUSTAT.';

COMMENT ON COLUMN population_eustat_nationality.relation_with_activity IS
'Relationship with the labour market (active, inactive, employed, unemployed).';


-- =====================================================
-- EPA CONTRACT STATISTICS
-- =====================================================
CREATE TABLE IF NOT EXISTS epa_contract_stats (
    id BIGSERIAL PRIMARY KEY,
    sex TEXT NOT NULL,
    age_group TEXT NOT NULL,
    indicator TEXT NOT NULL,
    disability TEXT NOT NULL,
    year INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

COMMENT ON TABLE epa_contract_stats IS
'Employment and contract indicators from the EPA survey.';

COMMENT ON COLUMN epa_contract_stats.indicator IS
'Type of employment indicator measured.';

COMMENT ON COLUMN epa_contract_stats.value IS
'Numeric value of the indicator (may be negative for variations).';


-- =====================================================
-- MIGRATION / BIRTH COUNTRY
-- =====================================================
CREATE TABLE IF NOT EXISTS migration_birth_country (
    id BIGSERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    indicator TEXT NOT NULL,
    sex TEXT,
    age_group TEXT,
    nationality TEXT,
    periodicity TEXT,
    data_type TEXT,
    year INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT
);

COMMENT ON TABLE migration_birth_country IS
'Migration indicators by country of birth published by INE.';

COMMENT ON COLUMN migration_birth_country.indicator IS
'Migration-related indicator defined by INE.';

COMMENT ON COLUMN migration_birth_country.value IS
'Numeric value of the migration indicator.';


-- =====================================================
-- CRIME DETENTIONS
-- =====================================================
CREATE TABLE IF NOT EXISTS crime_detentions (
    id BIGSERIAL PRIMARY KEY,
    province TEXT NOT NULL,
    region TEXT,
    nationality TEXT NOT NULL,
    sex TEXT NOT NULL,
    year INTEGER NOT NULL,
    value INTEGER NOT NULL
);

COMMENT ON TABLE crime_detentions IS
'Number of crime detentions by demographic characteristics.';


-- =====================================================
-- TOTAL CRIME RECORDS
-- =====================================================
CREATE TABLE IF NOT EXISTS crime_detentions_total (
    id BIGSERIAL PRIMARY KEY,
    territory TEXT NOT NULL,
    crime_type TEXT NOT NULL,
    age_group TEXT NOT NULL,
    sex TEXT NOT NULL,
    year INTEGER NOT NULL,
    value INTEGER NOT NULL
);

COMMENT ON TABLE crime_detentions_total IS
'Aggregated crime records by type and demographic group.';


-- =====================================================
-- INE EMPLOYMENT
-- =====================================================
CREATE TABLE IF NOT EXISTS ine_employment (
    id BIGSERIAL PRIMARY KEY,
    relation_activity TEXT NOT NULL,
    territory TEXT NOT NULL,
    sex TEXT NOT NULL,
    sector TEXT NOT NULL,
    nationality TEXT NOT NULL,
    year INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

COMMENT ON TABLE ine_employment IS
'Employment indicators published by INE.';


-- =====================================================
-- EUSTAT ACTIVITY RATES
-- =====================================================
CREATE TABLE IF NOT EXISTS eustat_activity_nationality (
    id BIGSERIAL PRIMARY KEY,
    rate_type TEXT NOT NULL,
    province TEXT NOT NULL,
    nationality TEXT NOT NULL,
    quarter TEXT NOT NULL,
    year INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

COMMENT ON TABLE eustat_activity_nationality IS
'Labour activity rates by nationality published by EUSTAT.';


-- =====================================================
-- ELECTION RESULTS
-- =====================================================
CREATE TABLE IF NOT EXISTS elections_parlamento_vasco (
    id BIGSERIAL PRIMARY KEY,
    year TEXT,
    month TEXT,
    party_name TEXT,
    seats BIGINT
);

COMMENT ON TABLE elections_parlamento_vasco IS
'Basque Parliament election results by political party.';


-- =====================================================
-- HOUSING PRICES
-- =====================================================
CREATE TABLE IF NOT EXISTS housing_prices_annual (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    province TEXT NOT NULL,
    region_type TEXT,
    price_per_m2 NUMERIC(12,2) NOT NULL,
    source TEXT DEFAULT 'MIVAU/Eustat',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE housing_prices_annual IS
'Annual housing prices per square meter by province.';


-- =====================================================
-- POVERTY AND LIVING CONDITIONS
-- =====================================================
CREATE TABLE IF NOT EXISTS ine_poverty_stats (
    id BIGSERIAL PRIMARY KEY,
    level TEXT NOT NULL DEFAULT 'ccaa',
    territory TEXT NOT NULL,
    indicator TEXT NOT NULL,
    year INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

COMMENT ON TABLE ine_poverty_stats IS
'Poverty and living conditions indicators published by INE.';


-- =====================================================
-- INE PADRÓN — FOREIGN POPULATION BY PROVINCE
-- =====================================================
CREATE TABLE IF NOT EXISTS ine_padron_foreign (
    id                  BIGSERIAL PRIMARY KEY,
    province            TEXT NOT NULL,
    province_std        TEXT NOT NULL,
    year                INTEGER NOT NULL,
    foreign_population  INTEGER NOT NULL
);

COMMENT ON TABLE ine_padron_foreign IS
'Annual foreign population by province from the INE Padrón Continuo (1998-2022). Fills years missing from the EUSTAT nationality source.';

COMMENT ON COLUMN ine_padron_foreign.province IS
'Province name as published by INE.';

COMMENT ON COLUMN ine_padron_foreign.province_std IS
'Standardised province name for joins (Araba, Bizkaia, Gipuzkoa).';

COMMENT ON COLUMN ine_padron_foreign.year IS
'Reference year of the population count.';

COMMENT ON COLUMN ine_padron_foreign.foreign_population IS
'Number of registered foreign residents in the province.';


-- =====================================================
-- INE ECP — FOREIGN POPULATION BY PROVINCE (2021+)
-- =====================================================
CREATE TABLE IF NOT EXISTS ine_ecp_foreign (
    id                      BIGSERIAL PRIMARY KEY,
    province_std            TEXT NOT NULL,
    year                    INTEGER NOT NULL,
    foreign_population      INTEGER NOT NULL,
    total_population        INTEGER NOT NULL,
    foreign_population_pct  NUMERIC(6,2) NOT NULL
);

COMMENT ON TABLE ine_ecp_foreign IS
'Annual foreign and total population by province from the INE Estadística Continua de Población (2021-2025). Replaces the discontinued Padrón Continuo with broader estimation methodology.';

COMMENT ON COLUMN ine_ecp_foreign.province_std IS
'Standardised province name (Araba, Bizkaia, Gipuzkoa).';

COMMENT ON COLUMN ine_ecp_foreign.year IS
'Reference year of the population estimate.';

COMMENT ON COLUMN ine_ecp_foreign.foreign_population IS
'Estimated number of foreign residents in the province.';

COMMENT ON COLUMN ine_ecp_foreign.total_population IS
'Estimated total population of the province.';

COMMENT ON COLUMN ine_ecp_foreign.foreign_population_pct IS
'Foreign population as a percentage of total population.';
"""


# ======================================================
# TABLE CREATION
# ======================================================

def create_tables():
    """
    Executes the SQL schema creation script.
    """

    print("Connecting to Postgres/Supabase...")

    conn = get_conn()
    cur = conn.cursor()

    try:
        print("Creating tables and documentation...")
        cur.execute(SQL)
        conn.commit()
        print("Database schema created successfully.")

    finally:
        cur.close()
        conn.close()


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    create_tables()
