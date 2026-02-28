"""
Full database wipe script.

This script completely deletes the public schema and recreates it.
USE WITH EXTREME CAUTION.

All tables, data, functions, views and constraints inside the
public schema will be permanently removed.
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
# FULL RESET SQL
# ======================================================

RESET_SQL = """
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;
"""


# ======================================================
# EXECUTION
# ======================================================

def reset_database():
    """
    Completely wipes the public schema.
    """

    print("Connecting to Postgres/Supabase...")
    conn = get_conn()
    cur = conn.cursor()

    try:
        print("⚠ Resetting database...")
        cur.execute(RESET_SQL)
        conn.commit()
        print("Database wiped successfully.")

    except Exception as e:
        conn.rollback()
        print("Error while resetting database:", e)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    confirm = input(
        "This will DELETE ALL DATA in the public schema.\n"
        "Type 'Y' to continue: "
    )

    if confirm == "Y":
        reset_database()
    else:
        print("Operation cancelled.")
