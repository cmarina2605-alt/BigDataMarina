"""
Quick connectivity test for the Supabase PostgreSQL database.

Executes a trivial SELECT 1 query to verify that the credentials
in the .env file are correct and the database is reachable.
"""

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def test_connection():
    """Open a connection, run SELECT 1, and print the result."""
    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        dbname=os.getenv("DB_NAME"),
    )

    cur = conn.cursor()
    cur.execute("SELECT 1;")
    print("Connection OK:", cur.fetchone())

    cur.close()
    conn.close()


if __name__ == "__main__":
    test_connection()
