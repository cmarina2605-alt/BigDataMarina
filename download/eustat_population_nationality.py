"""
EUSTAT population by nationality download script (Selenium-based).

This script uses Selenium to automate the download of population data
by nationality from the EUSTAT PX-Web interface.

The use of Selenium is required because EUSTAT does not provide a stable
API or direct download endpoint for this dataset, and manual interaction
is normally required via the web interface.
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from pathlib import Path

import glob
import os
import time


# ======================================================
# CONFIGURATION
# ======================================================

URL = "https://www.eustat.eus/bankupx/pxweb/en/DB/-/PX_050407_cempa_empa_pa16.px"

BASE = Path("data_raw").resolve()
BASE.mkdir(exist_ok=True)


# ======================================================
# MAIN EXECUTION
# ======================================================

def run():
    """
    Executes the Selenium workflow to download population data by nationality.

    The function:
    - Opens the PX-Web dataset page
    - Selects all available values for each variable
    - Generates the data table
    - Downloads the CSV file
    - Renames the downloaded file to a stable filename

    Some steps may appear redundant, but they are intentionally kept
    to ensure the script works reliably with the EUSTAT web interface.
    """

    print("\n====== EUSTAT NATIONALITY DOWNLOAD ======")
    print("Download directory:", BASE)

    options = Options()

    # Headless mode is disabled because file downloads may fail
    # options.add_argument("--headless=new")

    prefs = {
        "download.default_directory": str(BASE),
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)

    # Allow file downloads via Chrome DevTools Protocol
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": str(BASE)}
    )

    wait = WebDriverWait(driver, 120)

    driver.get(URL)

    try:
        # --------------------------------------------------
        # WAIT FOR VARIABLE SELECTOR
        # --------------------------------------------------
        print("Waiting for variable selector...")
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@value='Select all']")
            )
        )

        # --------------------------------------------------
        # SELECT ALL VALUES FOR EACH VARIABLE
        # --------------------------------------------------
        print("Clicking 'Select all' buttons...")

        buttons = driver.find_elements(
            By.XPATH,
            "//input[@value='Select all']"
        )

        for b in buttons:
            driver.execute_script("arguments[0].click();", b)
            time.sleep(0.5)

        # --------------------------------------------------
        # GENERATE TABLE
        # --------------------------------------------------
        print("Generating table...")
        show_btn = driver.find_element(
            By.ID,
            "ctl00_ContentPlaceHolderMain_"
            "VariableSelector1_VariableSelector1_ButtonViewTable"
        )
        driver.execute_script("arguments[0].click();", show_btn)

        # Wait until the URL indicates table view
        wait.until(lambda d: "tableViewLayout2" in d.current_url)
        print("Table is ready")

        # --------------------------------------------------
        # DOWNLOAD CSV
        # --------------------------------------------------
        print("Downloading CSV...")

        csv_btn = wait.until(
            EC.element_to_be_clickable((
                By.ID,
                "ctl00_ctl00_ContentPlaceHolderMain_"
                "ShortcutFileFileTypeCsvWithHeadingAndSemiColon"
            ))
        )

        driver.execute_script("arguments[0].click();", csv_btn)
        time.sleep(10)

        print("Download completed")

    finally:
        driver.quit()

    # --------------------------------------------------
    # WAIT FOR FILE AND RENAME
    # --------------------------------------------------
    for _ in range(120):
        files = glob.glob(str(BASE / "*.csv"))
        if files:
            latest = max(files, key=os.path.getmtime)

            new_name = BASE / "eustat_population_nationality.csv"
            os.replace(latest, new_name)

            print("Renamed to:", new_name)
            break

        time.sleep(1)
    else:
        raise TimeoutError("CSV file was not downloaded")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    run()
