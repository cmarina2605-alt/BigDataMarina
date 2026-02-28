"""
EUSTAT activity by nationality download script (Selenium-based).

This script automates the download of activity rate data by nationality
from the EUSTAT PX-Web interface.

IMPORTANT NOTE:
Some parts of this script may look redundant or repetitive
(e.g. multiple selectors, repeated waits, fallback actions).
These parts are intentionally kept unchanged to improve robustness
when interacting with a dynamic and unstable web interface.
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from pathlib import Path
import time
import os


# ======================================================
# CONFIGURATION
# ======================================================

URL = "https://www.eustat.eus/bankupx/pxweb/en/DB/-/PX_050403_cpra_tab17.px"
OUT_NAME = "eustat_activity_nationality.csv"

BASE = Path("data_raw").resolve()
BASE.mkdir(exist_ok=True)


# ======================================================
# HELPER FUNCTIONS
# ======================================================

def wait_download(folder, ext=".csv", timeout=120):
    """
    Waits until a downloaded file appears in the given folder.

    This function checks the folder every second and returns
    the most recently modified file with the specified extension.

    The loop and timeout may appear redundant, but they are
    intentionally used to handle slow or delayed browser downloads.
    """
    for _ in range(timeout):
        files = list(Path(folder).glob(f"*{ext}"))
        if files:
            return max(files, key=os.path.getmtime)
        time.sleep(1)

    raise TimeoutError("The file was not downloaded within the timeout.")


def close_popup_if_present(driver, wait, max_attempts=4):
    """
    Attempts to close jQuery UI modal dialogs that may block the page.

    Multiple selectors and fallback mechanisms are used because
    the modal structure and buttons may change depending on
    the dataset or EUSTAT interface updates.

    This logic may look redundant, but it is intentionally kept
    to ensure the popup is closed in as many scenarios as possible.
    """

    for attempt in range(max_attempts):
        try:
            modal = WebDriverWait(driver, 8).until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, ".ui-dialog, .ui-dialog-titlebar")
                )
            )

            close_selectors = [
                ".ui-dialog-titlebar-close",
                "button.ui-dialog-titlebar-close",
                ".ui-button.ui-corner-all.ui-widget",
                "button.ui-button.ui-corner-all.ui-widget",
                ".ui-dialog-buttonpane button",
                "button:contains('Close')",
                "button:contains('OK')",
                "button:contains('Cerrar')",
                ".ui-dialog-titlebar .ui-icon-closethick"
            ]

            for sel in close_selectors:
                try:
                    btn = modal.find_element(By.CSS_SELECTOR, sel)
                    if btn.is_displayed() and btn.is_enabled():
                        driver.execute_script(
                            "arguments[0].click();", btn
                        )
                        time.sleep(1.2)
                        return True
                except NoSuchElementException:
                    continue

            # Fallback: send ESC key if no close button works
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(1.2)
            return True

        except TimeoutException:
            time.sleep(1.5)
            continue

    return False


# ======================================================
# MAIN EXECUTION
# ======================================================

def run():
    """
    Executes the full Selenium workflow to download the CSV file.

    The function:
    - Opens the PX-Web page
    - Selects all available filter values
    - Generates the data table
    - Handles blocking modal dialogs
    - Downloads the CSV file
    - Renames the downloaded file

    The use of multiple waits and retries is intentional and
    helps prevent intermittent failures.
    """

    print("\n====== EUSTAT DOWNLOAD ======")
    print("Destination folder:", BASE)

    options = Options()
    # options.add_argument("--headless=new")  # Optional headless execution

    prefs = {
        "download.default_directory": str(BASE),
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)

    # Required to allow file downloads via Chrome DevTools
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": str(BASE)}
    )

    wait = WebDriverWait(driver, 120)
    driver.get(URL)

    try:
        # ==================================
        # SELECT ALL FILTER VALUES
        # ==================================
        print("Selecting all filter values...")

        wait.until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR,
             "input.variableselector_valuesselect_select_all_button")
        ))

        buttons = driver.find_elements(
            By.CSS_SELECTOR,
            "input.variableselector_valuesselect_select_all_button"
        )

        for b in buttons:
            driver.execute_script("arguments[0].click();", b)
            time.sleep(0.2)

        # ==================================
        # GENERATE TABLE
        # ==================================
        print("Generating table...")

        show_btn = wait.until(EC.element_to_be_clickable((
            By.ID,
            "ctl00_ContentPlaceHolderMain_"
            "VariableSelector1_VariableSelector1_ButtonViewTable"
        )))

        driver.execute_script("arguments[0].click();", show_btn)
        time.sleep(2.5)

        # Attempt to close Footnotes modal dialogs
        for _ in range(5):
            if close_popup_if_present(driver, wait):
                break
            time.sleep(1.8)

        # ==================================
        # WAIT FOR TABLE TO APPEAR
        # ==================================
        print("Waiting for table to be visible...")

        table_locators = [
            (By.ID, "ctl00_ctl00_ContentPlaceHolderMain_cphMain_Table1_Table1_DataTable"),
            (By.CSS_SELECTOR, "[id*='Table1_DataTable']"),
            (By.CSS_SELECTOR, "[id*='Table1']"),
            (By.CSS_SELECTOR, "table[id*='DataTable'], table.k-grid-table"),
            (By.CSS_SELECTOR, ".pxweb-table, .k-grid, .table-responsive table"),
            (By.CSS_SELECTOR, "table")
        ]

        table_element = None
        for by, value in table_locators:
            try:
                table_element = wait.until(
                    EC.visibility_of_element_located((by, value))
                )
                break
            except TimeoutException:
                continue

        if table_element is None:
            driver.save_screenshot("debug_table_not_found.png")
            raise TimeoutException("Table could not be located.")

        # ==================================
        # DOWNLOAD CSV
        # ==================================
        print("Downloading CSV...")

        csv_btn = wait.until(EC.element_to_be_clickable((
            By.ID,
            "ctl00_ctl00_ContentPlaceHolderMain_"
            "ShortcutFileFileTypeCsvWithHeadingAndSemiColon"
        )))

        driver.execute_script("arguments[0].click();", csv_btn)
        time.sleep(10)

    finally:
        driver.quit()

    # ==================================
    # WAIT FOR FILE AND RENAME
    # ==================================

    latest = wait_download(BASE, ".csv")
    new_path = BASE / OUT_NAME
    os.replace(latest, new_path)

    print("Download completed:", new_path)


# ======================================================

if __name__ == "__main__":
    run()
