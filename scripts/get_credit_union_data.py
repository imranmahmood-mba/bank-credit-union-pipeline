"""
Script to navigate to a page, click the "Select All General Information" button,
click additional checkboxes, then click a download button that navigates to a new page.
After clicking the download button, the script waits until the download is complete.
"""

import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def main():
    # URL of the target page; update as needed
    url = (
        "https://webapps.ncua.gov/CustomQuery/Home/SelectAccount?"
        "SelectedCycle=09%2F2024&SelectedCuField0=Total%20Assets&"
        "SelectedCuField1=No%20More%20Criteria&SelectedCuField2=No%20More%20Criteria&"
        "SelectedCuField3=No%20More%20Criteria&SelectedCuField4=No%20More%20Criteria&"
        "SelectedOperator0=Greater%20Than%20or%20Equal%20to&SelectedOperator1=Equal%20To&"
        "SelectedOperator2=Equal%20To&SelectedOperator3=Equal%20To&SelectedOperator4=Equal%20To&"
        "AndOr1=True&AndOr2=True&AndOr3=True&AndOr4=True&Value0=1"
    )

    # Define a download directory; ensure this directory exists
    download_dir = os.path.abspath("downloads")
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # Configure Chrome options for automatic download
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_dir,  # set download folder
        "download.prompt_for_download": False,         # disable download prompt
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,                  # enable safe browsing
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Initialize the Chrome WebDriver with our options
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 10)  # Explicit wait

    try:
        # Step 1: Navigate to the initial page
        driver.get(url)
        print("Navigated to the initial page.")
        time.sleep(5)

        # Step 2: Click on the first button (adjust IDs as necessary)
        btn_all_agg_tot = wait.until(EC.element_to_be_clickable((By.ID, "btnAllGenInfo")))
        btn_all_agg_tot.click()
        print("Clicked on button with id 'btnAllGenInfo'.")

        # Step 3: Click on the checkbox with id "cbxAggTotalList_0"
        checkbox_agg_total = wait.until(EC.element_to_be_clickable((By.ID, "cbxAggTotalList_0")))
        checkbox_agg_total.click()
        print("Clicked on checkbox with id 'cbxAggTotalList_0'.")

        checkbox_agg_total = wait.until(EC.element_to_be_clickable((By.ID, "cbxAggTotalList_27")))
        checkbox_agg_total.click()
        print("Clicked on checkbox with id 'cbxAggTotalList_27'.")

        # Step 4: Click on the submit button with id "cmdSubmit" which navigates to a new page
        submit_button = wait.until(EC.element_to_be_clickable((By.ID, "cmdSubmit")))
        submit_button.click()
        print("Clicked on submit button with id 'cmdSubmit'. Navigating to the new page...")

        # Step 5: Wait for the new page to load by waiting for the div with id "linkDiv" to be present
        time.sleep(5)
        link_div = wait.until(EC.presence_of_element_located((By.ID, "linkDiv")))
        print("New page loaded; 'linkDiv' is present.")

        # Step 6: Click the download button inside "linkDiv"
        # Replace 'targetButtonId' with the actual id of your download button.
        first_link = link_div.find_element(By.CSS_SELECTOR, "a")

        first_link.click()
        print("Clicked on the download button.")

        # Optional pause to observe the result
        time.sleep(3)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the browser once done
        driver.quit()
        print("Browser closed.")


if __name__ == "__main__":
    main()
