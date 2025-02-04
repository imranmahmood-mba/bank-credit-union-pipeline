"""
Script to navigate to a page, click the "Select All General Information" button,
click additional checkboxes, then click a download button that navigates to a new page.
After clicking the download button, the script waits until the download is complete.
"""

import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from helper_functions import get_latest_file


def get_credit_union_data(month='09', year='2024'):
    # URL of the target page; update as needed
    url = (
        "https://webapps.ncua.gov/CustomQuery/Home/SelectAccount?"
        f"SelectedCycle={month}%2F{year}&SelectedCuField0=Total%20Assets&"
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
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        # Disable Chrome's PDF viewer to force download:
        "plugins.always_open_pdf_externally": True,
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
        time.sleep(3)
        first_link = link_div.find_element(By.CSS_SELECTOR, "a")
        download_url = first_link.get_attribute('href')
        print("Extracted Download URL:", download_url)

        # Step 7: Navigate to the download URL
        driver.get(download_url)
        print("Navigated to the download URL.")
        time.sleep(5)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        driver.quit()
        print("Browser closed.")


# Read the Excel file into a DataFrame
def prepare_data(file_path, month='09', year='2024'):
    dim_sheet_name = 'ProfileGenInfo'
    fact_sheet_name = 'Total Accounts'
    df_dim_sheet = pd.read_excel(file_path,
                                 sheet_name=dim_sheet_name)
    df_dim_sheet = df_dim_sheet[['CUNumber', 'CUName', 'City', 'State', 'URL']]
    df_fact_sheet = pd.read_excel(file_path,
                                  sheet_name=fact_sheet_name).rename(columns={'Charter': 'charter_id', '010': 'assets', 'AS0009': 'deposits'})
    df_fact_sheet['year'] = int(year)
    df_fact_sheet['month'] = int(month)
    return (df_dim_sheet, df_fact_sheet)


def main():
    # month and year are used to determine what quarter to get data for
    month = '09'
    year = '2024'
    CURRENT_TIMESTAMP = datetime.now().timestamp() # used to append to file
    get_credit_union_data(month, year) # get the credit union data using selenium
    latest_file = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai/downloads/', '*.xlsx') # get the excel file to process
    df_dim_sheet, df_fact_sheet = prepare_data(latest_file) # process data
    df_dim_sheet.to_csv(f'cu_dim_data_{CURRENT_TIMESTAMP}.csv', index=False)
    df_fact_sheet.to_csv(f'cu_fact_data_{CURRENT_TIMESTAMP}.csv', index=False)


if __name__ == "__main__":
    main()
