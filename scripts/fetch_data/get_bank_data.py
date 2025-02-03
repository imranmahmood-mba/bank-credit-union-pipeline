import requests
import math
import pandas as pd
from datetime import datetime


def get_number_of_institutions():
    """Gets the number of institutions available in the dataset

    Returns:
        int: number of institutions
    """
    try:
        url = "https://banks.data.fdic.gov/api/institutions"
        response = requests.get(url)
        return response.json()['meta']['total']
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve the total number of institutions: {e}") from e


def get_institution_data_json(url: str, limit: int, offset: int, *fields: str) -> dict:
    """
    Retrieve institution data from the API with a given limit and offset, including specified fields.

    This function builds the API request using the 'params' argument, which helps
    properly encode query parameters. It also checks for HTTP errors and ensures the
    returned content is valid JSON.

    Args:
        url (str): Base URL for the API.
        limit (int): Maximum number of institutions to return. (API limit is 10k)
        offset (int): The offset from which to start returning records.
        *fields (str): Variable number of field names to include in the API call.

    Returns:
        dict: The JSON response from the API.

    Raises:
        RuntimeError: If the API request fails or the JSON cannot be decoded.
    """
    params = {
        "fields": ",".join(fields),  # Join fields with commas
        "limit": limit,
        "offset": offset
    }

    try:
        # Create response
        response = requests.get(url, params=params)

        # Raise an exception if the HTTP request returned an unsuccessful status code.
        response.raise_for_status()
    except requests.RequestException as e:
        # Provide a clear error message including the URL and parameters used.
        raise RuntimeError(f"Error fetching data from {url} with params {params}: {e}") from e

    try:
        # Return the response parsed as JSON.
        return response.json()
    except ValueError as e:
        # Raise an error if JSON decoding fails.
        raise RuntimeError("Error decoding JSON response") from e


def get_bank_dim_data(number_of_institutions: int) -> list:
    """
    Retrieve all bank dimension data from the FDIC API in chunks.

    This function performs the following steps:
      1. Retrieves the total number of institutions.
      2. Computes the number of iterations required using math.ceil so that all records
         are covered in batches of 10,000.
      3. Iterates over the required pages, fetching a chunk of data each time
         using the get_institution_data_json function.
      4. Accumulates and returns all fetched data as a list of dictionaries.

    Returns:
        list: A list where each element is a dictionary containing data from one chunk of the API.

    Raises:
        RuntimeError: If any data chunk retrieval encounters an error.
    """
    base_url = "https://banks.data.fdic.gov/api/institutions"

    # Step 1: Determine the number of iterations required
    iterations = math.ceil(number_of_institutions / 10000)

    all_data = []

    # Step 2: Iterate over each page and fetch data.
    for i in range(1, iterations + 1):
        # Calculate the offset for the current chunk. This allows you to paginate through the data
        offset = (i - 1) * 10000
        data_chunk = {'data': []}

        try:
            data_chunk = get_institution_data_json(base_url, 10000, offset,
                                                   'WEBADDR', 'NAME', 'CITY',
                                                   'STNAME')
            print(f"Fetched chunk {i} (records {offset} to {offset + 10000})")
            for data in data_chunk.get('data', []):
                print(data)
                all_data.append(data['data'])
            i += 1
        except Exception as e:
            print(f"Warning: Failed to fetch data chunk {i} (offset {offset}): {e}")
            return all_data
    # Step 3: Return the list of all data chunks.
    return all_data


def get_bank_fact_data(number_of_institutions: int) -> list:
    """
    Retrieve all bank dimension data from the FDIC API in chunks.

    This function performs the following steps:
      1. Retrieves the total number of institutions.
      2. Computes the number of iterations required using math.ceil so that all records
         are covered in batches of 10,000.
      3. Iterates over the required pages, fetching a chunk of data each time
         using the get_institution_data_json function.
      4. Accumulates and returns all fetched data as a list of dictionaries.

    Returns:
        list: A list where each element is a dictionary containing data from one chunk of the API.

    Raises:
        RuntimeError: If any data chunk retrieval encounters an error.
    """
    base_url = "https://banks.data.fdic.gov/api/financials"

    all_data = []
    i = 1 # Used to calclate offset to paginate thru data
    while True:
        # Calculate the offset for the current chunk. This allows you to paginate through the data
        offset = (i - 1) * 10000
        data_chunk = {'data': []}

        try:
            data_chunk = get_institution_data_json(base_url, 10000, offset,
                                                   'DEP', 'ASSET', 'REPDTE')
            print(f"Fetched chunk {i} (records {offset} to {offset + 10000})")
            for data in data_chunk.get('data', []):
                all_data.append(data['data'])
            i += 1
        except Exception as e:
            print(f"Warning: Failed to fetch data chunk {i} (offset {offset}): {e}")
            return all_data

    return all_data


def main():
    print("Running main")
    CURRENT_TIMESTAMP = datetime.now().timestamp()
    number_of_institutions = get_number_of_institutions()

    bank_dim_data = get_bank_dim_data(number_of_institutions)
    bank_dim_df = pd.DataFrame(bank_dim_data)
    bank_dim_df.to_csv(f'bank_dim_data_{CURRENT_TIMESTAMP}.csv')

    bank_fact_data = get_bank_fact_data(number_of_institutions)
    bank_fact_df = pd.DataFrame(bank_fact_data)
    bank_fact_df.to_csv(f'bank_fact_data_{CURRENT_TIMESTAMP}.csv')


if __name__ == '__main__':
    main()
