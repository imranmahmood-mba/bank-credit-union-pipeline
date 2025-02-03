import pandas as pd
from datetime import datetime
from helper_functions import get_latest_file
from load_data.load_to_bucket import upload_file_to_gcs
from load_data.write_to_table import truncate_table, write_csv_to_big_query_table


def state_to_abbreviation(state_name: str) -> str:
    """
    Convert a U.S. state name to its two-letter postal abbreviation.

    Args:
        state_name (str): The full name of the state (e.g., "California").

    Returns:
        str: The two-letter postal abbreviation (e.g., "CA").
             Returns None if the state name is not recognized.
    """
    # Mapping dictionary: state names (title case) to two-letter codes.
    mapping = {
        'Alabama': 'AL',
        'Alaska': 'AK',
        'Arizona': 'AZ',
        'Arkansas': 'AR',
        'California': 'CA',
        'Colorado': 'CO',
        'Connecticut': 'CT',
        'Delaware': 'DE',
        'Florida': 'FL',
        'Georgia': 'GA',
        'Hawaii': 'HI',
        'Idaho': 'ID',
        'Illinois': 'IL',
        'Indiana': 'IN',
        'Iowa': 'IA',
        'Kansas': 'KS',
        'Kentucky': 'KY',
        'Louisiana': 'LA',
        'Maine': 'ME',
        'Maryland': 'MD',
        'Massachusetts': 'MA',
        'Michigan': 'MI',
        'Minnesota': 'MN',
        'Mississippi': 'MS',
        'Missouri': 'MO',
        'Montana': 'MT',
        'Nebraska': 'NE',
        'Nevada': 'NV',
        'New Hampshire': 'NH',
        'New Jersey': 'NJ',
        'New Mexico': 'NM',
        'New York': 'NY',
        'North Carolina': 'NC',
        'North Dakota': 'ND',
        'Ohio': 'OH',
        'Oklahoma': 'OK',
        'Oregon': 'OR',
        'Pennsylvania': 'PA',
        'Rhode Island': 'RI',
        'South Carolina': 'SC',
        'South Dakota': 'SD',
        'Tennessee': 'TN',
        'Texas': 'TX',
        'Utah': 'UT',
        'Vermont': 'VT',
        'Virginia': 'VA',
        'Washington': 'WA',
        'West Virginia': 'WV',
        'Wisconsin': 'WI',
        'Wyoming': 'WY'
    }

    # Normalize the input state name by removing extra spaces
    # and converting to title case (e.g., " texas " -> "Texas").
    normalized_state = state_name.strip().title()

    # Return the abbreviation if it exists, or None if not found.
    return mapping.get(normalized_state, None)


def format_bank_dim_data(input_file_path: str, output_file_path: str):
    """Formats the csv containing the bank's dimensional data

    Args:
        input_file_path (str): file path to csv containing the bank
        dimensional data
        output_file_path (str): file path to save csv containing the formatted
        bank dimensional data

    """
    df = pd.read_csv(input_file_path, index_col=False).drop(['Unnamed: 0'],
                                                            axis=1)
    df.rename({'STNAME': 'state', 'WEBADDR': 'url', 'CITY': 'city',
               'ID': 'charter_id', 'NAME': 'name'}, inplace=True, axis=1)
    # Abbreviate state names.
    df['state'] = df['state'].apply(state_to_abbreviation)
    # Save to new file
    df.to_csv(output_file_path)


def format_bank_fact_data(input_file_path: str, output_file_path: str):
    """Formats the csv containing the bank's fact data

    Args:
        input_file_path (str): file path to csv containing the bank fact data
        output_file_path (str): file path to save csv containing the formatted
        bank fact data
    """
    df = pd.read_csv(input_file_path, index_col=False).drop(['Unnamed: 0'],
                                                            axis=1)
    df.rename(columns={'REPDTE': 'date', 'ID': 'charter_id', 'ASSET': 'assets',
                       'DEP': 'deposits', 'NAME': 'name'}, inplace=True)

    # Format date
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    df['month'] = df['date'].dt.strftime('%m')
    df['year'] = df['date'].dt.strftime('%Y')
    df.drop('date', inplace=True, axis=1)
    # Remove date suffixed to charter ID
    df['charter_id'] = df['charter_id'].astype(str)
    df['charter_id'] = df['charter_id'].str.split('_').str[0]
    # Replace nulls with 0
    df[['assets', 'deposits']].fillna(0, inplace=True)
    # Save to new file
    df.to_csv(output_file_path)


def main():
    CURRENT_TIMESTAMP = datetime.now().timestamp() # create timestamp to append to file name
    BUCKET_NAME = 'alpha-rank-ai-bucket'
    BANK_DIM_OUTPUT_PATH = f'formatted_bank_dim_data_{CURRENT_TIMESTAMP}.csv'
    BANK_FACT_OUTPUT_PATH = f'formatted_bank_fact_data_{CURRENT_TIMESTAMP}.csv'
    LATEST_BANK_DIM_FILE_PATH = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai', 'bank_dim_data*.csv')
    LATEST_BANK_FACT_FILE_PATH = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai', 'bank_fact_data*.csv')
    LATEST_FORMATTED_BANK_DIM_FILE_PATH = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai', 'formatted_bank_dim_data.csv*.csv')
    LATEST_FORMATTED_BANK_FACT_FILE_PATH = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai', 'formatted_bank_fact_data.csv*.csv')

    # Format the data files
    format_bank_dim_data(LATEST_BANK_DIM_FILE_PATH,
                         BANK_DIM_OUTPUT_PATH)
    format_bank_fact_data(LATEST_BANK_FACT_FILE_PATH,
                          BANK_FACT_OUTPUT_PATH)
    # Upload files to GCS bucket to persist files
    upload_file_to_gcs(BUCKET_NAME, LATEST_FORMATTED_BANK_DIM_FILE_PATH, BANK_DIM_OUTPUT_PATH)
    upload_file_to_gcs(BUCKET_NAME, LATEST_FORMATTED_BANK_FACT_FILE_PATH, BANK_FACT_OUTPUT_PATH)

    # Truncate bank dim data staging table before repopulating
    truncate_table('alpha-rank-ai.financial_institutions.dim_banks_staging')
    write_csv_to_big_query_table('alpha-rank-ai.financial_institutions.dim_banks_staging', 
                                 f'gs://{BUCKET_NAME}/{LATEST_FORMATTED_BANK_DIM_FILE_PATH}',
                                 autodetect=False)

    # Truncate bank fact data staging table before repopulating
    truncate_table('alpha-rank-ai.financial_institutions.fact_banks_staging')
    write_csv_to_big_query_table('alpha-rank-ai.financial_institutions.fact_banks_staging',
                                 f'gs://{BUCKET_NAME}/{LATEST_FORMATTED_BANK_FACT_FILE_PATH}',
                                 autodetect=False)

if __name__ == '__main__':
    main()
