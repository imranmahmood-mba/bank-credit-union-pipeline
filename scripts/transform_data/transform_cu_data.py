import pandas as pd
from datetime import datetime
from helper_functions import get_latest_file
from load_data.load_to_bucket import upload_file_to_gcs
from load_data.write_to_table import truncate_table, write_csv_to_big_query_table

def transform_fact_data(input_file_path: str, output_file_path: str):
    """Formats the csv containing the cu's fact data

    Args:
        input_file_path (str): file path to csv containing the cu fact data
        output_file_path (str): file path to save csv containing the formatted cu fact data
    """
    cu_fact_data_df = pd.read_csv(input_file_path, index_col=False)
    # Replace nan with 0
    cu_fact_data_df[['assets', 'deposits']].fillna(0, inplace=True)
    # Save formatted data to new file
    cu_fact_data_df.to_csv(output_file_path)


def transform_dim_data(input_file_path, output_file_path):
    """Formats the csv containing the cu's dimensional data

    Args:
        input_file_path (str): file path to csv containing the cu dimensional
        data
        output_file_path (str): file path to save csv containing the formatted
        cu dimensional data

    """
    cu_dim_data_df = pd.read_csv(input_file_path, index_col=False)
    # Rename columns
    cu_dim_data_df.rename(columns={'CUNumber': 'charter_id', 'CUName': 'name',
                                   'City': 'city', 'State': 'state',
                                   'URL': 'url'},
                          inplace=True)
    # Save formatted data to new file
    cu_dim_data_df.to_csv(output_file_path)


def main():
    CURRENT_TIMESTAMP = datetime.now().timestamp()
    BUCKET_NAME = 'alpha-rank-ai-bucket'
    CU_DIM_OUTPUT_PATH = f'formatted_cu_dim_data_{CURRENT_TIMESTAMP}.csv'
    CU_FACT_OUTPUT_PATH = f'formatted_cu_fact_data_{CURRENT_TIMESTAMP}.csv'
    LATEST_CU_DIM_FILE_PATH = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai', 'cu_dim_data*.csv')
    LATEST_CU_FACT_FILE_PATH = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai', 'cu_fact_data*.csv')
    LATEST_FORMATTED_CU_DIM_FILE_PATH = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai', 'formatted_cu_dim_data.csv*.csv')
    LATEST_FORMATTED_CU_FACT_FILE_PATH = get_latest_file('/Users/imranmahmood/Projects/alpha-rank-ai', 'formatted_cu_fact_data.csv*.csv')

    # Format the data files
    transform_dim_data(LATEST_CU_DIM_FILE_PATH,
                       CU_DIM_OUTPUT_PATH)
    transform_fact_data(LATEST_CU_FACT_FILE_PATH,
                        CU_FACT_OUTPUT_PATH)

    # Upload files to GCS bucket to persist files
    upload_file_to_gcs(BUCKET_NAME, LATEST_FORMATTED_CU_DIM_FILE_PATH,
                       CU_DIM_OUTPUT_PATH)
    upload_file_to_gcs(BUCKET_NAME, LATEST_FORMATTED_CU_FACT_FILE_PATH,
                       CU_FACT_OUTPUT_PATH)

    # Truncate cu dim data staging table before repopulating
    truncate_table('alpha-rank-ai.financial_institutions.dim_credit_unions_staging')
    write_csv_to_big_query_table('alpha-rank-ai.financial_institutions.dim_credit_unions_staging', 
                                 f'gs://{BUCKET_NAME}/{LATEST_FORMATTED_CU_DIM_FILE_PATH}',
                                 autodetect=False)

    # Truncate cu fact data staging table before repopulating
    truncate_table('alpha-rank-ai.financial_institutions.fact_credit_unions_staging')
    write_csv_to_big_query_table('alpha-rank-ai.financial_institutions.fact_credit_unions_staging',
                                 f'gs://{BUCKET_NAME}/{LATEST_FORMATTED_CU_FACT_FILE_PATH}',
                                 autodetect=False)


if __name__ == '__main__':
    main()
