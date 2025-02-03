from google.cloud import bigquery
from helper_functions import get_latest_file

# Initialize BigQuery client
client = bigquery.Client()


def write_csv_to_big_query_table(table_id: str, gcs_uri: str,
                                 client=client,
                                 autodetect=True):
    # Configure job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row if present
        autodetect=autodetect,  # Let BigQuery infer schema
    )

    # Load data from GCS to BigQuery
    load_job = client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()

    # Print confirmation
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")


def truncate_table(table_id, client=client):
    """
    Truncate a BigQuery table by deleting all rows from it.

    Args:
        table_id (str): table name in following format: "your-project-id.your-dataset-id.your-table-id"

    Returns:
        None
    """

    # SQL query to truncate the table
    query = f"TRUNCATE TABLE `{table_id}`"

    # Execute the query
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete

    print(f"Table {table_id} has been truncated.")


def fact_banks_merge_staging_to_main(client=client):
    # Merge is used to keep old data and only add in or update new data
    query = """MERGE `alpha-rank-ai.financial_institutions.fact_banks` AS main
                USING (
                  SELECT
                    distinct
                    s.month month,
                    s.year year,
                    s.assets,
                    cast(s.deposits as numeric) as deposits,
                    s.charter_id,
                  FROM `alpha-rank-ai.financial_institutions.fact_banks_staging` AS s
                ) AS staging
                ON main.charter_id = staging.charter_id and main.month = staging.month and main.year = staging.month
                WHEN NOT MATCHED THEN
                  INSERT (
                    month,
                    year,
                    assets,
                    deposits,
                    charter_id
                  )
                  VALUES (
                    staging.month,
                    staging.year,
                    staging.assets,
                    staging.deposits,
                    staging.charter_id
                  );
"""

    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete


def dim_banks_merge_staging_to_main(client=client):
    # Merge is used to keep old data and only add in or update new data
    query = """MERGE `alpha-rank-ai.financial_institutions.dim_banks` AS main
USING (
  SELECT
    distinct
    s.city,
    s.url,
    s.state,
    s.name,
    s.charter_id,
  FROM `alpha-rank-ai.financial_institutions.dim_banks_staging` AS s
) AS staging
ON main.charter_id = staging.charter_id and main.name = staging.name and main.city = staging.city and staging.url = main.url and main.state = staging.state and main.name = staging.name
WHEN NOT MATCHED THEN
  INSERT (
    city,
    url,
    state,
    name,
    charter_id
  )
  VALUES (
    staging.city,
    staging.url,
    staging.state,
    staging.name,
    staging.charter_id
  );
"""

    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete


def fact_credit_unions_merge_staging_to_main(client=client):
    # Merge is used to keep old data and only add in or update new data
    query = """MERGE `alpha-rank-ai.financial_institutions.fact_credit_unions` AS main
USING (
  SELECT
    distinct
    s.year,
    s.month,
    s.assets as assets,
    s.deposits as deposits,
    s.charter_id as charter_id,
  FROM `alpha-rank-ai.financial_institutions.fact_credit_unions_staging` AS s
) AS staging
ON main.charter_id = staging.charter_id and main.year = staging.year and main.month = staging.month
WHEN NOT MATCHED THEN
  INSERT (
    year,
    month,
    assets,
    deposits,
    charter_id
  )
  VALUES (
    staging.year,
    staging.month,
    staging.assets,
    staging.deposits,
    staging.charter_id
  );
"""

    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete


def dim_credit_unions_merge_staging_to_main(client=client):
    # Merge is used to keep old data and only add in or update new data
    query = """MERGE `alpha-rank-ai.financial_institutions.dim_credit_unions` AS main
USING (
  SELECT
    distinct
    s.city,
    s.url,
    s.state,
    s.name,
    s.charter_id,
  FROM `alpha-rank-ai.financial_institutions.dim_credit_unions_staging` AS s
) AS staging
ON main.charter_id = staging.charter_id and main.city = staging.city and main.url = staging.url and main.state = staging.state and main.name = staging.name
WHEN NOT MATCHED THEN
  INSERT (
    city,
    url,
    state,
    name,
    charter_id
  )
  VALUES (
    staging.city,
    staging.url,
    staging.state,
    staging.name,
    staging.charter_id
  );
"""

    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete


def main():
    # Populate main data tables with new data
    fact_banks_merge_staging_to_main()
    dim_banks_merge_staging_to_main()
    fact_credit_unions_merge_staging_to_main()
    dim_credit_unions_merge_staging_to_main()


if __name__ == '__main__':
    main()
