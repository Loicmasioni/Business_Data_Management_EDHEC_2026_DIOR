import pandas as pd
from google.cloud import bigquery
import os

class BigQueryClient:
    def __init__(self, project_id=None, credentials_path=None):
        # Prefer provided credentials_path, otherwise check environment
        cred_file = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_file:
            # Ensure it's an absolute path if it exists locally
            if not os.path.isabs(cred_file) and os.path.exists(cred_file):
                cred_file = os.path.abspath(cred_file)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_file
        
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.client = bigquery.Client(project=self.project_id)

    def query_to_dataframe(self, query):
        """
        Runs a SQL query and returns the results as a Pandas DataFrame.
        """
        try:
            print(f"Running query on project: {self.project_id}...")
            df = self.client.query(query).to_dataframe()
            print("Query complete!")
            return df
        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()

    def upload_dataframe(self, df, table_id, if_exists="append"):
        """
        Uploads a Pandas DataFrame to a BigQuery table.
        """
        try:
            # Table ID should be in the format 'dataset.table' or 'project.dataset.table'
            write_disposition = "WRITE_APPEND" if if_exists == "append" else "WRITE_TRUNCATE"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=True, # Automatically detect schema from DataFrame
            )
            
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete
            print(f"Successfully uploaded {len(df)} rows to {table_id}.")
            return True
        except Exception as e:
            print(f"An error occurred during upload: {e}")
            return False

    def get_recent_data(self, dataset_id, table_id, limit=50):
        """
        Generic method to fetch recent data from a table.
        """
        query = f"""
            SELECT *
            FROM `{self.project_id}.{dataset_id}.{table_id}`
            ORDER BY scrape_date DESC
            LIMIT {limit}
        """
        return self.query_to_dataframe(query)

    def get_dior_data(self, dataset_id, table_id, limit=50):
        """
        Fetches Dior specific data using the current scraper schema.
        """
        query = f"""
            SELECT
                product_name,
                retail_product_id,
                retail_price,
                category,
                scrape_date,
                product_url
            FROM
                `{self.project_id}.{dataset_id}.{table_id}`
            ORDER BY
                scrape_date DESC
            LIMIT {limit};
        """
        return self.query_to_dataframe(query)

    def save_to_bq(self, df, table_id):
        """
        Alias for upload_dataframe to match test_main.py.
        """
        return self.upload_dataframe(df, table_id)

class BigQueryManager(BigQueryClient):
    """
    Alias class to match test_main.py import.
    """
    pass
