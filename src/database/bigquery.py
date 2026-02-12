import pandas as pd
from google.cloud import bigquery
import os

class BigQueryClient:
    def __init__(self, project_id=None, credentials_path=None):
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        
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
        Optimized for appending data with automatic schema expansion.
        """
        try:
            # Determine the write disposition
            if if_exists == "replace":
                write_disposition = "WRITE_TRUNCATE"
                # Schema update options are NOT allowed with WRITE_TRUNCATE
                job_config = bigquery.LoadJobConfig(
                    write_disposition=write_disposition
                )
            else:
                write_disposition = "WRITE_APPEND"
                # ALLOW_FIELD_ADDITION is perfect for appending data when new columns appear
                job_config = bigquery.LoadJobConfig(
                    write_disposition=write_disposition,
                    schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
                )

            print(f"Uploading to {table_id} (Mode: {write_disposition})...")
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete
            print(f"Successfully uploaded {len(df)} rows to {table_id}.")
            return True
        except Exception as e:
            print(f"An error occurred during upload: {e}")
            return False

    def get_dior_data(self, dataset_id, table_id, limit=50):
        query = f"""
            SELECT
                url,
                title,
                COUNT(1) as num_occurrences,
                MAX(scrape_date) as last_scraped
            FROM
                `{self.project_id}.{dataset_id}.{table_id}`
            WHERE
                LENGTH(content) > 100
            GROUP BY
                url, title
            ORDER BY
                num_occurrences DESC
            LIMIT {limit};
        """
        return self.query_to_dataframe(query)
