import os
import pandas as pd
from google.cloud import bigquery

class BigQueryManager:
    def __init__(self, project_id="asli-api"):
        """
        Initialise le client BigQuery. 
        Docker se chargera de pointer vers le fichier JSON via GOOGLE_APPLICATION_CREDENTIALS.
        """
        self.project_id = project_id
        try:
            self.client = bigquery.Client(project=self.project_id)
            print(f"✅ Connecté au projet BigQuery : {self.project_id}")
        except Exception as e:
            print(f"❌ Erreur de connexion : {e}")

    def run_query(self, query):
        """Exécute une requête SQL et retourne un DataFrame."""
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            print(f"❌ Erreur lors de l'exécution de la requête : {e}")
            return pd.DataFrame()

    def save_to_bq(self, df, table_id):
        """Sauvegarde un DataFrame directement dans BigQuery."""
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Données envoyées vers {table_id}")