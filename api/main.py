from fastapi import FastAPI, BackgroundTasks, HTTPException
from src.scrapers.dior import DiorScraper
from src.scrapers.vestiaire import VestiaireScraper
from src.database.bigquery import BigQueryClient
import pandas as pd
import os

app = FastAPI(title="Dior Data Management API")

# Initialize clients (can also be done per request or via dependency injection)
dior_scraper = DiorScraper(headless=True)
vestiaire_scraper = VestiaireScraper(headless=True)
# project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "asli-api")
# bq_client = BigQueryClient(project_id=project_id)

@app.get("/")
async def root():
    return {"message": "Dior Data Management API is running"}

@app.post("/scrape/dior")
async def trigger_dior_scrape(background_tasks: BackgroundTasks, categories: dict = None):
    """
    Trigger a Dior scrape. If no categories are provided, defaults are used.
    """
    if not categories:
        categories = {
            "Bags": "https://www.dior.com/fr_fr/fashion/mode-homme/sacs/tous-les-sacs",
            "Ready-to-Wear": "https://www.dior.com/fr_fr/fashion/mode-homme/pret-a-porter/tout-le-pret-a-porter",
        }
    
    background_tasks.add_task(dior_scraper.scrape_all, categories)
    return {"message": "Dior scrape triggered in background", "categories": list(categories.keys())}

@app.get("/data/dior")
async def get_dior_data(dataset: str = "data_management_projet", table: str = "dior_data", limit: int = 50):
    """
    Fetch Dior data from BigQuery.
    """
    # This requires GOOGLE_APPLICATION_CREDENTIALS to be set
    bq_client = BigQueryClient() 
    df = bq_client.get_dior_data(dataset, table, limit)
    if df.empty:
        return {"message": "No data found or error occurred"}
    return df.to_dict(orient="records")

# Note: Vestiaire scrape usually needs the Dior data as seed.
# In a real app, you might want to chain these or use a task queue like Celery.

@app.post("/scrape/vestiaire")
async def trigger_vestiaire_scrape(background_tasks: BackgroundTasks, dataset: str = "data_management_projet", table: str = "dior_data"):
    """
    Trigger a Vestiaire scrape using Dior data from BigQuery as seeds.
    """
    bq_client = BigQueryClient()
    # Simplified: fetch some Dior products to use as seeds
    query = f"SELECT product_name, retail_product_id, retail_price, category FROM `{bq_client.project_id}.{dataset}.{table}` LIMIT 10"
    df_dior = bq_client.query_to_dataframe(query)
    
    if df_dior.empty:
        raise HTTPException(status_code=404, detail="No Dior products found to seed Vestiaire scrape")
        
    background_tasks.add_task(vestiaire_scraper.scrape_all_from_df, df_dior)
    return {"message": "Vestiaire scrape triggered in background for 10 products"}
