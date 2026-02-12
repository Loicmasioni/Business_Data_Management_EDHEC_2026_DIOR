from fastapi import FastAPI, BackgroundTasks, HTTPException
from src.scrapers.dior import DiorScraper
from src.scrapers.vestiaire import VestiaireScraper
from src.database.bigquery import BigQueryClient
from src.automation.scheduler import setup_daily_scheduler
from dotenv import load_dotenv
import pandas as pd
import os

# Load environment variables
load_dotenv()

app = FastAPI(title="Dior Data Management API")

def clean_for_json(df: pd.DataFrame):
    """
    Pandas dataframes can contain NaN, Inf, and -Inf which are not JSON compliant.
    This helper converts them to None.
    """
    # Replace Inf/-Inf with NaN, then NaN with None
    import numpy as np
    return df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)

# Initialize background scheduler for daily updates
setup_daily_scheduler(app)

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
    # Handle NaN for JSON compliance
    return clean_for_json(df).to_dict(orient="records")

# Note: Vestiaire scrape usually needs the Dior data as seed.
# In a real app, you might want to chain these or use a task queue like Celery.

@app.post("/pipeline/run")
async def run_pipeline(background_tasks: BackgroundTasks):
    """
    Triggers the full end-to-end analytical pipeline (Scrape -> Match -> Upload).
    """
    from run_pipeline import run_full_analytical_pipeline
    background_tasks.add_task(run_full_analytical_pipeline)
    return {"message": "Full analytical pipeline started in background"}

@app.get("/analytics/summary")
async def get_analytics_summary():
    """
    Returns high-level stats for Power BI or dashboard summaries.
    """
    bq = BigQueryClient()
    table = os.getenv("DIOR_TABLE_ID", "dior_data")
    dataset = os.getenv("BIGQUERY_DATASET_ID", "data_management_projet")
    full_path = f"{bq.project_id}.{dataset}.{table}"
    
    query = f"""
        SELECT 
            Source, 
            COUNT(*) as total_items, 
            AVG(SAFE_CAST(price AS FLOAT64)) as avg_price,
            MAX(scrape_date) as last_scraped
        FROM `{full_path}`
        GROUP BY Source
    """
    df = bq.query_to_dataframe(query)
    # Handle NaN for JSON compliance
    return clean_for_json(df).to_dict(orient="records")

@app.get("/analytics/investment-hotspots")
async def get_investment_hotspots(limit: int = 10):
    """
    Returns products with the highest value retention potential.
    """
    bq = BigQueryClient()
    table = os.getenv("DIOR_TABLE_ID", "dior_data")
    dataset = os.getenv("BIGQUERY_DATASET_ID", "data_management_projet")
    full_path = f"{bq.project_id}.{dataset}.{table}"
    
    # Logic: Products where we have both Dior and secondary markings
    # (This assumes the analytical mart logic has run and populated 'price' for matched items)
    query = f"""
        SELECT product_name, category, retail_price, price as resale_price, Source
        FROM `{full_path}`
        WHERE Source != 'Dior' AND price IS NOT NULL
        ORDER BY price DESC
        LIMIT {limit}
    """
    df = bq.query_to_dataframe(query)
    # Handle NaN for JSON compliance
    return clean_for_json(df).to_dict(orient="records")

@app.post("/scrape/vestiaire")
async def trigger_vestiaire_scrape(background_tasks: BackgroundTasks, dataset: str = "data_management_projet", table: str = None):
    """
    Trigger a Vestiaire scrape using Dior data from BigQuery as seeds.
    """
    if not table:
        table = os.getenv("DIOR_TABLE_ID", "dior_data")
        
    bq_client = BigQueryClient()
    # Simplified: fetch some Dior products to use as seeds
    query = f"SELECT product_name, retail_product_id, retail_price, category FROM `{bq_client.project_id}.{dataset}.{table}` LIMIT 10"
    df_dior = bq_client.query_to_dataframe(query)
    
    if df_dior.empty:
        raise HTTPException(status_code=404, detail="No Dior products found to seed Vestiaire scrape")
        
    background_tasks.add_task(vestiaire_scraper.scrape_all_from_df, df_dior)
    return {"message": "Vestiaire scrape triggered in background for 10 products"}
