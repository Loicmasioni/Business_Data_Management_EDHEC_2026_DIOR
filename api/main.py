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
    return {"message": "Dior Data Management API is running", "docs": "/docs"}

@app.get("/health")
async def health_check():
    """
    Health check endpoint for cloud deployments (e.g., GKE, Cloud Run).
    """
    return {"status": "healthy"}

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
        SELECT product_name, category, retail_price, price as resale_price, Source, scrape_date
        FROM `{full_path}`
        WHERE Source != 'Dior' AND price IS NOT NULL
        ORDER BY scrape_date DESC, price DESC
        LIMIT {limit}
    """
    df = bq.query_to_dataframe(query)
    return clean_for_json(df).to_dict(orient="records")

@app.get("/tools/exchange-rate")
async def get_exchange_rate(base: str = "USD", target: str = "EUR"):
    """
    Fetches the latest exchange rate from ExchangeRate-API.
    """
    import httpx
    api_key = os.getenv("EXCHANGE_RATE_API_KEY")
    if not api_key:
        return {"error": "API Key not configured"}
        
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base}"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            data = response.json()
            if response.status_code != 200:
                 return {"error": "External API error", "details": data}
            
            rate = data.get("conversion_rates", {}).get(target)
            if not rate:
                return {"error": f"Target currency {target} not found"}
                
            return {
                "base": base,
                "target": target,
                "rate": rate,
                "provider": "ExchangeRate-API"
            }
        except Exception as e:
             return {"error": str(e)}

@app.get("/analytics/brand-premium")
async def get_brand_premium():
    """
    Calculates the 'Dior Premium' (or Depreciation) by Category.
    Compares average Retail Price vs. Average Resale Price.
    """
    bq = BigQueryClient()
    table = os.getenv("DIOR_TABLE_ID", "dior_data")
    dataset = os.getenv("BIGQUERY_DATASET_ID", "data_management_projet")
    full_path = f"{bq.project_id}.{dataset}.{table}"

    query = f"""
        SELECT 
            category,
            AVG(CASE WHEN Source = 'Dior' THEN retail_price END) as avg_retail,
            AVG(CASE WHEN Source != 'Dior' THEN price END) as avg_resale,
            (AVG(CASE WHEN Source != 'Dior' THEN price END) - AVG(CASE WHEN Source = 'Dior' THEN retail_price END)) / AVG(CASE WHEN Source = 'Dior' THEN retail_price END) * 100 as premium_pct
        FROM `{full_path}`
        WHERE price IS NOT NULL OR retail_price IS NOT NULL
        GROUP BY category
        HAVING avg_retail IS NOT NULL AND avg_resale IS NOT NULL
        ORDER BY premium_pct DESC
    """
    df = bq.query_to_dataframe(query)
    return clean_for_json(df).to_dict(orient="records")

@app.get("/analytics/market-depth")
async def get_market_depth():
    """
    Returns the volume of listings per category across all sources.
    Useful for understanding market liquidity and saturation.
    """
    bq = BigQueryClient()
    table = os.getenv("DIOR_TABLE_ID", "dior_data")
    dataset = os.getenv("BIGQUERY_DATASET_ID", "data_management_projet")
    full_path = f"{bq.project_id}.{dataset}.{table}"

    query = f"""
        SELECT 
            category,
            Source,
            COUNT(*) as listing_count,
            AVG(price) as avg_price
        FROM `{full_path}`
        GROUP BY category, Source
        ORDER BY listing_count DESC
    """
    df = bq.query_to_dataframe(query)
    return clean_for_json(df).to_dict(orient="records")

@app.get("/analytics/scarcity-monitor")
async def get_scarcity_monitor(min_price: int = 1000, max_listings: int = 5):
    """
    Identifies 'Hidden Gems': High value items with low market availability.
    Signals potential scarcity value.
    """
    bq = BigQueryClient()
    table = os.getenv("DIOR_TABLE_ID", "dior_data")
    dataset = os.getenv("BIGQUERY_DATASET_ID", "data_management_projet")
    full_path = f"{bq.project_id}.{dataset}.{table}"

    query = f"""
        SELECT 
            product_name,
            category,
            AVG(price) as avg_resale_price,
            COUNT(*) as market_volume
        FROM `{full_path}`
        WHERE Source != 'Dior' AND price >= {min_price}
        GROUP BY product_name, category
        HAVING market_volume <= {max_listings}
        ORDER BY avg_resale_price DESC
        LIMIT 20
    """
    df = bq.query_to_dataframe(query)
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
