import os
import sys
import pandas as pd
import asyncio
import nest_asyncio
import numpy as np
from fastapi import FastAPI, BackgroundTasks
from src.automation.scheduler import setup_daily_scheduler
from dotenv import load_dotenv
from datetime import datetime
from transformers import pipeline
from src.scrapers.dior import DiorScraper
from src.scrapers.vestiaire import VestiaireScraper
from src.scrapers.rebag import scrape_rebag_dior_plp
from src.database.bigquery import BigQueryClient


current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

json_path = os.path.abspath(os.path.join(project_root, "..", "asli-api-7d30bc2d2a4e.json"))
if os.path.exists(json_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
os.environ["GOOGLE_CLOUD_PROJECT"] = "asli-api"

nest_asyncio.apply()
load_dotenv()
app = FastAPI(title="Dior Data Management API")

# Utils
def clean_for_json(df: pd.DataFrame):
    return df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)

def standardize_resale_df(df, source_name):
    if df.empty: return df
    mapping = {
        'Marque': 'brand', 'Nom': 'product_name', 'listing_title': 'product_name',
        'Prix': 'retail_price', 'resale_price': 'retail_price',
        'Lien': 'product_url', 'listing_url': 'product_url', 'condition': 'Condition'
    }
    df = df.rename(columns=mapping)
    if 'brand' not in df.columns: df['brand'] = 'Dior'
    df['scrape_date'] = datetime.now().strftime("%Y-%m-%d")
    return df




categories_to_scrape = {
    "Bags_Homme": "https://www.dior.com/fr_fr/fashion/mode-homme/sacs/tous-les-sacs",
    "Ready-to-Wear_Homme": "https://www.dior.com/fr_fr/fashion/mode-homme/pret-a-porter/tout-le-pret-a-porter",
    "Bags_Femme": "https://www.dior.com/fr_fr/fashion/mode-femme/sacs/tous-les-sacs",
    "Ready-to-Wear_Femme": "https://www.dior.com/fr_fr/fashion/mode-femme/pret-a-porter/tout-le-pret-a-porter",
    "TShirts-Polos": "https://www.dior.com/fr_fr/fashion/mode-homme/pret-a-porter/polos-t-shirts",
    "Shoes_Homme": "https://www.dior.com/fr_fr/fashion/mode-homme/chaussures/toutes-les-chaussures",
    "Shirts_Homme": "https://www.dior.com/fr_fr/fashion/mode-homme/pret-a-porter/chemises",
    "Shirts_Femme": "https://www.dior.com/fr_fr/fashion/mode-femme/pret-a-porter/chemises",
    "Shoes_Femme": "https://www.dior.com/fr_fr/fashion/mode-femme/souliers/tous-les-souliers",
    "Home Makeup": "https://www.dior.com/fr_fr/beauty/home-makeup/home-makeup.html",
    "Skin Care": "https://www.dior.com/fr_fr/beauty/le-soin/les-categories",
    "Bath and Body": "https://www.dior.com/fr_fr/beauty/page/bath-and-body-by-category.html"
}

# API 
setup_daily_scheduler(app)

@app.get("/")
async def root(): return {"message": "API is running"}

@app.get("/data/dior")
async def get_dior_data(limit: int = 50):
    bq = BigQueryClient()
    df = bq.get_dior_data("data_management_projet", "dior_data_final", limit)
    return clean_for_json(df).to_dict(orient="records")

@app.get("/analytics/summary")
async def get_analytics_summary():
    bq = BigQueryClient()
    query = "SELECT Source, COUNT(*) as count FROM `asli-api.data_management_projet.dior_data_final` GROUP BY Source"
    df = bq.query_to_dataframe(query)
    return clean_for_json(df).to_dict(orient="records")

@app.get("/analytics/brand-premium")
async def get_brand_premium():
    bq = BigQueryClient()
    query = """
        SELECT category, 
        AVG(CASE WHEN Source = 'Dior' THEN retail_price END) as avg_retail,
        AVG(CASE WHEN Source != 'Dior' THEN retail_price END) as avg_resale
        FROM `asli-api.data_management_projet.dior_data_final`
        GROUP BY category
    """
    df = bq.query_to_dataframe(query)
    return clean_for_json(df).to_dict(orient="records")

@app.get("/tools/exchange-rate")
async def get_exchange_rate(base: str = "USD", target: str = "EUR"):
    import httpx
    url = f"https://v6.exchangerate-api.com/v6/{os.getenv('EXCHANGE_RATE_API_KEY')}/latest/{base}"
    async with httpx.AsyncClient() as client:
        res = await client.get(url)
        return res.json()

# Pipeline
async def main_pipeline():
    print("🚀 Starting full integrated pipeline...")
    
    # A. Dior
    dior_tool = DiorScraper(headless=True)
    all_dior = []
    for cat, url in categories_to_scrape.items():
        data = await dior_tool.scrape_category(url, cat)
        if data: all_dior.extend(data)
    df_dior = pd.DataFrame(all_dior)

    # B. Rebag
    rebag_raw = await scrape_rebag_dior_plp(start_page=1, end_page=40)
    df_rebag = pd.DataFrame(rebag_raw)
    if not df_rebag.empty: df_rebag['Source'] = 'Rebag'

    # C. Vestiaire
    if not df_dior.empty:
        v_tool = VestiaireScraper(headless=True)
        v_raw = await v_tool.scrape_all_from_df(df_dior, max_concurrent=10)
        df_vest = pd.DataFrame(v_raw)
        if not df_vest.empty: df_vest['Source'] = 'Vestiaire'
    else:
        df_vest = pd.DataFrame()

    # Standardize & NLP
    df_rebag = standardize_resale_df(df_rebag, 'Rebag')
    df_vest = standardize_resale_df(df_vest, 'Vestiaire')

    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
    def apply_nlp(df):
        if df.empty: return df
        candidate_labels = ["Bags", "Ready-to-Wear", "Shoes", "Beauty"]
        texts = (df['brand'].fillna('') + " " + df['product_name'].fillna('')).tolist()
        res = classifier(texts, candidate_labels)
        df['category'] = [r['labels'][0] for r in res]
        return df

    df_rebag = apply_nlp(df_rebag)
    df_vest = apply_nlp(df_vest)

    
    target_order = ['retail_product_id', 'product_name', 'category', 'retail_price', 'currency', 'product_url', 'scrape_date', 'Condition', 'Source']
    def prep(df):
        for c in target_order: 
            if c not in df.columns: df[c] = None
        return df[target_order]

    final_df = pd.concat([prep(df_dior), prep(df_rebag), prep(df_vest)], ignore_index=True)
    
    bq = BigQueryClient()
    bq.upload_dataframe(final_df, "asli-api.data_management_projet.dior_data_final", if_exists="replace")
    print("✅ Pipeline Completed Successfully!")


if __name__ == "__main__":
    asyncio.run(main_pipeline())