import os
import sys
import pandas as pd
import asyncio
import nest_asyncio
import numpy as np
from fastapi import FastAPI, BackgroundTasks, HTTPException
from src.automation.scheduler import setup_daily_scheduler
from dotenv import load_dotenv
from datetime import datetime
from transformers import pipeline
from src.scrapers.dior import DiorScraper
from src.scrapers.vestiaire import VestiaireScraper
from src.scrapers.rebag import scrape_rebag_dior_plp
from src.database.bigquery import BigQueryClient
from src.analytics.currency import normalize_prices_to_eur


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
dior_scraper = DiorScraper(headless=True)
vestiaire_scraper = VestiaireScraper(headless=True)
DEFAULT_DATASET = "data_management_projet"
DEFAULT_TABLE = "dior_data_final"

# Utils
def clean_for_json(df: pd.DataFrame):
    return df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)

def get_full_table_path(project_id: str, dataset: str = None, table: str = None) -> str:
    """
    Resolve BigQuery table path from explicit args and env vars.
    Supports table values as:
    - table only (dior_data_final)
    - dataset.table
    - project.dataset.table
    """
    raw_table = table or DEFAULT_TABLE
    raw_dataset = dataset or DEFAULT_DATASET

    parts = raw_table.split(".")
    if len(parts) == 3:
        return raw_table
    if len(parts) == 2:
        return f"{project_id}.{raw_table}"
    return f"{project_id}.{raw_dataset}.{raw_table}"


def resolve_project_id(bq_client) -> str:
    return getattr(bq_client, "project_id", None) or os.getenv("GOOGLE_CLOUD_PROJECT", "asli-api")


def dataframe_or_404(df: pd.DataFrame, detail: str):
    if df.empty:
        raise HTTPException(status_code=404, detail=detail)
    return clean_for_json(df).to_dict(orient="records")


def normalized_price_eur_sql() -> str:
    usd_to_eur = float(os.getenv("USD_TO_EUR_RATE", "0.92"))
    return f"""
        COALESCE(
            retail_price_eur,
            CASE
                WHEN UPPER(IFNULL(currency, '')) = 'USD' OR Source = 'Rebag'
                    THEN SAFE_CAST(REPLACE(REGEXP_REPLACE(CAST(retail_price AS STRING), r'[^0-9,\\.]', ''), ',', '') AS FLOAT64) * {usd_to_eur}
                WHEN REGEXP_CONTAINS(CAST(retail_price AS STRING), r',') AND NOT REGEXP_CONTAINS(CAST(retail_price AS STRING), r'\\.')
                    THEN SAFE_CAST(REPLACE(REGEXP_REPLACE(CAST(retail_price AS STRING), r'[^0-9,]', ''), ',', '.') AS FLOAT64)
                ELSE SAFE_CAST(REGEXP_REPLACE(CAST(retail_price AS STRING), r'[^0-9\\.]', '') AS FLOAT64)
            END
        )
    """


def standardize_resale_df(df, source_name):
    if df.empty: return df
    mapping = {
        'Marque': 'brand', 'Nom': 'product_name', 'listing_title': 'product_name',
        'Prix': 'retail_price', 'resale_price': 'retail_price',
        'Lien': 'product_url', 'listing_url': 'product_url', 'condition': 'Condition'
    }
    df = df.rename(columns=mapping)
    if 'brand' not in df.columns: df['brand'] = 'Dior'
    if 'currency' not in df.columns:
        df['currency'] = 'USD' if source_name == "Rebag" else 'EUR'
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

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/scrape/dior")
async def trigger_dior_scrape(background_tasks: BackgroundTasks, categories: dict = None):
    if not categories:
        categories = {
            "Bags": "https://www.dior.com/fr_fr/fashion/mode-homme/sacs/tous-les-sacs",
            "Ready-to-Wear": "https://www.dior.com/fr_fr/fashion/mode-homme/pret-a-porter/tout-le-pret-a-porter",
        }

    background_tasks.add_task(dior_scraper.scrape_all, categories)
    return {"message": "Dior scrape triggered in background", "categories": list(categories.keys())}

@app.get("/data/dior")
async def get_dior_data(limit: int = 50, dataset: str = None, table: str = None):
    try:
        bq = BigQueryClient()
        full_table = get_full_table_path(resolve_project_id(bq), dataset=dataset, table=table)
        price_eur_expr = normalized_price_eur_sql()
        query = f"""
            SELECT
                product_name,
                retail_product_id,
                category,
                retail_price,
                currency,
                {price_eur_expr} AS price_eur,
                IFNULL(FORMAT('€%.2f', {price_eur_expr}), NULL) AS price_eur_formatted,
                Source,
                scrape_date,
                product_url
            FROM `{full_table}`
            ORDER BY scrape_date DESC
            LIMIT {limit}
        """
        df = bq.query_to_dataframe(query)
        return dataframe_or_404(df, "No Dior data found in dior_data_final")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch Dior data: {e}")

@app.post("/pipeline/run")
async def run_pipeline(background_tasks: BackgroundTasks):
    from run_pipeline import run_full_analytical_pipeline

    background_tasks.add_task(run_full_analytical_pipeline)
    return {"message": "Full analytical pipeline started in background"}

@app.get("/analytics/summary")
async def get_analytics_summary(dataset: str = None, table: str = None):
    try:
        bq = BigQueryClient()
        full_table = get_full_table_path(resolve_project_id(bq), dataset=dataset, table=table)
        price_eur_expr = normalized_price_eur_sql()
        query = f"""
            SELECT
                Source,
                COUNT(*) as count,
                AVG({price_eur_expr}) as avg_price_eur,
                MAX(scrape_date) as last_scraped
            FROM `{full_table}`
            GROUP BY Source
            ORDER BY count DESC
        """
        df = bq.query_to_dataframe(query)
        return dataframe_or_404(df, "No analytics summary data found in dior_data_final")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch analytics summary: {e}")

@app.get("/analytics/investment-hotspots")
async def get_investment_hotspots(limit: int = 10, dataset: str = None, table: str = None):
    try:
        bq = BigQueryClient()
        full_table = get_full_table_path(resolve_project_id(bq), dataset=dataset, table=table)
        price_eur_expr = normalized_price_eur_sql()
        query = f"""
            SELECT
                product_name,
                category,
                {price_eur_expr} as resale_price_eur,
                IFNULL(FORMAT('€%.2f', {price_eur_expr}), NULL) as resale_price_eur_formatted,
                Source,
                scrape_date
            FROM `{full_table}`
            WHERE Source != 'Dior'
              AND {price_eur_expr} IS NOT NULL
            ORDER BY resale_price_eur DESC, scrape_date DESC
            LIMIT {limit}
        """
        df = bq.query_to_dataframe(query)
        return dataframe_or_404(df, "No investment hotspot data found in dior_data_final")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch investment hotspots: {e}")

@app.get("/analytics/brand-premium")
async def get_brand_premium(dataset: str = None, table: str = None):
    try:
        bq = BigQueryClient()
        full_table = get_full_table_path(resolve_project_id(bq), dataset=dataset, table=table)
        price_eur_expr = normalized_price_eur_sql()
        query = f"""
            SELECT
                category,
                AVG(CASE WHEN Source = 'Dior' THEN {price_eur_expr} END) as avg_retail,
                AVG(CASE WHEN Source != 'Dior' THEN {price_eur_expr} END) as avg_resale,
                (
                    AVG(CASE WHEN Source != 'Dior' THEN {price_eur_expr} END)
                    - AVG(CASE WHEN Source = 'Dior' THEN {price_eur_expr} END)
                )
                / NULLIF(AVG(CASE WHEN Source = 'Dior' THEN {price_eur_expr} END), 0) * 100 as premium_pct
            FROM `{full_table}`
            GROUP BY category
            HAVING avg_retail IS NOT NULL AND avg_resale IS NOT NULL
            ORDER BY premium_pct DESC
        """
        df = bq.query_to_dataframe(query)
        return dataframe_or_404(df, "No brand premium data found in dior_data_final")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch brand premium analytics: {e}")

@app.get("/analytics/market-depth")
async def get_market_depth(dataset: str = None, table: str = None):
    try:
        bq = BigQueryClient()
        full_table = get_full_table_path(resolve_project_id(bq), dataset=dataset, table=table)
        price_eur_expr = normalized_price_eur_sql()
        query = f"""
            SELECT
                category,
                Source,
                COUNT(*) as listing_count,
                AVG({price_eur_expr}) as avg_price_eur
            FROM `{full_table}`
            GROUP BY category, Source
            ORDER BY listing_count DESC
        """
        df = bq.query_to_dataframe(query)
        return dataframe_or_404(df, "No market depth data found in dior_data_final")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch market depth analytics: {e}")

@app.get("/analytics/scarcity-monitor")
async def get_scarcity_monitor(min_price: int = 1000, max_listings: int = 5, dataset: str = None, table: str = None):
    try:
        bq = BigQueryClient()
        full_table = get_full_table_path(resolve_project_id(bq), dataset=dataset, table=table)
        price_eur_expr = normalized_price_eur_sql()
        query = f"""
            SELECT
                product_name,
                category,
                AVG({price_eur_expr}) as avg_resale_price_eur,
                IFNULL(FORMAT('€%.2f', AVG({price_eur_expr})), NULL) as avg_resale_price_eur_formatted,
                COUNT(*) as market_volume
            FROM `{full_table}`
            WHERE Source != 'Dior'
              AND {price_eur_expr} >= {min_price}
            GROUP BY product_name, category
            HAVING market_volume <= {max_listings}
            ORDER BY avg_resale_price_eur DESC
            LIMIT 20
        """
        df = bq.query_to_dataframe(query)
        return dataframe_or_404(df, "No scarcity monitor data found in dior_data_final")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch scarcity monitor analytics: {e}")

@app.post("/scrape/vestiaire")
async def trigger_vestiaire_scrape(background_tasks: BackgroundTasks, dataset: str = None, table: str = None):
    try:
        bq_client = BigQueryClient()
        full_table = get_full_table_path(resolve_project_id(bq_client), dataset=dataset, table=table)
        query = f"""
            SELECT product_name, retail_product_id, retail_price, category
            FROM `{full_table}`
            WHERE Source = 'Dior' AND product_name IS NOT NULL
            LIMIT 10
        """
        df_dior = bq_client.query_to_dataframe(query)

        if df_dior.empty:
            raise HTTPException(status_code=404, detail="No Dior products found to seed Vestiaire scrape")

        background_tasks.add_task(vestiaire_scraper.scrape_all_from_df, df_dior)
        return {"message": "Vestiaire scrape triggered in background for 10 products"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger Vestiaire scrape: {e}")

@app.get("/tools/exchange-rate")
async def get_exchange_rate(base: str = "USD", target: str = "EUR"):
    import httpx
    api_key = os.getenv("EXCHANGE_RATE_API_KEY") or os.getenv("FX_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="EXCHANGE_RATE_API_KEY is not configured")

    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base}"
    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(url)
            data = res.json()
            if res.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Exchange API error: {data}")
            rate = data.get("conversion_rates", {}).get(target)
            if rate is None:
                raise HTTPException(status_code=404, detail=f"Target currency {target} not found")
            return {
                "base": base,
                "target": target,
                "rate": rate,
                "provider": "ExchangeRate-API",
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to fetch exchange rate: {e}")

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
    if not df_dior.empty:
        df_dior["Source"] = "Dior"
        df_dior["Condition"] = "Brand New"

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
    df_dior = await normalize_prices_to_eur(df_dior, price_col="retail_price", currency_col="currency")
    df_rebag = await normalize_prices_to_eur(df_rebag, price_col="retail_price", currency_col="currency")
    df_vest = await normalize_prices_to_eur(df_vest, price_col="retail_price", currency_col="currency")

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

    
    target_order = [
        'retail_product_id',
        'product_name',
        'category',
        'retail_price',
        'retail_price_num',
        'fx_rate_to_eur',
        'retail_price_eur',
        'currency',
        'product_url',
        'scrape_date',
        'Condition',
        'Source',
    ]
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
