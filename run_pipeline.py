import os
import asyncio
import pandas as pd
from datetime import datetime

# Modularized imports
from src.scrapers.dior import scrape_all_dior_categories
from src.scrapers.vestiaire import scrape_vestiaire_dior
from src.scrapers.rebag import scrape_rebag_dior_plp
from src.database.bigquery import BigQueryManager
from src.analytics.normalization import DataNormalizer
from src.analytics.matching import ValueAnalyzer
from src.analytics.currency import normalize_prices_to_eur

import nest_asyncio

# Setup for nested async loops (useful in notebooks or some IDEs)
nest_asyncio.apply()

# --- CONFIGURATION ---
categories_to_scrape = {
    "Bags_Homme": "https://www.dior.com/fr_fr/fashion/mode-homme/sacs/tous-les-sacs",
    "Ready-to-Wear_Homme": "https://www.dior.com/fr_fr/fashion/mode-homme/pret-a-porter/tout-le-pret-a-porter",
    "Bags_Femme": "https://www.dior.com/fr_fr/fashion/mode-femme/sacs/tous-les-sacs",
    "Ready-to-Wear_Femme": "https://www.dior.com/fr_fr/fashion/mode-femme/pret-a-porter/tout-le-pret-a-porter",
    "Shoes_Homme": "https://www.dior.com/fr_fr/fashion/mode-homme/chaussures/toutes-les-chaussures",
    "Shoes_Femme": "https://www.dior.com/fr_fr/fashion/mode-femme/souliers/tous-les-souliers",
}

async def run_full_analytical_pipeline():
    print("🚀 Starting Dior Value Retention Pipeline...")
    
    # --- 1. SCRAPING LAYER ---
    print("\n[Step 1] Scraping Retail & Secondary Markets...")
    
    # Retail: Dior
    dior_data = await scrape_all_dior_categories(categories_to_scrape)
    df_retail = pd.DataFrame(dior_data)
    
    # Resale: Rebag & Vestiaire
    # (Scraping subset for demonstration/speed)
    rebag_data = await scrape_rebag_dior_plp(start_page=1, end_page=1)
    df_resale_1 = pd.DataFrame(rebag_data)
    
    vestiaire_data = await scrape_vestiaire_dior()
    df_resale_2 = pd.DataFrame(vestiaire_data)
    
    if df_retail.empty:
        print("❌ No retail data found. Aborting.")
        return

    # --- 2. NORMALIZATION LAYER ---
    print("\n[Step 2] Normalizing & Cleaning Data...")
    normalizer = DataNormalizer()
    
    # Process Retail
    df_retail['category'] = df_retail['category'].apply(normalizer.harmonize_category)
    df_retail['product_name_clean'] = df_retail['product_name'].apply(normalizer.clean_text)
    df_retail['Source'] = 'Dior'
    
    # Process Resale
    def prepare_resale(df, source):
        if df.empty: return df
        # Map columns if necessary (matching user's test_main.py logic)
        mapping = {'Nom': 'product_name', 'Prix': 'resale_price', 'Lien': 'product_url', 'Marque': 'brand'}
        df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})
        
        df = df.copy()
        if 'currency' not in df.columns:
            df['currency'] = 'USD' if source == 'Rebag' else 'EUR'
        df['category'] = df['product_name'].apply(normalizer.harmonize_category)
        df['product_name_clean'] = df['product_name'].apply(normalizer.clean_text)
        df['Source'] = source
        return df

    df_resale_1 = prepare_resale(df_resale_1, 'Rebag')
    df_resale_2 = prepare_resale(df_resale_2, 'Vestiaire')
    df_retail = await normalize_prices_to_eur(df_retail, price_col="retail_price", currency_col="currency")
    df_retail['retail_price_num'] = df_retail['retail_price_eur']
    df_resale_1 = await normalize_prices_to_eur(df_resale_1, price_col="resale_price", currency_col="currency")
    df_resale_1['resale_price_num'] = df_resale_1['retail_price_eur']
    df_resale_2 = await normalize_prices_to_eur(df_resale_2, price_col="resale_price", currency_col="currency")
    df_resale_2['resale_price_num'] = df_resale_2['retail_price_eur']
    
    df_all_resale = pd.concat([df_resale_1, df_resale_2], ignore_index=True)

    # --- 3. ANALYTICAL LAYER (Matching & Metrics) ---
    print("\n[Step 3] Performing Fuzzy Matching & Calculating RVR...")
    analyzer = ValueAnalyzer(similarity_threshold=0.75)
    
    # Match secondary listings to retail products
    df_matched = analyzer.match_listings(df_retail, df_all_resale)
    
    # Calculate Resale Value Retention (RVR)
    df_mart = analyzer.calculate_metrics(df_matched)
    
    if df_mart.empty:
        print("⚠️ No matches found between retail and resale. Check similarity thresholds.")
    else:
        print(f"✅ Generated Analytical Mart with {len(df_mart)} matched products.")

    # --- 4. DATA INJECTION ---
    print("\n[Step 4] Injecting results into BigQuery...")
    bq_manager = BigQueryManager()
    
    # Save everything to the unified table
    unified_table_id = os.getenv("DIOR_TABLE_ID", "data_management_projet.dior_data")
    
    print(f"Uploading Analytical Mart ({len(df_mart)} rows)...")
    bq_manager.save_to_bq(df_mart, unified_table_id)
    
    print(f"Uploading Raw Retail Data ({len(df_retail)} rows)...")
    bq_manager.save_to_bq(df_retail, unified_table_id)
    
    print("\n🏁 Pipeline Complete! Your data is ready in the unified table.")

if __name__ == "__main__":
    asyncio.run(run_full_analytical_pipeline())
