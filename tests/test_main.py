import os
import asyncio
import pandas as pd
from datetime import datetime
from transformers import pipeline

# Modularized imports
from src.scrapers.dior import scrape_all_dior_categories
from src.scrapers.vestiaire import scrape_vestiaire_dior
from src.scrapers.rebag import scrape_rebag_dior_plp
from src.database.bigquery import BigQueryManager

import nest_asyncio

# Setup for nested async loops
nest_asyncio.apply()

# --- CONFIGURATION ---
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

# --- 1. SCRAPING DATA ---
async def main_pipeline():
    print("Starting scraping...")

    # Scrape Dior
    # Note: Using the wrapper function
    all_data = await scrape_all_dior_categories(categories_to_scrape)
    df_dior = pd.DataFrame(all_data)
    if not df_dior.empty:
        df_dior['Condition'] = 'Brand New'
        df_dior['Source'] = 'Dior'
        # Fix: Ensure columns match for later concatenation
        # The scraper already names it 'category', so no rename needed unless typo exists in original
        if 'Catergories' in df_dior.columns:
            df_dior = df_dior.rename(columns={'Catergories': 'category'})

    # Scrape Rebag
    data_resale_1 = await scrape_rebag_dior_plp(start_page=1, end_page=2) # Limited for testing
    df_resale_1 = pd.DataFrame(data_resale_1)
    if not df_resale_1.empty:
        df_resale_1 = df_resale_1.drop_duplicates(subset=["Lien"])
        df_resale_1['Source'] = 'Rebag'

    # Scrape Vestiaire
    data_resale_2 = await scrape_vestiaire_dior()
    df_resale_2 = pd.DataFrame(data_resale_2)
    if not df_resale_2.empty:
        df_resale_2['Source'] = 'Vestiaire'

    # --- 2. DATA STANDARDIZATION ---
    def standardize_resale_df(df, source_name):
        if df.empty: return df
        # Mapping
        mapping = {
            'Marque': 'brand',
            'Nom': 'product_name',
            'Prix': 'retail_price',
            'Lien': 'product_url',
            'Condition': 'Condition'
        }
        df = df.rename(columns=mapping)
        
        # Fill missing standardized columns
        df['retail_product_id'] = None
        df['currency'] = 'USD' 
        df['image_url'] = None
        df['availability'] = 'In Stock'
        df['scrape_date'] = datetime.now().strftime("%Y-%m-%d")
        
        return df

    print("Standardizing resale data...")
    df_resale_1 = standardize_resale_df(df_resale_1, 'Rebag')
    df_resale_2 = standardize_resale_df(df_resale_2, 'Vestiaire')

    # --- 3. NLP CATEGORIZATION (Optimized) ---
    print("Initializing NLP Model...")
    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

    # Define labels from reference data (Dior)
    if not df_dior.empty:
        candidate_labels = df_dior['category'].dropna().unique().tolist()
    else:
        candidate_labels = ["Bags", "Ready-to-Wear", "Shoes", "Jewelry"]

    def apply_batch_categorization(df):
        if df.empty: return df
        print(f"Categorizing {len(df)} products...")
        
        # Create combined text for classification
        texts = (df['brand'].fillna('') + " " + df['product_name'].fillna('')).tolist()
        
        # Process in batch
        results = classifier(texts, candidate_labels)
        
        df['category'] = [r['labels'][0] for r in results]
        return df

    df_resale_1 = apply_batch_categorization(df_resale_1)
    df_resale_2 = apply_batch_categorization(df_resale_2)

    # --- 4. COMBINE AND SAVE ---
    target_order = [
        'retail_product_id', 'product_name', 'category', 'retail_price',
        'currency', 'product_url', 'image_url', 'availability',
        'scrape_date', 'Condition', 'Source'
    ]

    # Ensure Dior data has the same columns (adding missing ones)
    if not df_dior.empty:
        for col in target_order:
            if col not in df_dior.columns:
                df_dior[col] = None

    dfs_to_combine = []
    if not df_dior.empty: dfs_to_combine.append(df_dior[target_order])
    if not df_resale_1.empty: dfs_to_combine.append(df_resale_1[target_order])
    if not df_resale_2.empty: dfs_to_combine.append(df_resale_2[target_order])

    if dfs_to_combine:
        final_df = pd.concat(dfs_to_combine, ignore_index=True)
        print(f"TOTAL DATA TO UPLOAD: {len(final_df)} rows")
        
        # Initialize BigQueryManager
        # Paths can be set via env or passed here
        manager = BigQueryManager()
        # Ensure project and dataset match your environment
        table_id = os.getenv("MART_TABLE_ID", "asli-api.dior_dataset.dior_products")
        manager.save_to_bq(final_df, table_id)
        print("✅ All data saved to BigQuery")
    else:
        print("No data collected to save.")

if __name__ == "__main__":
    asyncio.run(main_pipeline())