import os
import sys

# --- PATH AND CREDENTIALS SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__)) # The 'api' folder
project_root = os.path.abspath(os.path.join(current_dir, "..")) # The Dior project folder

# Add project root to sys.path for modular imports
if project_root not in sys.path:
    sys.path.append(project_root)

# Locate the JSON key (Go up one more level from project_root)
# This assumes the JSON is in the 'Business data management' folder
json_path = os.path.abspath(os.path.join(project_root, "..", "asli-api-7d30bc2d2a4e.json"))

if os.path.exists(json_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    print(f"Success: Service account key located at {json_path}")
else:
    # This will help you see exactly where the script is looking
    print(f"Warning: Key not found at {json_path}. Check file location.")

os.environ["GOOGLE_CLOUD_PROJECT"] = "asli-api"


import asyncio
import pandas as pd
from datetime import datetime
from transformers import pipeline

# Modularized imports
from src.scrapers.dior import DiorScraper
from src.scrapers.vestiaire import VestiaireScraper
from src.scrapers.rebag import scrape_rebag_dior_plp
from src.database.bigquery import BigQueryClient

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


    # --- PART A: DIOR (RETAIL SOURCE) ---
    dior_tool = DiorScraper(headless=True)
    all_dior_list = []

    
    # Iterate through category configuration
    for category_name, url in categories_to_scrape.items():
        print(f"Action: Scraping Dior Category [{category_name}]...")
        try:
            # Method from dior.py
            category_data = await dior_tool.scrape_category(url, category_name)
            if category_data:
                all_dior_list.extend(category_data)
        except Exception as e:
            print(f"Error: Failed to scrape Dior category {category_name}: {e}")

    df_dior = pd.DataFrame(all_dior_list)
    
    # Post-processing Dior Data
    if not df_dior.empty:
        df_dior['Condition'] = 'Brand New'
        df_dior['Source'] = 'Dior'
        # Fix potential naming typos from scraper
        if 'Catergories' in df_dior.columns:
            df_dior = df_dior.rename(columns={'Catergories': 'category'})
        # Ensure ID column exists for the next step
        if 'retail_product_id' not in df_dior.columns:
            df_dior['retail_product_id'] = df_dior.index.astype(str)

    # --- PART B: REBAG (INDEPENDENT RESALE) ---
    print("Action: Scraping Rebag Resale listings...")
    # scrape_rebag_dior_plp is a function, not a class method
    rebag_data = await scrape_rebag_dior_plp(start_page=1, end_page=40) 
    df_resale_1 = pd.DataFrame(rebag_data)
    if not df_resale_1.empty:
        df_resale_1 = df_resale_1.drop_duplicates(subset=["Lien"])
        df_resale_1['Source'] = 'Rebag'

    # --- PART C: VESTIAIRE (DEPENDENT RESALE) ---
    print("Action: Scraping Vestiaire Resale listings using Dior seeds...")
    if not df_dior.empty:
        vestiaire_tool = VestiaireScraper(headless=True)
        # Pass df_dior as seed data to the class method
        vestiaire_data = await vestiaire_tool.scrape_all_from_df(df_dior, max_concurrent=10)
        df_resale_2 = pd.DataFrame(vestiaire_data)
        if not df_resale_2.empty:
            df_resale_2['Source'] = 'Vestiaire'
    else:
        print("Warning: Skipping Vestiaire because Dior retail data is empty.")
        df_resale_2 = pd.DataFrame()

    # --- 2. DATA STANDARDIZATION ---
    def standardize_resale_df(df, source_name):
        if df.empty: return df
        
        # Enhanced mapping to cover both Rebag and Vestiaire fields
        mapping = {
            'Marque': 'brand',
            'Nom': 'product_name',           # For Rebag
            'listing_title': 'product_name', # For Vestiaire
            'Prix': 'retail_price',          # For Rebag
            'resale_price': 'retail_price',  # For Vestiaire
            'Lien': 'product_url',           # For Rebag
            'listing_url': 'product_url',    # For Vestiaire
            'condition': 'Condition'         # For Vestiaire
        }
        df = df.rename(columns=mapping)
        
        # Add a default brand if missing (since we're scraping Dior)
        if 'brand' not in df.columns:
            df['brand'] = 'Dior'
            
        # Fill missing standardized columns
        df['retail_product_id'] = df.get('retail_product_id', None)
        df['currency'] = df.get('currency', 'USD') 
        df['image_url'] = df.get('image_url', None)
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
        
        # FIX: Ensure we are dealing with Pandas Series, not raw strings
        brand_data = df['brand'] if 'brand' in df.columns else pd.Series([''] * len(df))
        product_data = df['product_name'] if 'product_name' in df.columns else pd.Series([''] * len(df))
        
        # Now fillna() will work correctly
        texts = (brand_data.fillna('') + " " + product_data.fillna('')).tolist()
        
        results = classifier(texts, candidate_labels)
        df['category'] = [r['labels'][0] for r in results]
        return df

    df_resale_1 = apply_batch_categorization(df_resale_1)
    df_resale_2 = apply_batch_categorization(df_resale_2)

    # --- 4. COMBINE AND BACKUP ---
    print("Finalizing data for export...")

    target_order = [
        'retail_product_id', 'product_name', 'category', 'retail_price',
        'currency', 'product_url', 'image_url', 'availability',
        'scrape_date', 'Condition', 'Source'
    ]


    def ensure_columns_and_reorder(df, columns):
        if df.empty:
            return pd.DataFrame(columns=columns)

        for col in columns:
            if col not in df.columns:
                df[col] = None

        return df[columns]
    
    # Ensure all DataFrames have the same columns before concatenation
    df_dior_ready = ensure_columns_and_reorder(df_dior, target_order)
    df_rebag_ready = ensure_columns_and_reorder(df_resale_1, target_order)
    df_vest_ready = ensure_columns_and_reorder(df_resale_2, target_order)

    final_df = pd.concat([df_dior_ready, df_rebag_ready, df_vest_ready], ignore_index=True)

    if not final_df.empty:
        # STEP 1: LOCAL CSV BACKUP (Safety First)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"dior_final_extraction_{timestamp}.csv"
        # utf-8-sig ensures French accents display correctly in Excel
        final_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f"💾 EMERGENCY BACKUP SAVED: {csv_filename}")

        # STEP 2: BIGQUERY UPLOAD (To the new final table)
        print(f"TOTAL DATA TO UPLOAD: {len(final_df)} rows")
        manager = BigQueryClient()
        
        # Target the new clean table
        table_id = "asli-api.data_management_projet.dior_data_final"
        
        # Use 'replace' to ensure the schema is fresh and clean
        success = manager.upload_dataframe(final_df, table_id, if_exists="replace")
        
        if success:
            print(f"✅ SUCCESS: Data is now live in BigQuery table: {table_id}")
        else:
            print("❌ BQ UPLOAD FAILED. Please check the error above, but don't worry—you have the CSV!")

# --- THE START BUTTON ---
if __name__ == "__main__":
    try:
        # This triggers the engine to run your main_pipeline function
        asyncio.run(main_pipeline())
    except KeyboardInterrupt:
        print("\nPipeline stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")