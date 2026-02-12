import os
import asyncio
import pandas as pd
from datetime import datetime
from transformers import pipeline
from dior_scraper import scrape_all_dior_categories
from vestiaire_scraper import scrape_vestiaire_dior
from rebag_scraper import scrape_rebag_dior_plp
import nest_asyncio
from bigquery_io import BigQueryManager

# Setup for nested async loops
nest_asyncio.apply()

# --- CONFIGURATION ---
# Fix: Used unique keys to prevent dictionary overwrites
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
print("Starting scraping...")

# Scrape Dior
all_data = asyncio.run(scrape_all_dior_categories(categories_to_scrape))
df_dior = pd.DataFrame(all_data)
df_dior['Condition'] = 'Brand New'
df_dior['Source'] = 'Dior'
# Fix: Ensure columns match for later concatenation
df_dior = df_dior.rename(columns={'Catergories': 'category'}) # Fixing typo

# Scrape Rebag
data_resale_1 = asyncio.run(scrape_rebag_dior_plp(start_page=1, end_page=10))
df_resale_1 = pd.DataFrame(data_resale_1).drop_duplicates(subset=["Lien"])
df_resale_1['Source'] = 'Rebag'

# Scrape Vestiaire
data_resale_2 = asyncio.run(scrape_vestiaire_dior())
df_resale_2 = pd.DataFrame(data_resale_2)
df_resale_2['Source'] = 'Vestiaire'

# --- 2. DATA STANDARDIZATION ---
def standardize_resale_df(df, source_name):
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
    df['currency'] = 'USD' # Adjust as necessary
    df['image_url'] = None
    df['availability'] = 'In Stock'
    df['scrape_date'] = datetime.now().strftime("%Y-%m-%d")
    
    return df

print("Standardizing resale data...")
df_resale_1 = standardize_resale_df(df_resale_1, 'Rebag')
df_resale_2 = standardize_resale_df(df_resale_2, 'Vestiaire')

# --- 3. NLP CATEGORIZATION (Optimized) ---
print("Initializing NLP Model...")
# Fix: Load model ONCE
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# Define labels from reference data (Dior)
candidate_labels = df_dior['category'].dropna().unique().tolist()

def apply_batch_categorization(df):
    if df.empty: return df
    print(f"Categorizing {len(df)} products...")
    
    # Create combined text for classification
    texts = (df['brand'].fillna('') + " " + df['product_name'].fillna('')).tolist()
    
    # Fix: Process in batch instead of row-by-row
    results = classifier(texts, candidate_labels)
    
    df['category'] = [r['labels'][0] for r in results]
    return df

df_resale_1 = apply_batch_categorization(df_resale_1)
df_resale_2 = apply_batch_categorization(df_resale_2)

# --- 4. COMBINE AND SAVE ---
# Ensure all DataFrames have the same columns in the same order
target_order = [
    'retail_product_id', 'product_name', 'category', 'retail_price',
    'currency', 'product_url', 'image_url', 'availability',
    'scrape_date', 'Condition', 'Source'
]

# Ensure Dior data has the same columns (adding missing ones)
for col in target_order:
    if col not in df_dior.columns:
        df_dior[col] = None

final_df = pd.concat([df_dior[target_order], df_resale_1[target_order], df_resale_2[target_order]], ignore_index=True)

print(f"TOTAL DATA TO UPLOAD: {len(final_df)} rows")
BigQueryManager().save_to_bq(final_df, "asli-api.dior_dataset.dior_products")
print("✅ All data saved to BigQuery")