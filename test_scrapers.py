import asyncio
from src.scrapers.dior import DiorScraper
from src.scrapers.vestiaire import VestiaireScraper
import pandas as pd

async def test_dior():
    print("Testing Dior Scraper...")
    scraper = DiorScraper(headless=True)
    categories = {
        "Bags": "https://www.dior.com/fr_fr/fashion/mode-homme/sacs/tous-les-sacs",
    }
    # Scrape only a small sample if possible, or just check the first category
    results = await scraper.scrape_all(categories)
    print(f"Found {len(results)} products.")
    if results:
        print("Sample product:", results[0])
    return results

async def test_vestiaire(dior_results):
    print("\nTesting Vestiaire Scraper...")
    scraper = VestiaireScraper(headless=True)
    if not dior_results:
        print("No Dior results to seed Vestiaire test.")
        return
    
    # Use the first Dior product as a seed
    df_dior = pd.DataFrame([dior_results[0]])
    results = await scraper.scrape_all_from_df(df_dior)
    print(f"Found {len(results)} resale listings.")
    if results:
        print("Sample listing:", results[0])

if __name__ == "__main__":
    dior_data = asyncio.run(test_dior())
    # Vestiaire might be slow or get blocked, so test with caution
    if dior_data:
        asyncio.run(test_vestiaire(dior_data[:1]))
