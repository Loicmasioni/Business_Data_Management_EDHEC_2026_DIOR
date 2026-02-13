import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from datetime import datetime

class VestiaireScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.base_url = "https://fr.vestiairecollective.com"

    async def scrape_product(self, browser, product_name):
        """
        Search for a specific product and return the first result.
        """
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        search_query = product_name.replace(" ", "+")
        url = f"{self.base_url}/search/?q=Dior+{search_query}"
        
        try:
            # Increased timeout to 60s to reduce timeout errors
            await page.goto(url, wait_until="networkidle", timeout=60000)
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Selector for the first product card
            product_card = soup.select_one('div[class*="product-card_productCard"]')
            
            if product_card:
                title_el = product_card.select_one('p[class*="product-card_productCard__title"]')
                price_el = product_card.select_one('span[class*="product-card_productCard__price"]')
                link_el = product_card.select_one('a')
                
                return {
                    "listing_title": title_el.get_text(strip=True) if title_el else product_name,
                    "resale_price": price_el.get_text(strip=True) if price_el else "N/A",
                    "listing_url": self.base_url + link_el.get('href', '') if link_el else url,
                    "condition": "Pre-owned",
                    "scrape_date": datetime.now().strftime("%Y-%m-%d")
                }
        except Exception as e:
            print(f"[Error] Failed to scrape Vestiaire for {product_name}: {e}")
            return None
        finally:
            await page.close()
            await context.close()

    async def scrape_all_from_df(self, df_dior, max_concurrent=10):
        """
        Uses Dior products as seeds to scrape Vestiaire with concurrency control.
        """
        if df_dior.empty:
            return []

        product_names = df_dior['product_name'].unique().tolist()
        results = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            semaphore = asyncio.Semaphore(max_concurrent)

            async def sem_task(name):
                async with semaphore:
                    return await self.scrape_product(browser, name)

            tasks = [sem_task(name) for name in product_names]
            scraped_results = await asyncio.gather(*tasks)
            
            results = [r for r in scraped_results if r is not None]
            await browser.close()
            
        return results


async def scrape_vestiaire_dior(product_names=None, max_items=20, headless=True, max_concurrent=10):
    """
    Backward-compatible wrapper used by pipeline modules.
    """
    if product_names is None:
        product_names = [
            "Lady Dior Bag",
            "Saddle Bag",
            "Book Tote",
            "Dior Caro Bag",
            "Dior Bobby Bag",
        ]

    if max_items is not None and max_items > 0:
        product_names = product_names[:max_items]

    seed_df = pd.DataFrame({"product_name": product_names})
    scraper = VestiaireScraper(headless=headless)
    return await scraper.scrape_all_from_df(seed_df, max_concurrent=max_concurrent)

if __name__ == "__main__":
    # Quick test run
    scraper = VestiaireScraper(headless=True)
    test_df = pd.DataFrame({"product_name": ["Saddle Bag", "Lady Dior"]})
    data = asyncio.run(scraper.scrape_all_from_df(test_df, max_concurrent=2))
    print(f"Scraped {len(data)} items from Vestiaire.")
