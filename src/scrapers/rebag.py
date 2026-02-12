import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from datetime import datetime

async def scrape_rebag_dior_plp(start_page=1, end_page=1):
    """
    Scrapes Rebag for Dior products across multiple pages.
    """
    all_products = []
    base_url = "https://www.rebag.com/search/dior"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        for page_num in range(start_page, end_page + 1):
            url = f"{base_url}?page={page_num}"
            print(f"[Rebag] Scraping page {page_num}...")
            
            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                # Scroll a bit to ensure lazy load
                await page.mouse.wheel(0, 1000)
                await asyncio.sleep(2)
                content = await page.content()
            except Exception as e:
                print(f"[Error] Failed to scrape Rebag page {page_num}: {e}")
                continue

            soup = BeautifulSoup(content, 'html.parser')
            # Adjust selectors based on Rebag's current structure
            items = soup.select('div.product-card') 
            
            for item in items:
                try:
                    name_el = item.select_one('.product-name')
                    price_el = item.select_one('.product-price')
                    link_el = item.select_one('a')
                    
                    if name_el and price_el:
                        all_products.append({
                            "Marque": "Dior",
                            "Nom": name_el.get_text(strip=True),
                            "Prix": price_el.get_text(strip=True),
                            "Lien": "https://www.rebag.com" + link_el.get('href', '') if link_el else "N/A",
                            "Condition": "Pre-owned",
                            "scrape_date": datetime.now().strftime("%Y-%m-%d")
                        })
                except Exception:
                    continue
            
            await asyncio.sleep(1)

        await browser.close()
        return all_products

if __name__ == "__main__":
    # Local test run
    data_resale = asyncio.run(scrape_rebag_dior_plp(start_page=1, end_page=2))
    
    df_resale_1 = pd.DataFrame(data_resale)
    if not df_resale_1.empty:
        df_resale_1 = df_resale_1.drop_duplicates(subset=["Lien"])
        print(f"Total Rebag items found: {len(df_resale_1)}")
        print(df_resale_1.head())