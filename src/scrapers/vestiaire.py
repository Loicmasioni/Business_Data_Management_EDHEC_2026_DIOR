import re
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from datetime import datetime

class VestiaireScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

    async def scrape_product(self, product_row):
        """
        Scrapes Vestiaire for a specific Dior product as a seed.
        """
        product_name = product_row['product_name']
        retail_id = product_row['retail_product_id']
        retail_price = product_row['retail_price']
        retail_cat = product_row['category']

        # Derive keywords (remove brand and generic words)
        keywords = re.sub(r'dior|christian|sac |handbag |pochette ', '', product_name, flags=re.IGNORECASE).strip()
        search_query = f"Dior {keywords}"

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(user_agent=self.user_agent)
            page = await context.new_page()

            query_encoded = search_query.replace(' ', '+')
            url = f"https://fr.vestiairecollective.com/search/?q={query_encoded}"

            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.mouse.wheel(0, 500)
                await asyncio.sleep(1)
                content = await page.content()
            except Exception as e:
                print(f"[Error] Failed to scrape Vestiaire for {product_name}: {e}")
                content = ""
            finally:
                await browser.close()

        if not content:
            return []

        soup = BeautifulSoup(content, 'html.parser')
        listings = []
        scrape_date = datetime.now().strftime("%Y-%m-%d")
        cards = soup.select('a[class*="product-card_productCard"]')[:5]

        for card in cards:
            aria_label = card.get('aria-label', '')
            listing_url = "https://fr.vestiairecollective.com" + card.get('href', '')
            resale_id = card.get('id', '').replace('product_id_', '')
            name_el = card.select_one('h3')
            listing_title = name_el.get_text(separator=" ", strip=True) if name_el else "N/A"

            price_el = card.select_one('span[class*="productDetails__price"]') or card.select_one('p[class*="price"]')
            resale_price = price_el.get_text(strip=True) if price_el else "N/A"

            # Robust price extraction check
            if resale_price == "N/A" and aria_label:
                price_match = re.search(r"(\d[\d\s]*€)", aria_label.replace('\u00a0', ' '))
                resale_price = price_match.group(1).strip() if price_match else "N/A"

            country_match = re.search(r"Expédié depuis ([^,\.]+)", aria_label)
            seller_country = country_match.group(1).strip() if country_match else "N/A"
            condition = "Vintage" if "vintage" in aria_label.lower() else "Pre-owned"

            listings.append({
                "listing_id": resale_id,
                "listing_title": listing_title,
                "category": retail_cat,
                "resale_price": resale_price,
                "currency": "EUR" if "€" in str(resale_price) else "N/A",
                "condition": condition,
                "listing_date": scrape_date,
                "listing_url": listing_url,
                "seller_country": seller_country,
                "parent_retail_id": retail_id,
                "parent_retail_price": retail_price,
                "scrape_date": scrape_date
            })
        return listings

    async def scrape_all_from_df(self, retail_df):
        all_resale = []
        # Deduplicate unique Dior products to avoid redundant scrapes
        seeds = retail_df.drop_duplicates(subset=['retail_product_id'])
        total_seeds = len(seeds)

        print(f"[Info] Starting resale scrape for {total_seeds} unique retail products...")

        for idx, (_, product) in enumerate(seeds.iterrows()):
            results = await self.scrape_product(product)
            all_resale.extend(results)

            if (idx + 1) % 5 == 0 or (idx + 1) == total_seeds:
                print(f"[Progress] Processed {idx+1}/{total_seeds} products... ({len(all_resale)} listings found)")

            # Slightly longer sleep for large batches to avoid blocking
            await asyncio.sleep(1.5)

        return all_resale

async def scrape_vestiaire_dior():
    scraper = VestiaireScraper()
    # If called without arguments, it might just scrape a general search or return empty
    # For compatibility with test_main.py which calls it without args:
    dummy_row = {'product_name': 'Dior Bag', 'retail_product_id': 'general', 'retail_price': 'N/A', 'category': 'Bags'}
    return await scraper.scrape_product(dummy_row)
