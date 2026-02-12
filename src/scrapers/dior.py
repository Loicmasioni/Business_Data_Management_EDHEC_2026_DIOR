import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from datetime import datetime

class DiorScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

    async def scrape_category(self, target_url, category_name="General"):
        """
        Scrapes a specific Dior category page using a Google Translate proxy check bypass.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(user_agent=self.user_agent)
            page = await context.new_page()

            bypass_url = f"https://translate.google.com/translate?sl=auto&tl=fr&u={target_url}"
            print(f"[Dior] Scraping category '{category_name}' via Proxy...")

            try:
                await page.goto(bypass_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(5)  # Give translation time to settle

                # Scroll to load dynamic content
                for _ in range(5):
                    await page.mouse.wheel(0, 2000)
                    await asyncio.sleep(2)

                content = await page.content()
            except Exception as e:
                print(f"[Error] Failed to scrape {category_name}: {e}")
                content = ""
            finally:
                await browser.close()

            if not content:
                return []

            soup = BeautifulSoup(content, 'html.parser')
            products = []
            scrape_date = datetime.now().strftime("%Y-%m-%d")
            items = soup.select('div[data-testid^="product-card-"]')

            for item in items:
                testid = item.get('data-testid', '')
                retail_product_id = testid.replace('product-card-', '') if testid else "N/A"

                name_el = item.select_one('[data-testid="product-title"]')
                product_name = name_el.get_text(separator=" ", strip=True) if name_el else "N/A"

                price_el = item.select_one('[data-testid="price-line"]')
                retail_price = price_el.get_text(separator=" ", strip=True) if price_el else "N/A"

                img_el = item.select_one('img.main-asset')
                image_url = img_el.get('src') if img_el else "N/A"

                link_el = item.select_one('a.product-card__link')
                raw_url = link_el.get('href') if link_el else "N/A"
                product_url = raw_url.split('?')[0] if raw_url != "N/A" else "N/A"

                full_text = item.get_text().lower()
                availability = "Unavailable" if "indisponible" in full_text else "In Stock"

                if product_name != "N/A":
                    products.append({
                        "retail_product_id": retail_product_id,
                        "product_name": product_name,
                        "category": category_name,
                        "retail_price": retail_price,
                        "currency": "EUR",
                        "product_url": product_url,
                        "image_url": image_url,
                        "availability": availability,
                        "scrape_date": scrape_date
                    })

            return products

    async def scrape_all(self, categories_dict):
        all_results = []
        for cat_name, url in categories_dict.items():
            data = await self.scrape_category(url, cat_name)
            all_results.extend(data)
            print(f"[Done] Collected {len(data)} items from {cat_name}.")
        return all_results

async def scrape_all_dior_categories(categories_dict):
    scraper = DiorScraper()
    return await scraper.scrape_all(categories_dict)
