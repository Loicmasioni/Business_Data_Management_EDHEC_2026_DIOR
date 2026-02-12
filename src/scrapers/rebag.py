import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE = "https://shop.rebag.com"
COLLECTION = "https://shop.rebag.com/collections/christian-dior"

async def scrape_rebag_dior_plp(start_page=1, end_page=6):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
        )

        async def block_resources(route):
            if route.request.resource_type in ("image", "font", "media"):
                await route.abort()
            else:
                await route.continue_()
        await context.route("**/*", block_resources)

        page = await context.new_page()
        page.set_default_timeout(120000)
        page.set_default_navigation_timeout(120000)

        listings = []
        seen = set()

        for i in range(start_page, end_page + 1):
            url = f"{COLLECTION}?page={i}"
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_selector("span.products-carousel__card-title", timeout=30000)

            soup = BeautifulSoup(await page.content(), "html.parser")
            cards = soup.select('a[href*="/products/"]')

            for card in cards:
                href = card.get("href")
                if not href or "/products/" not in href:
                    continue

                link = href if href.startswith("http") else BASE + href
                link = link.split("?")[0]
                if link in seen:
                    continue

                brand_el = card.select_one("div.products-carousel__card-designer")
                name_el  = card.select_one("span.products-carousel__card-title")
                cond_el  = card.select_one("span.products-carousel__card-condition")
                price_el = card.select_one("span.rewards-plus-plp__product-price-value")

                brand = brand_el.get_text(strip=True) if brand_el else "Dior"
                name  = name_el.get_text(strip=True) if name_el else "N/A"
                condition = cond_el.get_text(strip=True) if cond_el else "N/A"
                price = price_el.get_text(strip=True) if price_el else "N/A"

                if name == "N/A" and price == "N/A":
                    continue

                seen.add(link)
                listings.append({
                    "Source": "Rebag",
                    "Marque": brand,
                    "Nom": name,
                    "Prix": price,
                    "Condition": condition,
                    "Lien": link
                })

        await browser.close()
        return listings


if __name__ == "__main__":
    data_resale = asyncio.run(scrape_rebag_dior_plp(start_page=1, end_page=2))
    
    df_resale_1 = pd.DataFrame(data_resale)
    df_resale_1 = df_resale_1.drop_duplicates(subset=["Lien"])
    print(f"Total Rebag items found: {len(df_resale_1)}")
    print(df_resale_1.head())