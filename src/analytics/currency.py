import os
import re
from typing import Dict

import httpx
import pandas as pd


SYMBOL_TO_CCY = {
    "€": "EUR",
    "$": "USD",
    "£": "GBP",
    "¥": "JPY",
}


def infer_currency_from_text(price_text: str, fallback: str = "EUR") -> str:
    if not isinstance(price_text, str):
        return fallback
    for symbol, ccy in SYMBOL_TO_CCY.items():
        if symbol in price_text:
            return ccy
    upper = price_text.upper()
    for ccy in ("EUR", "USD", "GBP", "JPY", "CHF", "AED", "CNY", "KRW", "SGD"):
        if ccy in upper:
            return ccy
    return fallback


def parse_price_to_float(value) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.upper() == "N/A":
        return 0.0
    text = text.replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text:
        return 0.0
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        if text.count(",") == 1:
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


async def fetch_rates_to_eur(currencies) -> Dict[str, float]:
    rates = {"EUR": 1.0}
    api_key = os.getenv("EXCHANGE_RATE_API_KEY")
    unique_ccy = sorted({str(c).upper() for c in currencies if c})
    non_eur = [c for c in unique_ccy if c != "EUR"]
    if not api_key and non_eur:
        raise RuntimeError("EXCHANGE_RATE_API_KEY is missing and non-EUR prices were detected.")
    if not api_key:
        return rates

    async with httpx.AsyncClient(timeout=30.0) as client:
        for ccy in unique_ccy:
            if ccy == "EUR":
                continue
            url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{ccy}"
            res = await client.get(url)
            if res.status_code != 200:
                continue
            payload = res.json()
            ccy_rates = payload.get("conversion_rates", {})
            eur_rate = ccy_rates.get("EUR")
            if eur_rate:
                rates[ccy] = float(eur_rate)
    missing = [c for c in non_eur if c not in rates]
    if missing:
        raise RuntimeError(f"Missing FX rates to EUR for currencies: {', '.join(missing)}")
    return rates


async def normalize_prices_to_eur(
    df: pd.DataFrame,
    price_col: str = "retail_price",
    currency_col: str = "currency",
) -> pd.DataFrame:
    if df.empty or price_col not in df.columns:
        return df

    if currency_col not in df.columns:
        df[currency_col] = None

    df = df.copy()
    df[currency_col] = df[currency_col].fillna("").astype(str).str.upper()
    inferred = df.apply(
        lambda row: row[currency_col] if row[currency_col] else infer_currency_from_text(row[price_col]),
        axis=1,
    )
    df[currency_col] = inferred

    df["retail_price_num"] = df[price_col].apply(parse_price_to_float)
    rates = await fetch_rates_to_eur(df[currency_col].tolist())
    df["fx_rate_to_eur"] = df[currency_col].apply(lambda c: rates.get(str(c).upper(), 1.0))
    df["retail_price_eur"] = (df["retail_price_num"] * df["fx_rate_to_eur"]).round(2)

    # Keep the canonical pipeline currency as EUR on every scrape.
    df[price_col] = df["retail_price_eur"]
    df[currency_col] = "EUR"
    return df
