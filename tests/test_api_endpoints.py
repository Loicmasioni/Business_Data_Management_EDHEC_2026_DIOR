import pandas as pd
from fastapi.testclient import TestClient
import types
import sys

import api.main as main


class FakeBigQueryClient:
    def __init__(self, *args, **kwargs):
        pass

    def get_dior_data(self, dataset_id, table_id, limit):
        return pd.DataFrame(
            [
                {
                    "product_name": "Lady Dior",
                    "retail_product_id": "ABC123",
                    "retail_price": 5000,
                    "category": "Bags",
                    "scrape_date": "2026-02-12",
                    "product_url": "https://example.com/item",
                }
            ]
        )

    def query_to_dataframe(self, query):
        if "price_eur_formatted" in query:
            return pd.DataFrame(
                [
                    {
                        "product_name": "Lady Dior",
                        "retail_product_id": "ABC123",
                        "category": "Bags",
                        "retail_price": "5000 €",
                        "currency": "EUR",
                        "price_eur": 5000.0,
                        "price_eur_formatted": "€5000.00",
                        "Source": "Dior",
                        "scrape_date": "2026-02-12",
                        "product_url": "https://example.com/item",
                    }
                ]
            )
        if "GROUP BY Source" in query:
            return pd.DataFrame(
                [{"Source": "Dior", "count": 10}, {"Source": "Rebag", "count": 4}]
            )
        return pd.DataFrame(
            [{"category": "Bags", "avg_retail": 5000.0, "avg_resale": 3800.0}]
        )


async def fake_async_noop(*args, **kwargs):
    return None


class FakeHttpResponse:
    status_code = 200

    def json(self):
        return {"base_code": "USD", "conversion_rates": {"EUR": 0.92}}


class FakeAsyncClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url):
        return FakeHttpResponse()


def test_root_endpoint():
    client = TestClient(main.app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "API is running"}


def test_health_endpoint():
    client = TestClient(main.app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_data_dior_endpoint(monkeypatch):
    monkeypatch.setattr(main, "BigQueryClient", FakeBigQueryClient)
    client = TestClient(main.app)

    response = client.get("/data/dior?limit=5")
    assert response.status_code == 200

    payload = response.json()
    assert isinstance(payload, list)
    assert payload[0]["product_name"] == "Lady Dior"


def test_data_dior_not_found_returns_404(monkeypatch):
    class EmptyDataBigQueryClient(FakeBigQueryClient):
        def query_to_dataframe(self, query):
            return pd.DataFrame()

    monkeypatch.setattr(main, "BigQueryClient", EmptyDataBigQueryClient)
    client = TestClient(main.app)

    response = client.get("/data/dior?limit=5")
    assert response.status_code == 404
    assert response.json()["detail"] == "No Dior data found in dior_data_final"


def test_analytics_summary_endpoint(monkeypatch):
    monkeypatch.setattr(main, "BigQueryClient", FakeBigQueryClient)
    client = TestClient(main.app)

    response = client.get("/analytics/summary")
    assert response.status_code == 200
    assert response.json() == [{"Source": "Dior", "count": 10}, {"Source": "Rebag", "count": 4}]


def test_analytics_brand_premium_endpoint(monkeypatch):
    monkeypatch.setattr(main, "BigQueryClient", FakeBigQueryClient)
    client = TestClient(main.app)

    response = client.get("/analytics/brand-premium")
    assert response.status_code == 200
    assert response.json() == [{"category": "Bags", "avg_retail": 5000.0, "avg_resale": 3800.0}]


def test_exchange_rate_endpoint(monkeypatch):
    monkeypatch.setattr(main, "BigQueryClient", FakeBigQueryClient)
    monkeypatch.setattr("httpx.AsyncClient", FakeAsyncClient)
    monkeypatch.setenv("EXCHANGE_RATE_API_KEY", "fake-key")
    client = TestClient(main.app)

    response = client.get("/tools/exchange-rate?base=USD&target=EUR")
    assert response.status_code == 200
    assert response.json() == {
        "base": "USD",
        "target": "EUR",
        "rate": 0.92,
        "provider": "ExchangeRate-API",
    }


def test_trigger_dior_scrape_endpoint(monkeypatch):
    monkeypatch.setattr(main.dior_scraper, "scrape_all", fake_async_noop)
    client = TestClient(main.app)

    response = client.post("/scrape/dior")
    assert response.status_code == 200
    assert response.json()["message"] == "Dior scrape triggered in background"


def test_trigger_vestiaire_scrape_endpoint(monkeypatch):
    monkeypatch.setattr(main, "BigQueryClient", FakeBigQueryClient)
    monkeypatch.setattr(main.vestiaire_scraper, "scrape_all_from_df", fake_async_noop)
    client = TestClient(main.app)

    response = client.post("/scrape/vestiaire")
    assert response.status_code == 200
    assert response.json()["message"] == "Vestiaire scrape triggered in background for 10 products"


def test_pipeline_run_endpoint(monkeypatch):
    fake_module = types.ModuleType("run_pipeline")

    async def _fake_pipeline():
        return None

    fake_module.run_full_analytical_pipeline = _fake_pipeline
    monkeypatch.setitem(sys.modules, "run_pipeline", fake_module)
    client = TestClient(main.app)

    response = client.post("/pipeline/run")
    assert response.status_code == 200
    assert response.json() == {"message": "Full analytical pipeline started in background"}
