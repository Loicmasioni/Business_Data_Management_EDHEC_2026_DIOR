# Tests Directory

This directory contains test scripts for validating the data pipeline components.

## Test Files

- `test_main.py` - Integration test for the full pipeline (scraping → NLP → BigQuery)
- `test_scrapers.py` - Unit tests for individual scrapers (Dior, Vestiaire)

## Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test
python tests/test_scrapers.py
```
