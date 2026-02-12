#!/bin/bash

# Dior Data Management Project Setup Script
# Ideal for clean installations

echo "--- Dior Project Flawless Setup ---"

# 1. Environment Check
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed."
    exit 1
fi

# 2. Virtual Environment
echo "[1/4] Creating Virtual Environment..."
python3 -m venv venv
source venv/bin/activate

# 3. Dependencies
echo "[2/4] Installing dependencies..."
pip install --upgrade pip
pip install -e .
pip install transformers torch scikit-learn tqdm # NLP Stack

# 4. Playwright Setup
echo "[3/4] Setting up Playwright..."
playwright install chromium

# 5. Environment Config
echo "[4/4] Configuring environment..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "(!) Created .env file. Please update it with your BigQuery credentials."
else
    echo "(!) .env file already exists."
fi

# 6. Final check
echo "--- Setup Complete ---"
echo "To activate the environment: source venv/bin/activate"
echo "To run the full pipeline:    python test_main.py"
echo "To start the API:           uvicorn api.main:app --reload"
