#!/bin/bash
# Setup virtual environment for YouTube Ingestion Tool

echo "🔧 Setting up virtual environment..."

# Create venv if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate venv
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "To activate venv manually:"
echo "  source venv/bin/activate"
echo ""
echo "Then run:"
echo "  python3 ingest.py"

