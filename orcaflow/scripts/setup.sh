#!/bin/bash

# OrcaFlow Project Setup Script

set -e

echo "🚀 OrcaFlow Project Setup"
echo "=========================="

# Create Python virtual environment
echo "📦 Setting up Python environment..."
python -m venv venv

# Activate venv
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Install API dependencies
echo "📦 Installing API dependencies..."
pip install -r orcaflow/api/requirements.txt

# Install ML classifier dependencies
echo "📦 Installing ML classifier dependencies..."
pip install -r orcaflow/ml-classifier/requirements.txt

# Train ML model
echo "🧠 Training ML classifier..."
python orcaflow/ml-classifier/train.py

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p orcaflow/models
mkdir -p orcaflow/logs
mkdir -p data/{input,output}

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Start docker-compose: docker-compose up -d"
echo "2. Start API: python orcaflow/api/main.py"
echo "3. Access dashboard: http://localhost:8000/docs"
echo ""
