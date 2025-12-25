# LLM Bot Traffic Analysis Pipeline
# Multi-provider CDN log ingestion and analysis

FROM python:3.12-slim

# Install minimal system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Install the package in development mode
RUN pip install -e .

# Create data directory
RUN mkdir -p /app/data

# Default command
CMD ["bash"]
