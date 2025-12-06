-- Create separate database for stock data
CREATE DATABASE stockdata;

-- Create user for stock data access
CREATE USER stockuser WITH PASSWORD 'stockpass123';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE stockdata TO stockuser;

-- Connect to stockdata database
\c stockdata

-- Grant schema privileges
GRANT ALL ON SCHEMA public TO stockuser;

-- Create stock_prices table
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DECIMAL(10, 2),
    high DECIMAL(10, 2),
    low DECIMAL(10, 2),
    close DECIMAL(10, 2),
    volume BIGINT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp)
);

-- Create index for faster queries
CREATE INDEX idx_symbol_timestamp ON stock_prices(symbol, timestamp DESC);
CREATE INDEX idx_fetched_at ON stock_prices(fetched_at DESC);

-- Grant table privileges to stockuser
GRANT ALL PRIVILEGES ON TABLE stock_prices TO stockuser;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO stockuser;

-- Create a metadata table to track API fetch status
CREATE TABLE IF NOT EXISTS fetch_metadata (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    fetch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    records_inserted INTEGER DEFAULT 0,
    error_message TEXT,
    api_calls_remaining INTEGER
);

CREATE INDEX idx_fetch_metadata_symbol ON fetch_metadata(symbol, fetch_timestamp DESC);

GRANT ALL PRIVILEGES ON TABLE fetch_metadata TO stockuser;
