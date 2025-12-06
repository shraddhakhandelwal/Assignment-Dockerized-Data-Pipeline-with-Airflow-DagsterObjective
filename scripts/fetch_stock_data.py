"""
Stock Market Data Fetcher
Fetches stock data from Alpha Vantage API and stores it in PostgreSQL
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import requests
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockDataFetcher:
    """Handles fetching and storing stock market data"""
    
    def __init__(self):
        """Initialize the fetcher with configuration from environment variables"""
        self.api_key = os.getenv('STOCK_API_KEY')
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'stockdata'),
            'user': os.getenv('POSTGRES_USER', 'stockuser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'stockpass123')
        }
        self.base_url = 'https://www.alphavantage.co/query'
        
        # Validate configuration
        if not self.api_key or self.api_key == 'your_alpha_vantage_api_key_here':
            logger.warning("API key not configured properly. Using demo mode.")
            self.api_key = 'demo'
    
    def fetch_stock_data(self, symbol: str = 'IBM', interval: str = 'daily') -> Optional[Dict]:
        """
        Fetch stock data from Alpha Vantage API
        
        Args:
            symbol: Stock symbol (e.g., 'IBM', 'AAPL')
            interval: Time interval ('daily' or '60min' for hourly)
        
        Returns:
            Dictionary containing stock data or None if error
        """
        try:
            # Determine function based on interval
            if interval == 'daily':
                function = 'TIME_SERIES_DAILY'
                time_key = 'Time Series (Daily)'
            else:
                function = 'TIME_SERIES_INTRADAY'
                time_key = f'Time Series ({interval})'
            
            params = {
                'function': function,
                'symbol': symbol,
                'apikey': self.api_key,
                'outputsize': 'compact'  # Get last 100 data points
            }
            
            if interval != 'daily':
                params['interval'] = interval
            
            logger.info(f"Fetching {interval} data for {symbol}...")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
            
            if 'Note' in data:
                logger.warning(f"API Note: {data['Note']}")
                return None
            
            if time_key not in data:
                logger.error(f"Unexpected API response format: {list(data.keys())}")
                return None
            
            logger.info(f"Successfully fetched data for {symbol}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing API response: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in fetch_stock_data: {str(e)}")
            return None
    
    def parse_stock_data(self, data: Dict, symbol: str, interval: str = 'daily') -> List[Tuple]:
        """
        Parse stock data from API response
        
        Args:
            data: API response data
            symbol: Stock symbol
            interval: Time interval
        
        Returns:
            List of tuples containing parsed data
        """
        try:
            if interval == 'daily':
                time_key = 'Time Series (Daily)'
            else:
                time_key = f'Time Series ({interval})'
            
            time_series = data.get(time_key, {})
            
            if not time_series:
                logger.warning("No time series data found in response")
                return []
            
            parsed_data = []
            
            for timestamp, values in time_series.items():
                try:
                    # Parse timestamp
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S' if ' ' in timestamp else '%Y-%m-%d')
                    
                    # Extract values with error handling for missing data
                    open_price = self._safe_decimal(values.get('1. open'))
                    high_price = self._safe_decimal(values.get('2. high'))
                    low_price = self._safe_decimal(values.get('3. low'))
                    close_price = self._safe_decimal(values.get('4. close'))
                    volume = self._safe_int(values.get('5. volume'))
                    
                    parsed_data.append((
                        symbol,
                        dt,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume
                    ))
                    
                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing data point for {timestamp}: {str(e)}")
                    continue
            
            logger.info(f"Parsed {len(parsed_data)} data points for {symbol}")
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing stock data: {str(e)}")
            return []
    
    def _safe_decimal(self, value: Optional[str]) -> Optional[float]:
        """Safely convert string to decimal, return None if invalid"""
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value: Optional[str]) -> Optional[int]:
        """Safely convert string to integer, return None if invalid"""
        try:
            return int(value) if value else None
        except (ValueError, TypeError):
            return None
    
    def store_stock_data(self, parsed_data: List[Tuple], symbol: str) -> Tuple[bool, int, Optional[str]]:
        """
        Store parsed stock data in PostgreSQL database
        
        Args:
            parsed_data: List of tuples containing stock data
            symbol: Stock symbol
        
        Returns:
            Tuple of (success, records_inserted, error_message)
        """
        conn = None
        cursor = None
        records_inserted = 0
        
        try:
            # Connect to database
            logger.info(f"Connecting to database at {self.db_config['host']}...")
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            if not parsed_data:
                logger.warning("No data to insert")
                self._log_fetch_metadata(cursor, symbol, 'NO_DATA', 0, "No data points to insert")
                conn.commit()
                return True, 0, "No data to insert"
            
            # Insert data using ON CONFLICT to handle duplicates
            insert_query = """
                INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol, timestamp) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    fetched_at = CURRENT_TIMESTAMP
            """
            
            execute_values(cursor, insert_query, parsed_data)
            records_inserted = cursor.rowcount
            
            # Log metadata
            self._log_fetch_metadata(cursor, symbol, 'SUCCESS', records_inserted, None)
            
            conn.commit()
            logger.info(f"Successfully inserted/updated {records_inserted} records for {symbol}")
            
            return True, records_inserted, None
            
        except psycopg2.Error as e:
            error_msg = f"Database error: {str(e)}"
            logger.error(error_msg)
            
            if conn:
                conn.rollback()
            
            # Try to log the error
            try:
                if cursor:
                    self._log_fetch_metadata(cursor, symbol, 'ERROR', 0, error_msg)
                    conn.commit()
            except Exception as log_error:
                logger.error(f"Failed to log error metadata: {str(log_error)}")
            
            return False, 0, error_msg
            
        except Exception as e:
            error_msg = f"Unexpected error storing data: {str(e)}"
            logger.error(error_msg)
            
            if conn:
                conn.rollback()
            
            return False, 0, error_msg
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                logger.info("Database connection closed")
    
    def _log_fetch_metadata(self, cursor, symbol: str, status: str, 
                           records_inserted: int, error_message: Optional[str]):
        """Log fetch metadata to database"""
        try:
            cursor.execute("""
                INSERT INTO fetch_metadata (symbol, status, records_inserted, error_message)
                VALUES (%s, %s, %s, %s)
            """, (symbol, status, records_inserted, error_message))
        except Exception as e:
            logger.error(f"Error logging metadata: {str(e)}")
    
    def run(self, symbol: str = 'IBM', interval: str = 'daily') -> Dict:
        """
        Main execution method to fetch and store stock data
        
        Args:
            symbol: Stock symbol to fetch
            interval: Time interval ('daily' or '60min')
        
        Returns:
            Dictionary with execution results
        """
        logger.info(f"Starting stock data fetch for {symbol} ({interval})")
        
        result = {
            'symbol': symbol,
            'interval': interval,
            'success': False,
            'records_inserted': 0,
            'error': None,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # Fetch data from API
            data = self.fetch_stock_data(symbol, interval)
            
            if not data:
                result['error'] = "Failed to fetch data from API"
                logger.error(result['error'])
                return result
            
            # Parse data
            parsed_data = self.parse_stock_data(data, symbol, interval)
            
            if not parsed_data:
                result['error'] = "No valid data parsed from API response"
                logger.warning(result['error'])
                # Still return success as API call worked
                result['success'] = True
                return result
            
            # Store data
            success, records_inserted, error = self.store_stock_data(parsed_data, symbol)
            
            result['success'] = success
            result['records_inserted'] = records_inserted
            result['error'] = error
            
            if success:
                logger.info(f"Pipeline completed successfully: {records_inserted} records processed")
            else:
                logger.error(f"Pipeline failed: {error}")
            
            return result
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            logger.error(error_msg)
            result['error'] = error_msg
            return result


def main():
    """Main entry point for script execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch and store stock market data')
    parser.add_argument('--symbol', default='IBM', help='Stock symbol (default: IBM)')
    parser.add_argument('--interval', default='daily', 
                       choices=['daily', '60min'],
                       help='Data interval (default: daily)')
    
    args = parser.parse_args()
    
    fetcher = StockDataFetcher()
    result = fetcher.run(symbol=args.symbol, interval=args.interval)
    
    # Print result as JSON for easy parsing
    print(json.dumps(result, indent=2))
    
    # Exit with appropriate code
    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
