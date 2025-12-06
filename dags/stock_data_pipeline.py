"""
Airflow DAG for Stock Market Data Pipeline
Fetches stock data from Alpha Vantage API and stores in PostgreSQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import sys
import os
import logging

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from fetch_stock_data import StockDataFetcher

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
}

# DAG definition
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetch and store stock market data from Alpha Vantage API',
    schedule_interval='@daily',  # Run daily, can be changed to '@hourly' for hourly execution
    start_date=days_ago(1),
    catchup=False,
    tags=['stock', 'data-pipeline', 'alpha-vantage'],
)


def fetch_and_store_stock_data(symbol: str, interval: str = 'daily', **context):
    """
    Task function to fetch and store stock data
    
    Args:
        symbol: Stock symbol to fetch
        interval: Data interval (daily or 60min)
        context: Airflow context
    """
    logger.info(f"Starting data fetch for {symbol} with interval {interval}")
    
    try:
        # Initialize fetcher
        fetcher = StockDataFetcher()
        
        # Run the pipeline
        result = fetcher.run(symbol=symbol, interval=interval)
        
        # Push result to XCom for monitoring
        context['task_instance'].xcom_push(key='fetch_result', value=result)
        
        # Log result
        if result['success']:
            logger.info(f"Successfully processed {result['records_inserted']} records for {symbol}")
        else:
            logger.error(f"Failed to process data for {symbol}: {result.get('error')}")
            raise Exception(f"Data fetch failed: {result.get('error')}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in fetch_and_store_stock_data: {str(e)}")
        raise


def validate_environment(**context):
    """
    Validate that all required environment variables are set
    """
    logger.info("Validating environment configuration...")
    
    required_vars = [
        'STOCK_API_KEY',
        'POSTGRES_HOST',
        'POSTGRES_DB',
        'POSTGRES_USER',
        'POSTGRES_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Check if API key is placeholder
    api_key = os.getenv('STOCK_API_KEY')
    if api_key == 'your_alpha_vantage_api_key_here':
        logger.warning("API key appears to be a placeholder. Using demo mode.")
    
    logger.info("Environment validation passed")
    return True


def log_pipeline_summary(**context):
    """
    Log summary of pipeline execution
    """
    ti = context['task_instance']
    
    # Get results from all fetch tasks
    results = {}
    for task_id in ['fetch_ibm_data', 'fetch_aapl_data', 'fetch_googl_data']:
        try:
            result = ti.xcom_pull(task_ids=task_id, key='fetch_result')
            if result:
                results[task_id] = result
        except Exception as e:
            logger.warning(f"Could not retrieve result for {task_id}: {str(e)}")
    
    # Log summary
    logger.info("=" * 50)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 50)
    
    total_records = 0
    successful_tasks = 0
    failed_tasks = 0
    
    for task_id, result in results.items():
        symbol = result.get('symbol', 'Unknown')
        success = result.get('success', False)
        records = result.get('records_inserted', 0)
        error = result.get('error')
        
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{task_id} ({symbol}): {status} - {records} records")
        
        if error:
            logger.info(f"  Error: {error}")
        
        if success:
            successful_tasks += 1
            total_records += records
        else:
            failed_tasks += 1
    
    logger.info("-" * 50)
    logger.info(f"Total Records Inserted: {total_records}")
    logger.info(f"Successful Tasks: {successful_tasks}/{len(results)}")
    logger.info(f"Failed Tasks: {failed_tasks}/{len(results)}")
    logger.info("=" * 50)
    
    return {
        'total_records': total_records,
        'successful_tasks': successful_tasks,
        'failed_tasks': failed_tasks
    }


# Define tasks
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

validate_env_task = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment,
    dag=dag,
)

# Create fetch tasks for multiple stocks
fetch_ibm_task = PythonOperator(
    task_id='fetch_ibm_data',
    python_callable=fetch_and_store_stock_data,
    op_kwargs={'symbol': 'IBM', 'interval': 'daily'},
    dag=dag,
)

fetch_aapl_task = PythonOperator(
    task_id='fetch_aapl_data',
    python_callable=fetch_and_store_stock_data,
    op_kwargs={'symbol': 'AAPL', 'interval': 'daily'},
    dag=dag,
)

fetch_googl_task = PythonOperator(
    task_id='fetch_googl_data',
    python_callable=fetch_and_store_stock_data,
    op_kwargs={'symbol': 'GOOGL', 'interval': 'daily'},
    dag=dag,
)

summary_task = PythonOperator(
    task_id='log_pipeline_summary',
    python_callable=log_pipeline_summary,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> validate_env_task >> [fetch_ibm_task, fetch_aapl_task, fetch_googl_task] >> summary_task >> end_task
