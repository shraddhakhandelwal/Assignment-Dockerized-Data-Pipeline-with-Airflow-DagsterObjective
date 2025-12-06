# Dockerized Stock Market Data Pipeline with Apache Airflow

A production-ready, scalable data pipeline that automatically fetches stock market data from Alpha Vantage API and stores it in PostgreSQL using Apache Airflow for orchestration.

## 📋 Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Scaling](#scaling)
- [Contributing](#contributing)

## 🚀 Features

- **Automated Data Collection**: Scheduled fetching of stock market data (daily or hourly)
- **Robust Error Handling**: Comprehensive error management and retry logic
- **Scalable Architecture**: Docker-based deployment for easy scaling
- **Data Persistence**: PostgreSQL database with optimized schema
- **Pipeline Orchestration**: Apache Airflow for workflow management
- **Environment Security**: Secure management of API keys and credentials
- **Multiple Stock Support**: Fetch data for multiple stocks in parallel
- **Monitoring & Logging**: Complete pipeline execution tracking

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose                           │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Airflow    │  │   Airflow    │  │  PostgreSQL  │      │
│  │  Webserver   │  │  Scheduler   │  │   Database   │      │
│  │   :8080      │  │              │  │    :5432     │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │              │
│         └──────────────────┴──────────────────┘              │
│                            │                                 │
│                   ┌────────▼────────┐                        │
│                   │  Stock Data DAG │                        │
│                   │                 │                        │
│                   │ ┌─────────────┐ │                        │
│                   │ │ Fetch IBM   │ │                        │
│                   │ └─────────────┘ │                        │
│                   │ ┌─────────────┐ │                        │
│                   │ │ Fetch AAPL  │ │                        │
│                   │ └─────────────┘ │                        │
│                   │ ┌─────────────┐ │                        │
│                   │ │ Fetch GOOGL │ │                        │
│                   │ └─────────────┘ │                        │
│                   └─────────────────┘                        │
└─────────────────────────────────────────────────────────────┘
                            │
                    ┌───────▼────────┐
                    │ Alpha Vantage  │
                    │      API       │
                    └────────────────┘
```

## 📦 Prerequisites

Before you begin, ensure you have the following installed:

- **Docker**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher
- **Git**: For cloning the repository
- **Alpha Vantage API Key**: Free API key from [Alpha Vantage](https://www.alphavantage.co/support/#api-key)

### System Requirements

- **Memory**: Minimum 4GB RAM (8GB recommended)
- **CPU**: Minimum 2 cores
- **Disk Space**: Minimum 10GB free space

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd "New folder (6)"
```

### 2. Get Alpha Vantage API Key

1. Visit [Alpha Vantage](https://www.alphavantage.co/support/#api-key)
2. Sign up for a free API key
3. Copy your API key

### 3. Configure Environment Variables

Edit the `.env` file and add your API key:

```bash
# For Windows (PowerShell)
notepad .env

# Update this line with your actual API key:
STOCK_API_KEY=your_actual_api_key_here
```

**Important**: Replace `your_alpha_vantage_api_key_here` with your actual API key.

### 4. Set Airflow UID (Linux/Mac only)

```bash
# Linux/Mac
echo "AIRFLOW_UID=$(id -u)" >> .env
```

For Windows, the default `AIRFLOW_UID=50000` in `.env` is sufficient.

### 5. Start the Pipeline

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f
```

### 6. Access Airflow Web UI

1. Open your browser and navigate to: http://localhost:8080
2. Login with default credentials:
   - **Username**: `airflow`
   - **Password**: `airflow`

### 7. Enable the DAG

1. In the Airflow UI, find the DAG named `stock_data_pipeline`
2. Toggle the switch to enable it
3. Click the "Play" button to trigger a manual run

## ⚙️ Configuration

### Environment Variables

All configuration is done through the `.env` file:

```env
# Airflow Configuration
AIRFLOW_UID=50000                           # User ID for Airflow (Linux/Mac: use $(id -u))
_AIRFLOW_WWW_USER_USERNAME=airflow          # Airflow web UI username
_AIRFLOW_WWW_USER_PASSWORD=airflow          # Airflow web UI password

# Stock API Configuration
STOCK_API_KEY=your_alpha_vantage_api_key_here  # Your Alpha Vantage API key

# PostgreSQL Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=stockdata
POSTGRES_USER=stockuser
POSTGRES_PASSWORD=stockpass123
```

### Modifying Stock Symbols

To fetch different stocks, edit `dags/stock_data_pipeline.py`:

```python
# Add new fetch task
fetch_msft_task = PythonOperator(
    task_id='fetch_msft_data',
    python_callable=fetch_and_store_stock_data,
    op_kwargs={'symbol': 'MSFT', 'interval': 'daily'},
    dag=dag,
)

# Update dependencies
start_task >> validate_env_task >> [fetch_ibm_task, fetch_aapl_task, fetch_googl_task, fetch_msft_task] >> summary_task >> end_task
```

### Changing Schedule

Edit the `schedule_interval` in `dags/stock_data_pipeline.py`:

```python
dag = DAG(
    'stock_data_pipeline',
    schedule_interval='@hourly',  # Options: @hourly, @daily, @weekly, or cron expression
    ...
)
```

## 📖 Usage

### Running the Pipeline Manually

```bash
# Trigger the DAG via Airflow CLI
docker-compose exec airflow-scheduler airflow dags trigger stock_data_pipeline
```

### Viewing Pipeline Logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
docker-compose logs -f postgres
```

### Accessing the Database

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U stockuser -d stockdata

# Query stock data
SELECT * FROM stock_prices ORDER BY timestamp DESC LIMIT 10;

# Check fetch metadata
SELECT * FROM fetch_metadata ORDER BY fetch_timestamp DESC LIMIT 10;

# Get summary statistics
SELECT 
    symbol, 
    COUNT(*) as records, 
    MIN(timestamp) as earliest, 
    MAX(timestamp) as latest
FROM stock_prices 
GROUP BY symbol;
```

### Running the Fetcher Script Independently

```bash
# Run for IBM stock (daily data)
docker-compose exec airflow-scheduler python /opt/airflow/scripts/fetch_stock_data.py --symbol IBM --interval daily

# Run for Apple stock
docker-compose exec airflow-scheduler python /opt/airflow/scripts/fetch_stock_data.py --symbol AAPL --interval daily
```

## 📁 Project Structure

```
New folder (6)/
├── dags/
│   └── stock_data_pipeline.py       # Airflow DAG definition
├── scripts/
│   └── fetch_stock_data.py          # Data fetching script
├── db_init/
│   └── 01_create_stockdata_db.sql   # Database initialization
├── logs/                             # Airflow logs (auto-created)
├── plugins/                          # Airflow plugins (optional)
├── docker-compose.yml               # Docker services configuration
├── .env                             # Environment variables
├── .env.example                     # Example environment file
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

### Key Files

- **docker-compose.yml**: Defines all Docker services (Airflow, PostgreSQL)
- **dags/stock_data_pipeline.py**: Main DAG with pipeline logic
- **scripts/fetch_stock_data.py**: Core data fetching and storage logic
- **db_init/01_create_stockdata_db.sql**: Database schema initialization
- **.env**: Configuration and secrets

## 📊 Monitoring

### Airflow Web UI

Access at http://localhost:8080

- **DAG View**: See all DAGs and their status
- **Graph View**: Visualize task dependencies
- **Task Logs**: View detailed logs for each task
- **Gantt Chart**: See task execution timeline

### Database Monitoring

```sql
-- Recent fetch status
SELECT * FROM fetch_metadata 
ORDER BY fetch_timestamp DESC 
LIMIT 20;

-- Success rate by symbol
SELECT 
    symbol,
    COUNT(*) as total_fetches,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
    AVG(records_inserted) as avg_records
FROM fetch_metadata
GROUP BY symbol;

-- Latest stock prices
SELECT symbol, timestamp, close, volume
FROM stock_prices
WHERE timestamp >= NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC;
```

## 🔧 Troubleshooting

### Common Issues

#### 1. API Rate Limit Exceeded

**Error**: `Note: Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute`

**Solution**: 
- Free tier allows 5 API calls per minute
- Add delays between tasks or upgrade to premium tier
- Use daily interval instead of hourly

#### 2. Docker Permission Issues (Linux)

**Error**: `Permission denied` when accessing files

**Solution**:
```bash
# Set correct Airflow UID
echo "AIRFLOW_UID=$(id -u)" >> .env

# Fix permissions
sudo chown -R $(id -u):$(id -g) logs dags plugins
```

#### 3. Database Connection Failed

**Error**: `Could not connect to PostgreSQL`

**Solution**:
```bash
# Check if PostgreSQL is running
docker-compose ps

# Restart services
docker-compose restart postgres airflow-scheduler
```

#### 4. Airflow Webserver Not Accessible

**Error**: Cannot access http://localhost:8080

**Solution**:
```bash
# Check if service is running
docker-compose ps airflow-webserver

# View logs
docker-compose logs airflow-webserver

# Restart webserver
docker-compose restart airflow-webserver
```

### Debugging Tips

```bash
# Check service health
docker-compose ps

# View real-time logs
docker-compose logs -f

# Execute commands in Airflow container
docker-compose exec airflow-scheduler bash

# Check database connection
docker-compose exec postgres pg_isready -U airflow

# Reset environment (WARNING: deletes all data)
docker-compose down -v
docker-compose up -d
```

## 📈 Scaling

### Horizontal Scaling

To handle more stocks or higher frequency:

1. **Use CeleryExecutor** instead of LocalExecutor:

```yaml
# In docker-compose.yml
environment:
  AIRFLOW__CORE__EXECUTOR: CeleryExecutor
  
# Add Redis and Celery workers
redis:
  image: redis:7
  
airflow-worker:
  <<: *airflow-common
  command: celery worker
```

2. **Increase parallel tasks**:

```python
# In dag
default_args = {
    'max_active_runs': 3,
}
```

### Vertical Scaling

```yaml
# In docker-compose.yml, add resource limits
services:
  postgres:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
```

### Performance Optimization

1. **Database Indexing**: Already included in initialization script
2. **Connection Pooling**: Configure in Airflow settings
3. **Batch Processing**: Process multiple stocks in single task
4. **Caching**: Cache API responses for development

## 🔒 Security Best Practices

1. **Never commit `.env` file** to version control
2. **Change default passwords** in production
3. **Use secrets management** for production (AWS Secrets Manager, HashiCorp Vault)
4. **Enable SSL/TLS** for database connections in production
5. **Regular security updates**: `docker-compose pull` to get latest images

## 🧪 Testing

```bash
# Test data fetcher independently
docker-compose exec airflow-scheduler python /opt/airflow/scripts/fetch_stock_data.py --symbol IBM

# Test database connection
docker-compose exec postgres psql -U stockuser -d stockdata -c "SELECT COUNT(*) FROM stock_prices;"

# Run DAG validation
docker-compose exec airflow-scheduler airflow dags test stock_data_pipeline 2024-01-01
```

## 🛑 Stopping the Pipeline

```bash
# Stop all services (keeps data)
docker-compose down

# Stop and remove all data
docker-compose down -v

# Stop specific service
docker-compose stop airflow-scheduler
```

## 📝 API Rate Limits

**Alpha Vantage Free Tier**:
- 5 API calls per minute
- 500 API calls per day

**Recommendations**:
- Use daily interval for free tier
- For hourly data, upgrade to premium or use multiple API keys
- Implement exponential backoff for rate limiting

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

For issues and questions:
- Check the [Troubleshooting](#troubleshooting) section
- Review [Airflow Documentation](https://airflow.apache.org/docs/)
- Check [Alpha Vantage API Docs](https://www.alphavantage.co/documentation/)

## 🎯 Next Steps

After successful deployment:

1. ✅ Verify pipeline runs successfully
2. ✅ Monitor first few runs in Airflow UI
3. ✅ Check data in PostgreSQL
4. ✅ Set up alerting (email/Slack)
5. ✅ Configure backup strategy
6. ✅ Plan for production deployment

---

**Built with ❤️ using Apache Airflow, Docker, and PostgreSQL**
