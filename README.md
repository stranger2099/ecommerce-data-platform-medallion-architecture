# E-Commerce Data Platform with Medallion Architecture

> A production-ready, cloud-native data platform built on Databricks, implementing the Medallion Architecture (Bronze-Silver-Gold) for end-to-end e-commerce analytics with advanced customer segmentation and product performance insights.

## Overview

This project demonstrates a complete modern data engineering solution using **Delta Live Tables (DLT)** on **Databricks** to process e-commerce supply chain data through a multi-layered medallion architecture. The platform delivers actionable business intelligence through RFM analysis, customer 360 views, and product performance metrics.

## Dataset

This project uses the **DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS** dataset:

**Citation:**
> Constante, Fabian; Silva, Fernando; Pereira, António (2019), "DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS", Mendeley Data, V5, doi: 10.17632/8gx2fvg2k6.5

The dataset is available at: https://data.mendeley.com/datasets/8gx2fvg2k6/5

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     RAW DATA INGESTION                            │
│              CSV Files → Cloud Files Format                       │
└───────────────────────────┬──────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER - Raw Data with Quality Checks                     │
│  • Schema enforcement (61 fields)                                 │
│  • Data quality expectations                                      │
│  • Streaming ingestion                                            │
└───────────────────────────┬──────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  SILVER LAYER - Cleansed & Conformed                             │
│  ├─ Customers Dimension (SCD Type 2)                             │
│  ├─ Products Dimension (SCD Type 2)                              │
│  └─ Orders Fact Table                                             │
└───────────────────────────┬──────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  GOLD LAYER - Analytics & Business Intelligence                  │
│  ├─ Customer 360 (RFM Analysis, 8 Segments)                      │
│  ├─ Product 360 (Performance Tiers, Health Scoring)              │
│  └─ Enriched Orders (Denormalized Analytics View)                │
└──────────────────────────────────────────────────────────────────┘
```

## Key Features

- **Medallion Architecture**: Implements industry-standard Bronze → Silver → Gold data layers
- **Streaming ETL**: Real-time data ingestion using CloudFiles and Delta Live Tables
- **Data Quality**: DLT expectations with expect_or_drop policies for data validation
- **Change Data Capture**: SCD Type 2 implementation for dimension tables using auto CDC
- **Advanced Analytics**:
  - RFM (Recency, Frequency, Monetary) customer segmentation
  - 8-tier customer classification (Champions, Loyal, At Risk, etc.)
  - Product performance metrics with strategic prioritization
- **Infrastructure as Code**: Full deployment automation with Databricks Asset Bundles
- **Performance Optimized**: Photon acceleration and serverless compute enabled
- **Synthetic Data Generator**: Built-in data generation for testing and demonstration

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Cloud Platform** | Databricks |
| **Data Processing** | PySpark, Delta Lake, Delta Live Tables |
| **Orchestration** | Databricks Workflows, DLT Pipelines |
| **Language** | Python 3.10+ |
| **Storage Format** | Delta Lake (Parquet + Transaction Log) |
| **Compute** | Serverless, Photon-accelerated |
| **CI/CD** | Databricks Asset Bundles, GitHub |
| **Testing** | pytest, Databricks Connect |
| **Package Management** | uv |

## Quick Start

### Prerequisites

- Databricks workspace (Community Edition or Enterprise)
- Python 3.10 or higher
- [uv package manager](https://docs.astral.sh/uv/getting-started/installation/)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ecommerce-data-platform-medallion-architecture/DLT_Ecommerce
   ```

2. **Install dependencies**
   ```bash
   uv sync --dev
   ```

3. **Configure Databricks authentication**
   ```bash
   databricks configure
   ```

### Deployment

**Development Environment:**
```bash
# Deploy to dev workspace
databricks bundle deploy --target dev

# Run the complete pipeline (data generation + ETL)
databricks bundle run
```

**Production Environment:**
```bash
# Deploy to production
databricks bundle deploy --target prod
```

### Testing

```bash
# Run local tests
uv run pytest
```

## Project Structure

```
DLT_Ecommerce/
├── databricks.yml                    # Bundle configuration (dev/prod targets)
├── pyproject.toml                    # Python dependencies
├── resources/
│   ├── pipeline.yml                  # DLT pipeline definition
│   └── job.yml                       # Databricks job workflow
├── src/
│   ├── syntheticloads.ipynb         # Synthetic data generator
│   └── ecomm_e2e_ingestion/
│       ├── transformations/
│       │   ├── bronze_work/
│       │   │   └── bronze_layer.py   # Raw data ingestion
│       │   ├── silver_work/
│       │   │   └── silver_layer.py   # Data cleansing & SCD-2
│       │   └── gold_work/
│       │       └── gold_layer.py     # Analytics & aggregations
│       └── utilities/
│           └── utils.py              # Shared utilities
└── fixtures/                         # Test fixtures
```

## Data Pipeline Details

### Bronze Layer (`bronze_layer.py`)
- **Input**: CSV files from cloud storage volumes
- **Processing**:
  - Schema validation (61 fields)
  - Column name normalization
  - Date parsing and type conversions
  - Data quality checks (non-null, positive values)
- **Output**: `bronze_complete` table

### Silver Layer (`silver_layer.py`)
- **Input**: Bronze layer tables (streaming)
- **Processing**:
  - Business entity extraction (Customers, Products, Orders)
  - Column renaming for semantic clarity
  - SCD Type 2 for dimensions (tracks historical changes)
- **Output**:
  - `silver_customers` + `dimSilverCustomers` (SCD-2)
  - `silver_products` + `dimSilverProducts` (SCD-2)
  - `silver_orders` (fact table)

### Gold Layer (`gold_layer.py`)
- **Input**: Silver layer tables (batch)
- **Processing**:
  - Multi-table joins and enrichment
  - RFM scoring calculation
  - Customer and product segmentation
- **Output**:
  - `orders_enriched`: Denormalized view with all dimensions
  - `customers360`: RFM analysis with 8-tier segmentation
  - `products360`: Performance metrics with strategic classification

## Business Analytics

### Customer 360 View
- **RFM Scoring**: Recency, Frequency, Monetary analysis (1-5 scale)
- **Segmentation**: Champions, Loyal Customers, Potential Loyalists, New Customers, At Risk, Cannot Lose Them, Hibernating, Lost, Prospects
- **Metrics**: Total orders, revenue, profit, AOV, days since last order

### Product 360 View
- **Performance Tiers**: Star, Strong, Steady, Weak, Slow Movers
- **Margin Analysis**: High, Medium, Low, Very Low
- **Health Scoring**: Healthy, At Risk, Monitor
- **Strategic Priorities**: Growth opportunity identification

## Development Workflow

### Local Development
```bash
# Option 1: Databricks Workspace IDE
# Use the web-based notebook interface

# Option 2: VS Code with Databricks Extension
# Install Databricks extension and connect to workspace

# Option 3: CLI-based development
databricks bundle validate          # Validate configuration
databricks bundle deploy --target dev
databricks bundle run
```

### Monitoring Pipeline
- Navigate to **Workflows → Delta Live Tables** in Databricks
- View pipeline graph, data quality metrics, and lineage
- Monitor streaming ingestion rates and processing times

## Configuration

### Pipeline Configuration (`resources/pipeline.yml`)
- **Catalog**: `ecomm_e2e`
- **Schema**: `ecomm_pipeline`
- **Compute**: Serverless with Photon enabled
- **Channel**: CURRENT (latest DLT features)

### Databricks Bundle (`databricks.yml`)
- **Dev Target**: Development workspace with isolated resources
- **Prod Target**: Production deployment with separate root path

## Data Quality

The platform implements comprehensive data quality checks:

| Layer | Quality Checks |
|-------|---------------|
| **Bronze** | Non-null validation, positive values, schema enforcement |
| **Silver** | Referential integrity, deduplication, SCD-2 consistency |
| **Gold** | Aggregation validation, business rule compliance |


## Future Enhancements

- [ ] Real-time streaming dashboard integration
- [ ] Automated alerting for anomaly detection

## Contributing

This is a portfolio project demonstrating data engineering best practices. Suggestions and feedback are welcome!

## License

This project is for educational and portfolio purposes.


---

*Built with ❤️ using Databricks, PySpark, and Delta Lake*
