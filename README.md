# ETL-Pipeline-PySpark

## Overview
This repository contains a sample ETL (Extract, Transform, Load) pipeline implementation using Apache PySpark. The project demonstrates best practices for building scalable data processing pipelines for Data Engineering workflows.

## Features
- **Extract**: Read data from various sources (CSV, Parquet, JSON)
- **Transform**: Data cleaning, filtering, and aggregation operations
- **Load**: Write processed data to target destinations with partitioning
- **Schema Definition**: Strongly-typed data schemas for data quality
- **Customer Tiering**: Business logic implementation for customer segmentation

## Technologies Used
- Apache Spark / PySpark
- Python 3.x
- Distributed Data Processing

## Project Structure
```
ETL-Pipeline-PySpark/
├── README.md
└── etl_pipeline.py    # Main ETL pipeline implementation
```

## Key Components

### Data Extraction
The pipeline reads data from CSV files with predefined schemas to ensure data quality and type safety.

### Data Transformation
- Null value filtering
- Data type conversions
- String normalization (trimming, uppercase)
- Customer tier classification based on purchase amounts
- Regional aggregations for business insights

### Data Loading
- Writes transformed data in Parquet format
- Implements partition strategies for optimized querying
- Supports overwrite and append modes

## Usage

```bash
# Run the ETL pipeline
spark-submit etl_pipeline.py
```

## Data Schema

| Field | Type | Description |
|-------|------|-------------|
| customer_id | Integer | Unique customer identifier |
| customer_name | String | Customer name |
| product_id | String | Product identifier |
| product_name | String | Product name |
| purchase_amount | Double | Transaction amount |
| region | String | Geographic region |

## ETL Workflow

1. **Initialize Spark Session** with optimized configurations
2. **Extract** source data from CSV with schema validation
3. **Transform** data through cleaning and enrichment
4. **Aggregate** metrics by region and customer tier
5. **Load** results to target storage (Parquet)
6. **Cleanup** Spark resources

## Skills Demonstrated
- PySpark DataFrame operations
- Data quality and validation
- ETL pipeline architecture
- Distributed data processing
- Performance optimization
- Business logic implementation

## Use Cases
- Customer analytics and segmentation
- Sales data processing
- Regional performance analysis
- Data warehouse population

## Author
Designed for Data Engineering portfolio demonstrations
