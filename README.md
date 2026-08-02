# Incremental File Ingestion Pipeline Using Auto Loader & Delta Lake

## Project Overview

This project implements an end-to-end incremental data ingestion pipeline using Databricks Auto Loader and Delta Lake. The pipeline processes only newly arrived transaction files, transforms the data through Bronze, Silver, and Gold layers, and generates business-ready KPIs while ensuring reliability, scalability, and fault tolerance.

The implementation follows the Medallion Architecture and demonstrates incremental data processing, schema evolution, checkpointing, deduplication, Delta MERGE operations, and pipeline monitoring.

---

## Problem Statement

Traditional batch ingestion systems repeatedly process entire datasets whenever new files arrive, resulting in:

- Increased processing time
- Higher compute cost
- Duplicate records
- Poor scalability
- Inefficient resource utilization

This project solves these challenges by processing only newly arrived files using Databricks Auto Loader and Delta Lake.

---

## Objectives

- Detect newly arrived files automatically
- Process only incremental data
- Implement checkpointing
- Ensure fault tolerance
- Support schema evolution
- Handle duplicate records
- Generate business KPIs
- Monitor pipeline execution

---

## Technologies Used

- Databricks
- PySpark
- Delta Lake
- Databricks Auto Loader
- Structured Streaming
- SQL

---

## Project Structure

```
incremental-file-ingestion-pipeline/
│
├── datasets/raw
│   ├── Fact_Sales_1.csv
│   ├── Fact_Sales_2.csv
│   └── 2010-12-08.csv
│
├── files/
│   ├── usecases_business_value.docx
│   ├── technical_implementation_guide.docx
│   └── Incremental File Ingestion Pipeline Using Auto Loader Concept & Delta Lake.docx
│
├── notebooks/
│   ├── 01_Data_Analysis.py
│   ├── 02_Bronze_Autoloader.py
│   ├── 03_Silver_Transformation.py
│   ├── 04_Gold_Aggregation.py
│   ├── 05_Audit_Monitoring.py
│   └── 06_Maintenance.py
│
├── README.md
└── .gitignore
```

---

## Pipeline Architecture

```
Landing Zone
      │
      ▼
Databricks Auto Loader
      │
      ▼
Bronze Layer
(Raw Data + Metadata)
      │
      ▼
Silver Layer
(Data Cleansing + Deduplication + MERGE)
      │
      ▼
Gold Layer
(Business KPIs)
      │
      ▼
Audit & Monitoring
      │
      ▼
Maintenance
(OPTIMIZE • VACUUM • Delta History)
```

---

# Pipeline Workflow

## Notebook 01 – Data Analysis

- Validate incoming datasets
- Analyze schema
- Check null values
- Detect duplicate records
- Generate descriptive statistics

---

## Notebook 02 – Bronze Layer

- Incremental ingestion using Auto Loader
- Schema inference
- Schema evolution
- Metadata enrichment
- Checkpointing
- Store raw data in Delta Lake

---

## Notebook 03 – Silver Layer

- Data cleansing
- Remove duplicate records
- Handle invalid values
- Standardize timestamps
- Calculate Total Sales
- Delta MERGE
- Add processed metadata

---

## Notebook 04 – Gold Layer

Generate business KPIs including:

- Total Records Processed
- Total Revenue
- Latest Data Freshness

---

## Notebook 05 – Audit & Monitoring

- Delta transaction history
- Pipeline validation
- Operational monitoring
- Table verification

---

## Notebook 06 – Maintenance

- Delta table optimization
- VACUUM
- Table health check
- Pipeline maintenance summary
- Table details

---

## Key Features

- Incremental File Processing
- Databricks Auto Loader
- Delta Lake
- Structured Streaming
- Schema Evolution
- Checkpointing
- Delta MERGE
- Data Deduplication
- Data Quality Validation
- Business KPI Generation
- Pipeline Monitoring
- Delta Table Maintenance

---

## Business Benefits

- Reduced Processing Time
- Cost Optimization
- Improved Scalability
- Reliable Incremental Processing
- Fault Tolerance
- Production-ready Data Pipeline

---

## Sample Output

### Bronze Layer

- Raw transaction records
- File metadata
- Ingestion timestamp

### Silver Layer

- Clean records
- Duplicate-free data
- Business metrics
- Processed timestamp

### Gold Layer

- Aggregated KPIs
- Revenue summary
- Processed record count

---

## Future Enhancements

- Azure Data Factory orchestration
- Delta Live Tables
- Data Quality Expectations
- Email and Slack Alerts
- Unity Catalog Integration
- CI/CD Deployment
- Automated Scheduling

---

## Author

**Md Sahil Ansari**

Data Engineering Intern Project

celebal technologies

---

## Acknowledgements

This project was developed as part of a Data Engineering internship assignment demonstrating incremental data ingestion using Databricks Auto Loader and Delta Lake.