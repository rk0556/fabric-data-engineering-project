# Retail Data Engineering Fabric - Architecture

## Overview

This project implements an end-to-end retail data engineering solution using Microsoft Fabric Medallion Architecture.

The solution ingests raw retail transaction data, applies data quality checks and transformations, creates analytics-ready Gold layer datasets, and enables business reporting through Power BI.

---

# Architecture Overview

```
Source CSV Files
        |
        v
Microsoft Fabric Lakehouse
        |
        v
+----------------+
| Bronze Layer   |
+----------------+
        |
        v
+----------------+
| Silver Layer   |
+----------------+
        |
        v
+----------------+
| Gold Layer     |
+----------------+
        |
        v
Power BI Semantic Model
        |
        v
Business Dashboards
```

---

# Technology Stack

| Component | Technology |
|-----------|------------|
| Data Platform | Microsoft Fabric |
| Storage | Fabric Lakehouse |
| Processing | PySpark |
| Storage Format | Delta Lake |
| Orchestration | Fabric Data Pipeline |
| Version Control | GitHub |
| Reporting | Power BI |

---

# Data Flow

## 1. Source Layer

Input:
- Retail transaction CSV files

Location:

```
Lakehouse Files/source/
```

The pipeline reads source files using PySpark with an explicit schema definition.

---

# 2. Bronze Layer

Purpose:

Store raw ingested data with minimal transformation.

Activities:

- Read CSV files
- Apply schema
- Preserve source values
- Add ingestion metadata

Additional metadata columns:

- _ingested_at
- _source_file

Output Table:

```
bronze_retail_transactions
```

---

# 3. Silver Layer

Purpose:

Create trusted and validated datasets.

Transformations:

- Date normalization
- Timestamp creation
- Data quality validation
- Missing value checks
- Business rule validation
- Record classification

Data Quality Checks:

- Missing transaction IDs
- Missing customer IDs
- Invalid dates
- Negative amounts
- Invalid ratings

Output Tables:

```
silver_retail_transactions

silver_retail_rejected
```

---

# 4. Gold Layer

Purpose:

Create business-ready analytical datasets using a star schema model.

## Dimension Tables

```
gold_dim_customer

gold_dim_product

gold_dim_date
```

## Fact Table

```
gold_fact_sales
```

The Gold layer supports:

- Power BI reporting
- KPI dashboards
- Business analysis

---

# Pipeline Execution and Monitoring

The pipeline includes execution audit logging.

Audit Table:

```
pipeline_execution_log
```

Captured information:

- Pipeline run ID
- Pipeline name
- Start time
- End time
- Execution status
- Source file
- Bronze record count
- Silver record count
- Gold record count
- Error messages

---

# Error Handling

The pipeline implements:

- Stage-level execution tracking
- Exception handling
- Failure message capture
- Guaranteed audit logging

Pipeline Stages:

```
Bronze Ingestion
        |
Silver Processing
        |
Gold Dimensional Modeling
```

If any stage fails:

- Pipeline status is marked as Failed
- Error details are stored in the audit log
- Execution history is preserved

---

# Deployment Approach

Development workflow:

```
Microsoft Fabric Workspace
            |
            v
GitHub Repository
            |
            v
Version Controlled Deployment
```

Git integration is used for:

- Notebook version control
- Pipeline version control
- Documentation management

---

# Future Enhancements

Planned improvements:

- Incremental data loading using watermark columns
- Automated data quality monitoring
- Pipeline scheduling
- Power BI semantic model optimization
- Row-level security implementation
