# End-To-End_Azure_Data_Engineering_Project

This project demonstrates a complete **data engineering pipeline on Microsoft Azure**, covering data ingestion, transformation, storage, governance, and deployment. The pipeline follows a modern **Lakehouse Architecture** using Azure Data Factory, Databricks, ADLS Gen2, Delta Lake, Unity Catalog, and CI/CD deployment with Databricks Asset Bundles.

#project architecture


<img width="1024" height="585" alt="image" src="https://github.com/user-attachments/assets/7485d1ce-b9ee-4771-bcb2-06de90b0a17d" />



---

## 🧰 Technologies Used

| Category | Tools / Services |
|--------|------------------|
| Cloud | Microsoft Azure |
| Storage | ADLS Gen2, Delta Lake |
| Orchestration | Azure Data Factory |
| Compute | Azure Databricks (PySpark) |
| Governance | Unity Catalog |
| Streaming | Databricks Autoloader |
| Integration | Azure Logic Apps |
| CI/CD | Databricks Asset Bundles |
| Database | Azure SQL Database |
| Languages | SQL, Python (PySpark), Jinja2 |

---

## 🚀 Key Features Implemented

- End-to-end data pipeline using Azure Data Factory
- Incremental data ingestion with watermark-based logic
- Historical backfill framework for batch processing
- Parameterized & looped pipelines for multi-table orchestration
- Transformation pipelines using PySpark in Databricks
- Silver & Gold layer data modeling using Delta Lake
- Lakehouse governance with Unity Catalog
- Streaming ingestion using Databricks Autoloader
- Metadata-driven pipeline framework using PySpark & Jinja2 templates
- Star Schema Data Warehouse design & SCD Type 2 implementation
- Production-grade pipelines using Delta Live Tables (DLT)
- CI/CD deployment using Databricks Asset Bundles

---





