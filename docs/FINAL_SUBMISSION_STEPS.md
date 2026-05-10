# Final submission steps - NYC Taxi Data Product

Pipeline has successfully completed and produced the Gold data product plus quality metrics.

## 1. Files generated locally
After the successful run, the important local outputs are:

- `warehouse.duckdb` - DuckDB database with `gold_monthly_borough_stats`
- `quality/data_quality_metrics.csv` - calculated quality metrics
- `export/gold_monthly_borough_stats.csv` - CSV export for consumers
- `export/gold_monthly_borough_stats.parquet` - Parquet export for consumers

## 2. Files to commit to Git
Commit the project repository with these key files:

- `data_product_contract.yaml`
- `quality/data_quality_metrics.csv`
- `docs/NYC_Taxi_Data_Product_Card.docx`
- `docs/NYC_Taxi_Data_Product_Card.pdf`
- `marketplace/MS_TEAMS_MARKETPLACE_DESCRIPTION.md`
- `scripts/`
- `sql/`
- `orchestration/`
- `streaming_extension/`

Recommended commands:

```powershell
git init
git add .
git commit -m "Add NYC Taxi data product contract and quality metrics"
git branch -M main
git remote add origin YOUR_REPOSITORY_URL
git push -u origin main
```

## 3. Files to upload to MS Teams Data Marketplace
Upload the following:

- `docs/NYC_Taxi_Data_Product_Card.pdf`
- `docs/NYC_Taxi_Data_Product_Card.docx`
- `data_product_contract.yaml`
- `quality/data_quality_metrics.csv`
- `export/gold_monthly_borough_stats.csv` or `export/gold_monthly_borough_stats.parquet`
- Git repository link

## 4. Short description to paste in Teams

Monthly NYC Yellow Taxi demand and revenue statistics by pickup and dropoff borough. Useful for dashboarding, borough comparison, revenue analysis and demonstrating a medallion data product with quality metrics, contract, orchestration and streaming extension.

## 5. Quality metrics from the successful run

| Metric | Current value | Threshold | Status |
|---|---:|---:|---|
| Gold row count | 3312 | > 0 | PASS |
| Gold key completeness | 100.0% | >= 95% | PASS |
| Positive trip count validity | 100.0% | 100% | PASS |
| Non-negative amount validity | 100.0% | >= 99% | PASS |
| Data freshness months | 74 | <= 6 months if current source files are downloaded | WARN |

Freshness is WARN because this run uses historical TLC source files. The product is still valid for historical analysis.
