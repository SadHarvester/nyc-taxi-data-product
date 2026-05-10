# Fix: HTTP 403 while downloading NYC Taxi files

## What happened

The TLC CloudFront endpoint may return `403 Forbidden` for some monthly Parquet files, for example:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-08.parquet
```

The first version of `scripts/download_data.py` stopped the whole pipeline when one monthly file failed. Because of that, `warehouse.duckdb` was not created and `scripts/generate_data_product_metrics.py` failed afterwards.

## What was changed

This version fixes the issue by:

1. Capping the default download range at `2014-01` to `2018-07`.
2. Adding browser-like request headers.
3. Skipping unavailable optional monthly files instead of stopping the whole pipeline.
4. Writing a download manifest to `state/download_manifest.json`.
5. Making the Bronze loader schema-safe for older NYC Taxi files that do not have columns such as `congestion_surcharge`.

## Recommended commands

From the project root run:

```powershell
.venv\Scripts\activate
python scripts/main.py --mode full
python scripts/export_gold_dataset.py
```

Then check:

```powershell
python scripts/generate_data_product_metrics.py
```

## Optional: custom date range

PowerShell example:

```powershell
$env:NYC_TAXI_START_YEAR="2018"
$env:NYC_TAXI_START_MONTH="1"
$env:NYC_TAXI_END_YEAR="2018"
$env:NYC_TAXI_END_MONTH="7"
python scripts/download_data.py
```

CMD example:

```cmd
set NYC_TAXI_START_YEAR=2018
set NYC_TAXI_START_MONTH=1
set NYC_TAXI_END_YEAR=2018
set NYC_TAXI_END_MONTH=7
python scripts/download_data.py
```
