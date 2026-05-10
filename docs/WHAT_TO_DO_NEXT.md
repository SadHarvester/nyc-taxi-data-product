# Co masz zrobić przed kolejnymi zajęciami

## 1. Wgraj projekt na Git

1. Rozpakuj ZIP.
2. Otwórz katalog `nyc_taxi_data_product_ready` w VS Code.
3. W terminalu uruchom:

```bash
git init
git add .
git commit -m "Add NYC Taxi data product contract and quality metrics"
```

4. Utwórz repozytorium na GitHub/GitLab.
5. Wypchnij kod:

```bash
git remote add origin TWOJ_LINK_DO_REPO
git branch -M main
git push -u origin main
```

Jeżeli repozytorium jest prywatne, wpisz w karcie produktu, że dostęp jest przez zaproszenie do repo lub przez eksport CSV/Parquet w MS Teams.

## 2. Zainstaluj zależności

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Jeżeli nie masz pliku `requirements.txt`, użyj:

```bash
pip install duckdb pandas requests dagster dagster-webserver pyarrow
```

## 3. Uruchom pipeline

Pełne przetworzenie:

```bash
python scripts/main.py --mode full
```

Wersja przyrostowa / idempotentna:

```bash
python scripts/main.py --mode incremental
```

Dagster UI:

```bash
python -m dagster dev -f orchestration/dagster_pipeline.py
```

## 4. Wygeneruj metryki jakości danych

Po pipeline uruchom:

```bash
python scripts/generate_data_product_metrics.py
```

Wynik zapisze się tutaj:

```text
quality/data_quality_metrics.csv
```

To jest Twoje potwierdzenie punktu 1 z zadania: minimum 3 metryki jakości danych. Projekt ma ich 5:

- `gold_row_count`
- `gold_key_completeness`
- `positive_trip_count_validity`
- `non_negative_amount_validity`
- `data_freshness_months`

## 5. Wyeksportuj produkt danych dla innych studentów

```bash
python scripts/export_gold_dataset.py
```

Powstaną pliki:

```text
export/gold_monthly_borough_stats.csv
export/gold_monthly_borough_stats.parquet
```

Te pliki możesz wrzucić do MS Teams jako gotowy dataset lub podać instrukcję dostępu przez DuckDB.

## 6. Co wrzucić do MS Teams Data Marketplace

Do folderu Data Marketplace wrzuć:

1. `docs/NYC_Taxi_Data_Product_Card.docx`
2. `docs/NYC_Taxi_Data_Product_Card.pdf`
3. `data_product_contract.yaml`
4. `quality/data_quality_metrics.csv` po uruchomieniu pipeline
5. link do repo Git
6. opcjonalnie eksport danych:
   - `export/gold_monthly_borough_stats.csv`
   - `export/gold_monthly_borough_stats.parquet`

## 7. Krótki opis do wklejenia na Teams

NYC Taxi Monthly Borough Statistics is a curated Gold-layer data product based on NYC Yellow Taxi trip records. It aggregates valid taxi trips by month, pickup borough and dropoff borough, making the data easy to use for dashboards, revenue analysis and borough-to-borough mobility analysis. The product includes a Data Product Contract, quality metrics, DuckDB access instructions and CSV/Parquet export options.

## 8. Jak ktoś ma skonsumować dane

Opcja SQL / DuckDB:

```sql
SELECT trip_month, pickup_borough, SUM(trip_count) AS trips, SUM(total_revenue) AS revenue
FROM gold_monthly_borough_stats
GROUP BY trip_month, pickup_borough
ORDER BY trip_month, trips DESC;
```

Opcja Python:

```python
import duckdb
con = duckdb.connect('warehouse.duckdb')
df = con.sql('SELECT * FROM gold_monthly_borough_stats LIMIT 100').df()
print(df.head())
```
