# MS Teams Data Marketplace - opis produktu

## Nazwa produktu
NYC Taxi Monthly Borough Statistics

## Krótki opis
Curated Gold-layer data product based on NYC Yellow Taxi trip records. The dataset shows monthly taxi trip counts, average distance, average amount and total revenue by pickup and dropoff borough.

## Do czego służy
Produkt pomaga szybko analizować ruch taksówek w Nowym Jorku bez pracy na surowych plikach Parquet. Nadaje się do dashboardów, prostych wizualizacji, analizy popytu i porównania przychodów między dzielnicami.

## Źródła danych
- NYC TLC Yellow Taxi Trip Records, Parquet
- NYC TLC Taxi Zone Lookup, CSV

## Dostęp
- DuckDB table: `gold_monthly_borough_stats` in `warehouse.duckdb`
- Export CSV: `export/gold_monthly_borough_stats.csv`
- Export Parquet: `export/gold_monthly_borough_stats.parquet`
- Contract file: `data_product_contract.yaml`

## Git / contract
Wklej tutaj link do swojego repozytorium Git po wypchnięciu projektu:

`TU_WKLEJ_LINK_DO_REPO`

Jeżeli repo jest prywatne: dostęp przez zaproszenie do repo albo przez pliki CSV/Parquet udostępnione w tym folderze MS Teams.

## Quality metrics - last local run
Calculated at: `2026-05-10 20:05 Europe/Warsaw`

| Metric | Current value | Threshold | Status |
|---|---:|---:|---|
| Gold row count | 3312 | > 0 | PASS |
| Gold key completeness | 100.0% | >= 95% | PASS |
| Positive trip count validity | 100.0% | 100% | PASS |
| Non-negative amount validity | 100.0% | >= 99% | PASS |
| Data freshness months | 74 | <= 6 months if current source files are downloaded | WARN |

Freshness is WARN because the downloaded TLC source files are historical. This does not block historical analysis of NYC Taxi patterns.

## Owner / contact
Wiktor Lenarczyk - MS Teams / PJATK
