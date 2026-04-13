# Setup And Run

## Prerequisites

- Docker Desktop with Compose V2
- Port availability: 3000, 5432, 9092, 9093

## Start stack

```bash
docker compose -f docker-compose.yaml up -d --build
```

## Stop stack

```bash
docker compose -f docker-compose.yaml down
```

## Check running services

```bash
docker compose -f docker-compose.yaml ps
```

Expected core services:

- kafka
- postgres
- producer
- consumer
- grafana

## Validate database ingest

```bash
docker compose -f docker-compose.yaml exec -T postgres psql -U admin -d kraf_db -c "SELECT COUNT(*) AS total_rows FROM stock_prices;"
```

Sample rows:

```bash
docker compose -f docker-compose.yaml exec -T postgres psql -U admin -d kraf_db -c "SELECT symbol, source, datetime FROM stock_prices ORDER BY datetime DESC LIMIT 10;"
```

## Run helper script

PowerShell helper for quick checks:

```powershell
./scripts/check_data.ps1 -ComposeFile docker-compose.yaml -ShowSampleRows
```

## Useful logs

```bash
docker compose -f docker-compose.yaml logs --since=10m producer consumer postgres
```
