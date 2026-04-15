# Operations And Troubleshooting

## Daily health checks

1. Check container state:

```bash
docker compose -f docker-compose.yaml ps
```

2. Check producer and consumer logs:

```bash
docker compose -f docker-compose.yaml logs --since=10m producer consumer
```

3. Check Postgres row growth:

```bash
docker compose -f docker-compose.yaml exec -T postgres psql -U admin -d kraf_db -c "SELECT COUNT(*) FROM stock_prices;"
```

## Common issues

### Postgres appears empty

Checks:

- Confirm query target is `kraf_db` and table `stock_prices`.
- Ensure query is not filtered to `source='yfinance'` during Yahoo outage.
- Compare `NOW()` and `MAX(datetime)`.

Diagnostic command:

```bash
docker compose -f docker-compose.yaml exec -T postgres psql -U admin -d kraf_db -c "SELECT NOW(), MAX(datetime), COUNT(*) FROM stock_prices;"
```

### Producer starts but Yahoo fetch fails

Symptoms:

- Many `JSONDecodeError` logs from yfinance.

Behavior:

- Producer automatically falls back to mock records when batch returns empty.

Action:

- Keep fallback enabled for pipeline continuity.
- If real Yahoo data is required, reduce batch size and verify outbound network policy.

### Symbols file missing

Symptoms:

- Producer startup warning about missing symbols file.

Current mitigation:

- Compose bind mounts `./producer/sp500_symbols.json` to `/app/data/sp500_symbols.json`.
- Producer has internal default symbol fallback.

### Authentication failures in Postgres logs

Symptoms:

- `password authentication failed` for legacy client users.

Current mitigation:

- Compatibility role `kraf_db` exists and has table grants.
- Preferred runtime credential remains `admin/admin` in compose.

## Recovery flow

1. Restart only producer:

```bash
docker compose -f docker-compose.yaml up -d --force-recreate producer
```

2. Restart only consumer:

```bash
docker compose -f docker-compose.yaml up -d --force-recreate consumer
```

3. Full restart:

```bash
docker compose -f docker-compose.yaml down
docker compose -f docker-compose.yaml up -d --build
```
